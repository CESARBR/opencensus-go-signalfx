// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package signalfx contains a SignalFx exporter that supports exporting
// OpenCensus views as SignalFx data points.
package signalfx

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"google.golang.org/api/support/bundler"
)

// Exporter exports stats to SignalFx.
// In this exporter the view name is mapped to the metric name
// and the tags are mapped to dimensions.
type Exporter struct {
	// Options used to register and log stats
	opts    Options
	bundler *bundler.Bundler
	client  *sfxclient.Scheduler
}

const (
	defaultBufferedViewDataLimit = 10000 // max number of view.Data in flight
	defaultBundleCountThreshold  = 100   // max number of view.Data per bundle
)

// defaultDelayThreshold is the amount of time we wait to receive new view.Data
// from the aggregation pipeline. We normally expect to receive it in rapid
// succession, so we set this to a small value to avoid waiting
// unnecessarily before submitting.
const defaultDelayThreshold = 200 * time.Millisecond

// Options contains options for configuring the exporter.
type Options struct {
	// Token contains the required token to send datapoints
	// to SignalFx. It cannot be empty.
	Token string

	// DatapointEndpoint contains the endpoint to send the datapoints
	// to SignalFx. The default value can be found as sfxclient.IngestEndpointV2.
	DatapointEndpoint string

	// ReportingDelay contains the reporting interval.
	// The default value is 20s.
	ReportingDelay time.Duration

	// OnError is the hook to be called when there is
	// an error uploading the stats or tracing data.
	// If no custom hook is set, errors are logged.
	OnError func(err error)
}

// NewExporter returns an exporter that exports stats to SignalFx.
func NewExporter(o Options) (*Exporter, error) {
	if o.Token == "" {
		err := fmt.Errorf("token cannot be empty on options %T", o)
		return nil, err
	}

	e := &Exporter{
		opts:   o,
		client: sfxclient.NewScheduler(),
	}

	b := bundler.NewBundler((*view.Data)(nil), func(items interface{}) {
		vds := items.([]*view.Data)
		e.sendBundle(vds)
	})
	e.bundler = b

	e.bundler.BufferedByteLimit = defaultBufferedViewDataLimit
	e.bundler.BundleCountThreshold = defaultBundleCountThreshold
	e.bundler.DelayThreshold = defaultDelayThreshold

	if e.opts.DatapointEndpoint != "" {
		e.client.Sink.(*sfxclient.HTTPSink).DatapointEndpoint = e.opts.DatapointEndpoint
	}
	e.client.Sink.(*sfxclient.HTTPSink).AuthToken = e.opts.Token

	if e.opts.ReportingDelay != 0 {
		e.client.ReportingDelay(e.opts.ReportingDelay)
	}

	go e.client.Schedule(context.Background())

	return e, nil
}

var _ view.Exporter = (*Exporter)(nil)

func (o *Options) onError(err error) {
	if o.OnError != nil {
		o.OnError(err)
	} else {
		log.Printf("Failed to export to SignalFx: %v", err)
	}
}

// ExportView exports to the SignalFx if view data has one or more rows.
// Each OpenCensus stats records will be converted to
// corresponding SignalFx Metric.
func (e *Exporter) ExportView(vd *view.Data) {
	e.bundler.Add(vd, 1)
}

func (e *Exporter) Flush() {
	e.bundler.Flush()
}

// toMetric receives the view data information and creates metrics that are adequate according to
// graphite documentation.
func (e *Exporter) toMetric(v *view.View, row *view.Row, vd *view.Data) signalFxMetric {
	switch data := row.Data.(type) {
	case *view.CountData:
		return signalFxMetric{
			metricName:     sanitize(vd.View.Name),
			metricType:     "cumulative_counter",
			metricValueInt: data.Value,
			timestamp:      time.Now(),
			dimensions:     buildDimensions(row.Tags),
		}
	case *view.SumData:
		return signalFxMetric{
			metricName:     sanitize(vd.View.Name),
			metricType:     "cumulative_counter",
			metricValueInt: int64(data.Value),
			timestamp:      time.Now(),
			dimensions:     buildDimensions(row.Tags),
		}
	case *view.LastValueData:
		return signalFxMetric{
			metricName:  sanitize(vd.View.Name),
			metricType:  "gauge",
			metricValue: data.Value,
			timestamp:   time.Now(),
			dimensions:  buildDimensions(row.Tags),
		}
	default:
		// TODO: add support for histograms (Aggregation.DistributionData).
		e.opts.onError(fmt.Errorf("aggregation %T is not yet supported", data))
		return signalFxMetric{}
	}
}

// buildDimensions uses the tag values and keys to create
// the dimensions used by SignalFx.
func buildDimensions(t []tag.Tag) map[string]string {
	values := make(map[string]string)
	for _, t := range t {
		values[t.Key.Name()] = t.Value
	}
	return values
}

type signalFxMetric struct {
	metricType     string
	metricName     string
	metricValue    float64
	metricValueInt int64
	timestamp      time.Time
	dimensions     map[string]string
	buckets        []int64
}

// sendBundle extracts stats data and calls toMetric
// to convert the data to metrics formatted to graphite.
func (e *Exporter) sendBundle(vds []*view.Data) {
	ctx := context.Background()
	for _, vd := range vds {
		for _, row := range vd.Rows {
			metric := e.toMetric(vd.View, row, vd)
			data := []*datapoint.Datapoint{}
			switch metric.metricType {
			case "gauge":
				data = []*datapoint.Datapoint{
					sfxclient.GaugeF(metric.metricName, metric.dimensions, metric.metricValue),
				}
			case "cumulative_counter":
				data = []*datapoint.Datapoint{
					sfxclient.Cumulative(metric.metricName, metric.dimensions, metric.metricValueInt),
				}

			default:
				e.opts.onError(fmt.Errorf("metric type not supported: %s", metric.metricType))
			}

			err := e.client.Sink.AddDatapoints(ctx, data)
			if err != nil {
				e.opts.onError(fmt.Errorf("error sending datapoint to SignalFx: %T", err))
			}
		}
	}
}

const labelKeySizeLimit = 128

// Sanitize returns a string that is truncated to 128 characters if it's too
// long, and replaces non-alphanumeric characters to underscores.
func sanitize(s string) string {
	if len(s) == 0 {
		return s
	}
	if len(s) > labelKeySizeLimit {
		s = s[:labelKeySizeLimit]
	}
	s = strings.Map(sanitizeRune, s)
	if unicode.IsDigit(rune(s[0])) {
		s = "key_" + s
	}
	if s[0] == '_' {
		s = "key" + s
	}
	return s
}

// sanitizeRune converts anything that is not a letter or digit to an underscore.
func sanitizeRune(r rune) rune {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return r
	}
	// Everything else turns into an underscore.
	return '_'
}
