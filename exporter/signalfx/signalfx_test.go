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

package signalfx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

type mSlice []*stats.Int64Measure

func (measures *mSlice) createAndAppend(name, desc, unit string) {
	m := stats.Int64(name, desc, unit)
	*measures = append(*measures, m)
}

type vCreator []*view.View

func (vc *vCreator) createAndAppend(name, description string, keys []tag.Key, measure stats.Measure, agg *view.Aggregation) {
	v := &view.View{
		Name:        name,
		Description: description,
		TagKeys:     keys,
		Measure:     measure,
		Aggregation: agg,
	}
	*vc = append(*vc, v)
}

type TestDatapoint struct {
	MetricType       string
	MetricName       string
	MetricValue      string
	MetricDimensions []Dimension
}

type Dimension struct {
	Key   *string
	Value *string
}

var MetricTypeName = map[int32]string{
	0: "GAUGE",
	1: "COUNTER",
	2: "ENUM",
	3: "CUMULATIVE_COUNTER",
}

func addDataPoint(dataPoints []TestDatapoint, dataPoint TestDatapoint) []TestDatapoint {
	exists := false
	if len(dataPoints) == 0 {
		dataPoints = append(dataPoints, dataPoint)
	} else {
		for _, point := range dataPoints {
			if point.MetricName == dataPoint.MetricName {
				exists = true
			}
		}
		if !exists {
			dataPoints = append(dataPoints, dataPoint)
		}
	}
	return dataPoints
}

func decodeToken(req *http.Request, tokenValue string) bool {
	var header []string
	for name, headers := range req.Header {
		name = strings.ToLower(name)
		for _, h := range headers {
			header = append(header, fmt.Sprintf("%v: %v", name, h))
		}
	}
	value := strings.Join(header, "\n")
	if strings.Contains(value, tokenValue) {
		return true
	}
	return false
}

func decodeDatapoints(seenBodyPoints com_signalfx_metrics_protobuf.DataPointUploadMessage,
	dataPoints []TestDatapoint) []TestDatapoint {
	for i := 0; i < len(seenBodyPoints.Datapoints); i++ {
		var dimensions []Dimension
		if len(seenBodyPoints.Datapoints[i].GetDimensions()) != 0 {
			for _, dimension := range seenBodyPoints.Datapoints[i].GetDimensions() {
				dimensions = append(dimensions,
					Dimension{
						Key:   dimension.Key,
						Value: dimension.Value,
					})
			}
		} else {
			dimensions = nil
		}
		metricType := int32(*seenBodyPoints.Datapoints[i].MetricType)
		var metricValue string
		if metricType == 3 {
			metricValue = strconv.FormatInt(seenBodyPoints.Datapoints[i].Value.GetIntValue(), 16)
		} else {
			metricValue = strconv.FormatFloat(*seenBodyPoints.Datapoints[i].Value.DoubleValue,
				'g', 1, 64)
		}
		dataPoint := TestDatapoint{
			MetricName:       *seenBodyPoints.Datapoints[i].Metric,
			MetricValue:      metricValue,
			MetricType:       MetricTypeName[metricType],
			MetricDimensions: dimensions,
		}
		dataPoints = addDataPoint(dataPoints, dataPoint)
	}
	return dataPoints
}

func TestExporterTokenSet(t *testing.T) {
	_, err := NewExporter(Options{Token: "token"})
	if err != nil {
		t.Fatalf("shold not have generater error with token not null. err: %T", err)
	}
}

func TestExporterTokenNotSet(t *testing.T) {
	_, err := NewExporter(Options{})
	if err == nil {
		t.Fatal("shold have generater error with token null")
	} else {
		if err.Error() != "token cannot be empty on options signalfx.Options" {
			t.Fatal("expected empty token error message")
		}
	}
}

var token = &SafeToken{}

type SafeToken struct {
	token bool
	m     sync.Mutex
}

func (i *SafeToken) Get() bool {
	i.m.Lock()
	// Defer `Unlock` until this method returns
	defer i.m.Unlock()
	// Return the value
	return i.token
}
func (i *SafeToken) Set(val bool) {
	i.m.Lock()
	defer i.m.Unlock()
	i.token = val
}

type SafeDatapoint struct {
	datapoint []TestDatapoint
	m         sync.Mutex
}

func (i *SafeDatapoint) Get() []TestDatapoint {
	i.m.Lock()
	// Defer `Unlock` until this method returns
	defer i.m.Unlock()
	// Return the value
	return i.datapoint
}
func (i *SafeDatapoint) Set(val []TestDatapoint) {
	i.m.Lock()
	defer i.m.Unlock()
	i.datapoint = val
}

var testGaugeDataPoints = &SafeDatapoint{}

func TestGaugeDataOutput(t *testing.T) {
	token.Set(false)
	tokenValue := "opencensusT0k3n"
	retString := `"OK"`
	retCode := http.StatusOK
	var blockResponse chan struct{}
	var cancelCallback func()

	seenBodyPoints := &com_signalfx_metrics_protobuf.DataPointUploadMessage{}
	handler := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		bodyBytes := bytes.Buffer{}
		_, err := io.Copy(&bodyBytes, req.Body)
		if err != nil {
			t.Fatal(err)
		}
		req.Body.Close()
		proto.Unmarshal(bodyBytes.Bytes(), seenBodyPoints)
		testGaugeDataPoints.Set(decodeDatapoints(*seenBodyPoints, testGaugeDataPoints.Get()))
		token.Set(decodeToken(req, tokenValue))

		rw.WriteHeader(retCode)
		io.WriteString(rw, retString)
		if blockResponse != nil {
			if cancelCallback != nil {
				cancelCallback()
			}
			select {
			case <-req.Context().Done():
			case <-blockResponse:
			}
		}
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")

	server := http.Server{
		Handler: handler,
	}
	serverDone := make(chan struct{})
	go func() {
		if err := server.Serve(l); err == nil {
			t.Log("I expect serve to eventually error")
		}
		close(serverDone)
	}()

	exporter, err := NewExporter(Options{Token: tokenValue,
		DatapointEndpoint: "http://" + l.Addr().String()})
	if err != nil {
		t.Fatalf("failed to create signalfx exporter: %v", err)
	}

	view.RegisterExporter(exporter)

	names := []string{"las", "cos", "tar"}

	var measures mSlice
	for _, name := range names {
		measures.createAndAppend("metric."+name, name, "")
	}

	var vc vCreator
	for _, m := range measures {
		vc.createAndAppend(m.Name(), m.Description(), nil, m, view.LastValue())
	}

	if err := view.Register(vc...); err != nil {
		t.Fatalf("failed to create views: %v", err)
	}
	defer view.Unregister(vc...)

	view.SetReportingPeriod(time.Millisecond)

	for _, m := range measures {
		stats.Record(context.Background(), m.M(1))
		<-time.After(150 * time.Millisecond)
	}

	if !token.Get() {
		t.Fatal("Token not properly set")
	}

	if len(testGaugeDataPoints.Get()) != len(measures) {
		t.Fatalf("expected %d, but got %d dataPoints", len(measures), len(testGaugeDataPoints.Get()))
	}

	for _, name := range names {
		correct := false
		metricName := "metric_" + name
		for _, point := range testGaugeDataPoints.Get() {
			if metricName == point.MetricName &&
				point.MetricValue == "1" &&
				point.MetricType == "GAUGE" {
				correct = true
			}
		}
		if !correct {
			t.Fatalf("expected metric name to be %s, "+
				"metric value to be 1 and metricType to be GAUGE", metricName)
		}
	}

	l.Close()
}

var testCounterDataPoints = &SafeDatapoint{}

func TestCounterDataOutput(t *testing.T) {
	token.Set(false)
	tokenValue := "S3cr3tT0k3n"
	retString := `"OK"`
	retCode := http.StatusOK
	var blockResponse chan struct{}
	var cancelCallback func()

	seenBodyPoints := &com_signalfx_metrics_protobuf.DataPointUploadMessage{}
	handler2 := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		bodyBytes := bytes.Buffer{}
		_, err := io.Copy(&bodyBytes, req.Body)
		if err != nil {
			t.Fatal(err)
		}
		req.Body.Close()
		proto.Unmarshal(bodyBytes.Bytes(), seenBodyPoints)
		testCounterDataPoints.Set(decodeDatapoints(*seenBodyPoints, testCounterDataPoints.Get()))
		token.Set(decodeToken(req, tokenValue))

		rw.WriteHeader(retCode)
		io.WriteString(rw, retString)
		if blockResponse != nil {
			if cancelCallback != nil {
				cancelCallback()
			}
			select {
			case <-req.Context().Done():
			case <-blockResponse:
			}
		}
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")

	server := http.Server{
		Handler: handler2,
	}
	serverDone := make(chan struct{})
	go func() {
		if err := server.Serve(l); err == nil {
			t.Log("I expect serve to eventually error")
		}
		close(serverDone)
	}()

	exporter, err := NewExporter(Options{Token: tokenValue,
		DatapointEndpoint: "http://" + l.Addr().String()})
	if err != nil {
		t.Fatalf("failed to create signalfx exporter: %v", err)
	}

	view.RegisterExporter(exporter)

	names := []string{"foo", "bar"}

	var measures mSlice
	for _, name := range names {
		measures.createAndAppend("test."+name, name, "")
	}

	var vc vCreator
	for _, m := range measures {
		vc.createAndAppend(m.Name(), m.Description(), nil, m, view.Count())
	}

	if err := view.Register(vc...); err != nil {
		t.Fatalf("failed to create views: %v", err)
	}
	defer view.Unregister(vc...)

	view.SetReportingPeriod(time.Millisecond)

	for _, m := range measures {
		stats.Record(context.Background(), m.M(1))
		<-time.After(150 * time.Millisecond)
	}

	if !token.Get() {
		t.Fatal("Token not properly set")
	}

	if len(testCounterDataPoints.Get()) != len(measures) {
		t.Fatalf("expected %d, but got %d dataPoints", len(measures), len(testCounterDataPoints.Get()))
	}

	for _, name := range names {
		correct := false
		metricName := "test_" + name
		for _, point := range testCounterDataPoints.Get() {
			if metricName == point.MetricName &&
				point.MetricValue == "1" &&
				point.MetricType == "CUMULATIVE_COUNTER" {
				correct = true
			}
		}
		if !correct {
			t.Fatalf("expected metric name to be %s, "+
				"metric value to be 1 and metricType to be CUMULATIVE_COUNTER", metricName)
		}
	}

	l.Close()
}

var testCounterDimDataPoints = &SafeDatapoint{}

func TestCounterDataDimensionsOutput(t *testing.T) {
	token.Set(false)
	tokenValue := "S3cr3tT0k3nD1m3ns10s"
	retString := `"OK"`
	retCode := http.StatusOK
	var blockResponse chan struct{}
	var cancelCallback func()

	seenBodyPoints := &com_signalfx_metrics_protobuf.DataPointUploadMessage{}
	handler := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		bodyBytes := bytes.Buffer{}
		_, err := io.Copy(&bodyBytes, req.Body)
		if err != nil {
			t.Fatal(err)
		}
		req.Body.Close()
		proto.Unmarshal(bodyBytes.Bytes(), seenBodyPoints)

		testCounterDimDataPoints.Set(decodeDatapoints(*seenBodyPoints, testCounterDimDataPoints.Get()))
		token.Set(decodeToken(req, tokenValue))

		rw.WriteHeader(retCode)
		io.WriteString(rw, retString)
		if blockResponse != nil {
			if cancelCallback != nil {
				cancelCallback()
			}
			select {
			case <-req.Context().Done():
			case <-blockResponse:
			}
		}
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")

	server := http.Server{
		Handler: handler,
	}
	serverDone := make(chan struct{})
	go func() {
		if err := server.Serve(l); err == nil {
			t.Log("I expect serve to eventually error")
		}
		close(serverDone)
	}()

	exporter, err := NewExporter(Options{Token: tokenValue,
		DatapointEndpoint: "http://" + l.Addr().String()})
	if err != nil {
		t.Fatalf("failed to create signalfx exporter: %v", err)
	}

	view.RegisterExporter(exporter)

	ctx := context.Background()

	// Creating tags for the views
	key1, err := tag.NewKey("name")
	if err != nil {
		t.Log(err)
	}

	key2, err := tag.NewKey("author")
	if err != nil {
		t.Log(err)
	}

	// Assigning values to the tags in the context
	ctx, err = tag.New(context.Background(),
		tag.Insert(key1, "video1"),
		tag.Upsert(key2, "john"),
	)

	var keys []tag.Key

	keys = append(keys, key1)
	keys = append(keys, key2)

	names := []string{"foo", "bar"}

	var measures mSlice
	for _, name := range names {
		measures.createAndAppend("test."+name, name, "")
	}

	var vc vCreator
	for _, m := range measures {
		vc.createAndAppend(m.Name(), m.Description(), keys, m, view.LastValue())
	}

	if err := view.Register(vc...); err != nil {
		t.Fatalf("failed to create views: %v", err)
	}
	defer view.Unregister(vc...)

	view.SetReportingPeriod(time.Millisecond)

	for _, m := range measures {
		stats.Record(ctx, m.M(1))
		<-time.After(150 * time.Millisecond)
	}

	if !token.Get() {
		t.Fatal("Token not properly set")
	}

	if len(testCounterDimDataPoints.Get()) != len(measures) {
		t.Fatalf("expected %d, but got %d dataPoints", len(measures), len(testCounterDimDataPoints.Get()))
	}

	for _, name := range names {
		correct := false
		metricName := "test_" + name
		for _, point := range testCounterDimDataPoints.Get() {
			if metricName == point.MetricName &&
				point.MetricValue == "1" &&
				point.MetricType == "GAUGE" {
				correct = true
			}
		}
		if !correct {
			t.Fatalf("expected metric name to be %s, "+
				"metric value to be 1 and metricType to be GAUGE", metricName)
		}
	}

	for _, point := range testCounterDimDataPoints.Get() {
		if len(point.MetricDimensions) != len(keys) {
			t.Fatalf("expected %d, but got %d dimensions", len(keys), len(point.MetricDimensions))
		}
		for _, dimension := range point.MetricDimensions {
			if (*dimension.Key != "name" && *dimension.Key != "author") ||
				(*dimension.Value != "video1" && *dimension.Value != "john") {
				t.Fatal("dimension not properly sent")
			}
		}
	}

	l.Close()
}
