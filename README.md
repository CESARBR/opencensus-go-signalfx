# OpenCensus SignalFx Stats Exporter for Go
[![Gitter chat][gitter-image]][gitter-url]

The _OpenCensus SignalFx Stats Exporter for Go_ is a stats exporter that
exports data to [SignalFx](https://signalfx.com), a real-time monitoring
solution for cloud and distributed applications. SignalFx ingests that
data and offers various visualizations on charts, dashboards and service
maps, as well as real-time anomaly detection.

## Quickstart

### Import

```
import "opencensus-go-signalfx/exporter/signalfx"
```

The API of this project is still evolving.
The use of vendoring or a dependency management tool is recommended.


### Prerequisites

To use this exporter, one must have a [SignalFx](https://signalfx.com)
account and corresponding [data ingest
token](https://docs.signalfx.com/en/latest/admin-guide/tokens.html).

OpenCensus Go libraries require Go 1.8 or later.

### Register the exporter

```go
func main() {
    // SignalFx token is part of the Options struct.
    // Stats will be reported every second by default.
    exporter, err := signalfx.NewExporter(signalfx.Options{Token: "token"})
    ...
}
```

It is possible to set a different endpoint for SignalFx, use:

```go
func main() {
    exporter, err := signalfx.NewExporter(signalfx.Options{Token: "token", DatapointEndpoint: "Endpoint"})
    ....
}
```

It is possible to set different reporting intervals by using `SetReportingPeriod()`, for example:

```go
func main() {
    exporter, err := signalfx.NewExporter(signalfx.Options{Token: "token"})
    view.RegisterExporter(exporter)
    ....
    view.SetReportingPeriod(5 * time.Second)
}
```

[gitter-image]: https://badges.gitter.im/census-instrumentation/lobby.svg
[gitter-url]: https://gitter.im/census-instrumentation/lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge