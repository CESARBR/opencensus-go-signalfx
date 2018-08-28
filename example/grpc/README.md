# Example gRPC server and client with OpenCensus SignalFx exporter

This example uses:

* gRPC to create an RPC server and client.
* SignalFx exporter
* The OpenCensus gRPC plugin to instrument the RPC server and client.
* Debugging exporters to print stats and traces to stdout.

```
$ go get go.opencensus.io/examples/grpc/...
```

First, run the server:

```
$ go run $(go env GOPATH)/src/go.opencensus.io/examples/grpc/helloworld_server/main.go
```

Then, set the `SIGNALFX_TOKEN` env variable and run the client:

```
$ go run $(go env GOPATH)/src/go.opencensus.io/examples/grpc/helloworld_client/main.go
```

You will see stats exported on the stdout.

You can also see the z-pages provided from the server:
* Traces: http://localhost:8081/debug/tracez
* RPCs: http://localhost:8081/debug/rpcz
