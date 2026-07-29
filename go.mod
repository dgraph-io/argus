module github.com/dgraph-io/argus

go 1.26.4

require (
	github.com/dgraph-io/dgo/v250 v250.0.0
	github.com/google/pprof v0.0.0-20260604005048-7023385849c0
	github.com/gorilla/websocket v1.5.3
	golang.org/x/sys v0.47.0
	google.golang.org/grpc v1.82.1
)

require (
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/dgraph-io/dgo/v250 => /Users/matthew/code/dgo
