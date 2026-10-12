.PHONY: build test vet run bench bench-delivery
build:
	GOWORK=off go build -o bin/source ./cmd/source
test:
	GOWORK=off go test -race ./...
vet:
	GOWORK=off go vet ./...
run:
	GOWORK=off go run ./cmd/source

bench:
	GOWORK=off go test ./internal/market ./internal/protocol -run '^$$' -bench 'Benchmark(GenerateUpdate|BuildMessage|SequencedFrameReuse)$$' -benchmem -count=3
bench-delivery:
	STREAM_EOS_WEBSOCKET_BENCH=1 GOWORK=off go test ./internal/websocket -run '^$$' -bench '^BenchmarkWebSocketDelivery$$' -benchmem -count=3
