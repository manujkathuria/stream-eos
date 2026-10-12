.PHONY: build test vet run run-a run-b bench bench-reader bench-kafka
build:
	GOWORK=off go build -o bin/ingestor ./cmd/ingestor
test:
	GOWORK=off go test -race ./...
vet:
	GOWORK=off go vet ./...
run:
	GOWORK=off go run ./cmd/ingestor
run-a:
	GOWORK=off go run ./cmd/ingestor -id A
run-b:
	GOWORK=off go run ./cmd/ingestor -id B
bench:
	GOWORK=off go test ./internal/websocket ./internal/ingest ./internal/kafka -run '^$$' -bench 'Benchmark(MessageBuffer|IngestorLoop|BuildRecords)$$' -benchmem -count=3
	$(MAKE) -C ../pkg bench-protocol

bench-reader:
	STREAM_EOS_WEBSOCKET_BENCH=1 GOWORK=off go test ./internal/websocket -run '^$$' -bench '^BenchmarkSourceRead$$' -benchmem -count=3
bench-kafka:
	STREAM_EOS_KAFKA_BENCH=1 GOWORK=off go test ./internal/kafka ./internal/ingest -run '^$$' -bench 'Benchmark(PublishAcknowledged|IngestorAcknowledged)$$' -benchmem -benchtime=100x -count=3
