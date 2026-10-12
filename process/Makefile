.PHONY: build test vet run bench bench-transaction bench-json bench-dedup bench-consumer test-kafka
build:
	GOWORK=off go build -o bin/process ./cmd/process
test:
	GOWORK=off go test -race ./...
vet:
	GOWORK=off go vet ./...
run:
	GOWORK=off go run ./cmd/process $(ARGS)
bench:
	GOWORK=off go test ./internal/engine ./internal/dedup ./internal/kafka -run '^$$' -bench . -benchmem -count=3
test-kafka:
	STREAM_EOS_KAFKA_TEST=1 GOWORK=off go test -race ./internal/engine ./internal/kafka -count=1

bench-consumer:
	STREAM_EOS_KAFKA_BENCH=1 GOWORK=off go test ./internal/kafka -run '^$$' -bench '^BenchmarkConsumerPaths/readbatch_eventid' -benchmem -benchtime=1x -count=3

bench-dedup:
	GOWORK=off go test ./internal/dedup -run '^$$' -bench '^BenchmarkRecordDedup/(stage|commit)/' -benchmem -benchtime=100x -count=3
	GOWORK=off go test ./internal/dedup -run '^$$' -bench '^BenchmarkFIFO(Lookup|Churn)$$' -benchmem -benchtime=200ms -count=3

bench-json:
	GOWORK=off go test ./internal/engine -run '^$$' -bench '^BenchmarkEncodeJSON/reusable$$' -benchmem -benchtime=500ms -count=3

bench-transaction:
	STREAM_EOS_KAFKA_BENCH=1 GOWORK=off go test ./internal/kafka -run '^$$' -bench '^BenchmarkTransactionCommit$$' -benchmem -benchtime=100x -count=3
