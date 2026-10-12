.PHONY: test vet bench bench-protocol
test:
	GOWORK=off go test -race ./...
vet:
	GOWORK=off go vet ./...
bench:
	GOWORK=off go test ./logger -run '^$$' -bench . -benchmem -count=3

bench-protocol:
	GOWORK=off go test ./protocol -run '^$$' -bench '^BenchmarkDecode(Into)?$$' -benchmem -count=3
