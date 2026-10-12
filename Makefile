.PHONY: demo-performance demo build test vet source ingest-a ingest-b process topic
build:
	$(MAKE) -C websocket-source build
	$(MAKE) -C ingestor build
	$(MAKE) -C process build
test:
	$(MAKE) -C process test
	$(MAKE) -C websocket-source test
	$(MAKE) -C pkg test
	$(MAKE) -C ingestor test
vet:
	$(MAKE) -C websocket-source vet
	$(MAKE) -C pkg vet
	$(MAKE) -C ingestor vet
	$(MAKE) -C process vet
process:
	$(MAKE) -C process run
source:
	$(MAKE) -C websocket-source run
ingest-a:
	$(MAKE) -C ingestor run-a
ingest-b:
	$(MAKE) -C ingestor run-b
topic:
	kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic market.ticks.raw --partitions 3 --replication-factor 1
	kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic market.ticks.processed --partitions 3 --replication-factor 1
	kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic market.ticks.progress --partitions 3 --replication-factor 1 --config cleanup.policy=compact
	kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic market.ticks.dedup --partitions 3 --replication-factor 1 --config cleanup.policy=compact

DEMO_BROKERS ?= localhost:9092
demo:
	@command -v go >/dev/null || { echo "Go is required; install the version in go.work."; exit 1; }
	@$(MAKE) --no-print-directory -s build
	@cd process && GOWORK=off go run ./cmd/demo -root .. -brokers "$(DEMO_BROKERS)"

DURATION ?= 10s
INPUT_TPS ?= 10000
demo-performance:
	@command -v go >/dev/null || { echo "Go is required; install the version in go.work."; exit 1; }
	@$(MAKE) --no-print-directory -s build
	@cd process && GOWORK=off go run ./cmd/demo -root .. -brokers "$(DEMO_BROKERS)" -performance -duration "$(DURATION)" -input-tps "$(INPUT_TPS)"
