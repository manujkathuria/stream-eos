# Processing journey

## 1. Start and recover

[cmd/process](../cmd/process/main.go) loads typed [configuration](../cmd/process/config.go), configures the shared logger, and creates a signal-aware context. The transactional producer initializes first, fencing the previous partition owner. It loads progress and restores FIFO membership and eviction order before constructing the input consumer. Legacy bootstrap is a separate explicit [migration](../cmd/process/bootstrap.go).

## 2. Read one partition

[Consumer.ReadBatch](../internal/kafka/consumer.go) polls the explicitly configured partition into caller-owned, bounded record storage. Each record owns its fixed-size key/value bytes. There is no group assignment or automatic offset commit. A saved checkpoint overrides the initial offset setting.

## 3. Stage unique IDs

[Processor.Process](../internal/engine/processor.go) checks event IDs against the [partition FIFO](../internal/dedup/partition.go) and the current pending batch. Duplicates are skipped. New IDs remain pending until Kafka confirms the transaction; committed membership is not changed early.

See the [deduplication walkthrough](deduplication.md) for the ring example, actual Go structures, ownership, and recovery rules.

## 4. Encode JSON

[JSON encoding](../internal/engine/json.go) reads the unique binary quotes and writes into processor-owned reusable buffers. Event identity and instrument keys are preserved. Those buffers stay valid through publication and transaction completion.

## 5. Commit together

[TransactionPublisher](../internal/kafka/transaction.go) publishes JSON output, changed FIFO slots, and the next input offset in one transaction. Confirmed commit promotes pending IDs into the local FIFO. Even an all-duplicate batch advances progress transactionally. A transaction error stops the process; a fresh owner restores committed state rather than guessing the outcome.

## 6. Stop

Cancellation unblocks polling. Transaction code retains its existing bounded completion/abort handling; the command closes owned consumer and publisher clients on every return. Kafka itself remains running. Local FIFO state is disposable because its committed form is persisted in Kafka.

## Find a benchmark

Tests and benchmarks live beside each stage: consumer polling/copying in `internal/kafka`, FIFO lookup/churn in `internal/dedup`, JSON and orchestration in `internal/engine`. `make bench` runs components; real Kafka recovery/transaction checks use `make test-kafka`. Module builds and checks run with `GOWORK=off`.
