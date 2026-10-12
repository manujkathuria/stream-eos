# Why we built it this way

The goal is to make received live events replayable, process duplicate deliveries safely, and recover consistent state after a restart.

| Decision | Why | Tradeoff |
| --- | --- | --- |
| Store received events in Kafka before processing. | Processing can pause and replay retained input. | Events lost before reaching Kafka remain unrecoverable. |
| Run ingestors A and B independently. | Reduces dependence on one connection or process. | Both can publish the same event, increasing input traffic and storage. At least one path must receive and successfully publish it. |
| Assign stable event IDs at the source. | Copies retain their identity even with different Kafka offsets. | Requires a source identity contract; equal prices alone cannot identify duplicates. |
| Keep an exact FIFO of recent IDs per partition. | Bounds duplicate history and preserves predictable eviction order. | Default capacity is one million unique IDs. Evicted IDs can produce output again; duplicate reads do not refresh their position. |
| Assign each partition explicitly to one sequential processor. | Keeps dedup state, buffer ownership, and commit order straightforward. | Scaling is across partitions. A stopped owner needs replacement; automatic reassignment is not implemented. |
| Commit output, FIFO changes, and progress in one Kafka transaction. | A restart restores matching output progress and duplicate history. | Commits add latency. State topics require compaction, and output readers must use `read_committed`. |
| Reuse bounded buffers and measure each stage. | Reduces application allocation churn and makes costs visible. | Kafka and WebSocket clients still allocate. Producer tuning remains a measured optimization opportunity. |

Stable transactional IDs fence a replaced processor. Startup restores the saved FIFO and its eviction order before consuming. Input progress is an application checkpoint in Kafka, rather than a consumer-group offset commit. A failed or uncertain transaction stops processing; restart resolves the committed state.

The implementation is locally verified with unit tests, race checks, and real Kafka transaction/recovery tests. The source is synthetic, the output boundary is Kafka, and local tests use one broker. Sustained full-pipeline throughput and multi-broker failure tolerance remain future work.

Follow the [data journey](journey.md) for the overview, or the [process journey](../process/docs/journey.md) and [benchmark results](../process/BENCHMARKS.md) for implementation evidence.

For a concrete implementation example, follow [deduplication from pending IDs to the committed FIFO](deduplication.md).
