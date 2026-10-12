# How deduplication works

Ingestors A and B can send us the same event. We use its event ID to answer: **“Have we already accepted this event?”** Kafka offsets and prices are not the identity.

We keep two sets:

- **Committed:** IDs saved by earlier successful transactions.
- **Pending:** new IDs accepted in the current batch, waiting for Kafka to confirm the transaction.

An ID found in either set is a duplicate. A new ID goes into pending. Only after Kafka confirms the transaction do we move it into committed history.

## Start with capacity 10 and a batch of 4

Imagine ten numbered slots, allocated once:

```text
slot:  0 1 2 3 4 5 6 7 8 9
FIFO: [_ _ _ _ _ _ _ _ _ _]
next = 0
```

The batch is **A, B, C, D**, all new IDs. We check each ID in the committed and pending maps, then remember it in pending.

```text
pending:   A B C D
committed: empty
FIFO:      [_ _ _ _ _ _ _ _ _ _]
```

Kafka confirms that output, duplicate history, and input progress were saved together. Now we write four slots and add those IDs to the committed map:

```text
FIFO:      [A B C D _ _ _ _ _ _]
next = 4
pending:   empty
committed: A B C D
```

For each ID, the main work is:

```go
fifo[next] = id
committed[id] = struct{}{}
next++
```

We do not move the existing IDs. `next` simply tells us where to write next.

## What happens when it is full?

After more successful batches:

```text
FIFO: [A B C D E F G H I J]
next = 0
```

The next batch contains **K, L, M, N**. They enter pending first. After Kafka confirms the transaction, adding K does this:

```go
delete(committed, fifo[next]) // Forget A.
fifo[next] = id               // Put K in slot 0.
committed[id] = struct{}{}    // Remember K.
next++                       // Slot 1 is next.
```

Repeat for L, M, and N:

```text
FIFO: [K L M N E F G H I J]
next = 4
```

The oldest remaining ID is E. Reading from slot 4 and wrapping around gives:

```text
oldest → E F G H I J K L M N → newest
```

This is a **ring buffer**. At the end of the slice, `next` wraps to zero. Nothing shifts. A, B, C, and D have been forgotten, so a later copy of one of them can be accepted again.

## What if the batch contains duplicates?

Suppose our first batch had been **A, B, A, C** instead:

| Event | What happens |
| --- | --- |
| A | New: add to pending. |
| B | New: add to pending. |
| A | Already pending: skip it. |
| C | New: add to pending. |

Only A, B, and C enter the FIFO after commit. A later batch containing A finds it in the committed map and skips it. Duplicate reads do not move IDs to the back of the FIFO.

## Why is it fast?

The map checks membership without scanning the entire history. Ring insertion writes one slot; eviction deletes one map entry. Map operations take expected constant time, and the ring is allocated before processing starts.

A batch of four performs roughly four sets of these operations—even when history contains a million IDs. Actual cost still depends on map size and memory access, but it does not scan a million entries for each event.

These are fast **in-memory operations**. Waiting for Kafka to commit is separate and takes much longer. If a transaction fails or its outcome is uncertain, processing stops and committed state is restored on restart; we do not guess that pending IDs succeeded.

Continue with [the implementation internals](../process/docs/deduplication-internals.md) for the actual struct, method flow, memory ownership, and recovery rules. See [benchmarks](../process/BENCHMARKS.md#deduplication) for measured costs.
