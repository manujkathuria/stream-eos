package protocol

// EventID is a 16-byte source-run identity followed by an eight-byte big-endian
// sequence. Independent source runs may reuse sequence numbers safely.
type EventID [24]byte
