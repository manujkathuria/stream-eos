package protocol

// DedupState describes the committed FIFO ring, not a wire-memory layout.
// Next is the slot to replace/append; Count is the number of occupied slots.
type DedupState struct {
	Capacity uint32
	Count    uint32
	Next     uint32
}
