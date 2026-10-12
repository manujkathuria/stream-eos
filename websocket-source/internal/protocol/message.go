package protocol

import (
	"encoding/binary"
	"fmt"
)

// BuildMessage writes a sequenced envelope into caller-owned storage and returns
// its valid length. It borrows update and active only for the call, does not
// modify them, and performs no generation, allocation on success, or I/O.
// Discard the output on error: insufficient storage may leave partial bytes.
func BuildMessage(message []byte, update *Update, streamID [16]byte, position uint64, active *[MaxPackets]bool) (int, error) {
	if len(message) < 22 {
		return 0, fmt.Errorf("message storage needs at least 22 header bytes")
	}
	copy(message[:4], "SMK1")
	copy(message[4:20], streamID[:])
	length := 22
	var count uint16
	for i := range update.Packets {
		enabled := active[i]
		if !enabled {
			continue
		}
		if len(message)-length < 194 {
			return 0, fmt.Errorf("message storage too small for subscribed packets")
		}
		// Filtering does not renumber events: all instruments advance together.
		binary.BigEndian.PutUint64(message[length:length+8], position*MaxPackets+uint64(i)+1)
		binary.BigEndian.PutUint16(message[length+8:length+10], 184)
		copy(message[length+10:length+194], update.Packets[i][:])
		length += 194
		count++
	}
	binary.BigEndian.PutUint16(message[20:22], count)
	return length, nil
}

// Update holds the encoded quote packets for one market position. The market
// owns mutation; message construction borrows it only while copying.
type Update struct {
	Packets [][184]byte // Borrowed from the market’s fixed backing storage.
}

const MaxPackets = 400
const MaxMessageBytes = 22 + MaxPackets*194
