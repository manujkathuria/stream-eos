package protocol

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"
)

func TestBuildMessageIdentityFilteringAndOwnership(t *testing.T) {
	update := Update{Packets: make([][184]byte, MaxPackets)}
	for i := range update.Packets {
		binary.BigEndian.PutUint32(update.Packets[i][:4], 9910001+uint32(i)*256)
	}
	streamID := [16]byte{1, 2, 3}
	before := append([][184]byte(nil), update.Packets...)
	var active [MaxPackets]bool
	active[0], active[399] = true, true
	storage := bytes.Repeat([]byte{0xff}, 500)
	length, err := BuildMessage(storage, &update, streamID, 5, &active)
	if err != nil || length != 410 {
		t.Fatalf("length=%d err=%v", length, err)
	}
	if string(storage[:4]) != "SMK1" || !bytes.Equal(storage[4:20], streamID[:]) || binary.BigEndian.Uint16(storage[20:22]) != 2 {
		t.Fatal("header")
	}
	if binary.BigEndian.Uint64(storage[22:30]) != 2001 || binary.BigEndian.Uint64(storage[216:224]) != 2400 {
		t.Fatal("filtered identities renumbered")
	}
	if binary.BigEndian.Uint16(storage[30:32]) != 184 || binary.BigEndian.Uint32(storage[226:230]) != 10012145 {
		t.Fatal("packet framing")
	}
	if !slices.Equal(update.Packets, before) || !active[0] || !active[399] {
		t.Fatal("construction changed its inputs")
	}
	if !bytes.Equal(storage[410:], bytes.Repeat([]byte{0xff}, 90)) {
		t.Fatal("wrote beyond valid message")
	}
}

func TestBuildMessageRejectsInsufficientStorage(t *testing.T) {
	update := Update{Packets: make([][184]byte, MaxPackets)}
	var active [MaxPackets]bool
	active[0], active[1] = true, true
	for _, size := range []int{0, 21, 215, 409, 410} {
		length, err := BuildMessage(make([]byte, size), &update, [16]byte{}, 0, &active)
		if (err == nil) != (size == 410) {
			t.Fatalf("size=%d length=%d err=%v", size, length, err)
		}
	}
}
