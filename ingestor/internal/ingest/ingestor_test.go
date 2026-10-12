package ingest

import (
	"context"
	"encoding/binary"
	"errors"
	"stream-eos/pkg/protocol"
	"testing"
)

type fakeSource struct {
	message []byte
	err     error
	reads   int
}

func (s *fakeSource) Read(context.Context) ([]byte, error) { s.reads++; return s.message, s.err }

type fakePublisher struct {
	err   error
	calls int
	ids   [][24]byte
}

func (p *fakePublisher) Publish(_ context.Context, events []protocol.Event) error {
	p.calls++
	for _, e := range events {
		p.ids = append(p.ids, e.ID)
	}
	return p.err
}
func frame() []byte {
	p := make([]byte, 216)
	copy(p, "SMK1")
	p[4] = 9
	binary.BigEndian.PutUint16(p[20:22], 1)
	binary.BigEndian.PutUint64(p[22:30], 401)
	binary.BigEndian.PutUint16(p[30:32], 184)
	binary.BigEndian.PutUint32(p[32:36], 9910001)
	return p
}
func TestRunPreservesDuplicateIdentity(t *testing.T) {
	s := &fakeSource{message: frame()}
	p := &fakePublisher{}
	runner, _ := New(s, p)
	stats, err := runner.Run(context.Background(), 2)
	if err != nil || stats.Messages != 2 || stats.Events != 2 || s.reads != 2 || p.calls != 2 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	if p.ids[0] != p.ids[1] || binary.BigEndian.Uint64(p.ids[0][16:]) != 401 {
		t.Fatal("source identity changed")
	}
}
func TestFailuresStopBeforeNextRead(t *testing.T) {
	failure := errors.New("failure")
	for _, tc := range []struct {
		name                string
		message             []byte
		readErr, publishErr error
		calls               int
	}{
		{name: "read", readErr: failure}, {name: "decode", message: []byte("bad")},
		{name: "publish", message: frame(), publishErr: failure, calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &fakeSource{message: tc.message, err: tc.readErr}
			p := &fakePublisher{err: tc.publishErr}
			runner, _ := New(s, p)
			stats, err := runner.Run(context.Background(), 2)
			if err == nil || stats != (Stats{}) || s.reads != 1 || p.calls != tc.calls {
				t.Fatalf("stats=%+v reads=%d calls=%d err=%v", stats, s.reads, p.calls, err)
			}
			if tc.name != "decode" && !errors.Is(err, failure) {
				t.Fatal("error identity lost")
			}
		})
	}
}
func TestCanceledRunDoesNotRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &fakeSource{}
	p := &fakePublisher{}
	runner, _ := New(s, p)
	_, err := runner.Run(ctx, 0)
	if !errors.Is(err, context.Canceled) || s.reads != 0 || p.calls != 0 {
		t.Fatal("canceled run performed I/O")
	}
}

func TestRunClearsBorrowedEventsAfterPublish(t *testing.T) {
	for _, failure := range []error{nil, errors.New("publish failed")} {
		source := &fakeSource{message: frame()}
		publisher := &fakePublisher{err: failure}
		runner, _ := New(source, publisher)
		_, err := runner.Run(context.Background(), 1)
		if (err == nil) != (failure == nil) {
			t.Fatal(err)
		}
		if len(runner.events) != 0 {
			t.Fatal("active event slice retained after publish")
		}
		for _, event := range runner.events[:cap(runner.events)] {
			if event.Quote != nil || event.ID != (protocol.EventID{}) || event.Instrument != 0 {
				t.Fatal("borrowed event references retained")
			}
		}
	}
}
