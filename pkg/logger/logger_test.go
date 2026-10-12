package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func preserveDefault(t *testing.T) {
	t.Helper()
	previous := slog.Default()
	writer, flags, prefix := log.Writer(), log.Flags(), log.Prefix()
	t.Cleanup(func() { slog.SetDefault(previous); log.SetOutput(writer); log.SetFlags(flags); log.SetPrefix(prefix) })
}

type formattingProbe struct{ called *bool }

func (p formattingProbe) String() string { *p.called = true; return "formatted" }

func TestLevelsFormattingAndCaller(t *testing.T) {
	preserveDefault(t)
	var output bytes.Buffer
	var level slog.LevelVar
	level.Set(slog.LevelWarn)
	if err := New(Config{Service: "ingestor", Level: &level, Output: &output, AddSource: true}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	called := false
	Info(ctx, "hidden %s", formattingProbe{&called})
	Debug(ctx, "hidden %d", 1)
	if called || output.Len() != 0 {
		t.Fatal("disabled logging formatted or wrote")
	}
	Warn(ctx, "retry %d", 2)
	Error(ctx, "failed %s", "offline")
	level.Set(slog.LevelDebug)
	Info(ctx, "records=%d bytes=%d", 4, 798)
	Debug(ctx, "progress %d%%", 50)
	decoder := json.NewDecoder(&output)
	for _, want := range []struct{ level, message string }{{"WARN", "retry 2"}, {"ERROR", "failed offline"}, {"INFO", "records=4 bytes=798"}, {"DEBUG", "progress 50%"}} {
		var entry struct {
			Level   string `json:"level"`
			Message string `json:"msg"`
			Service string `json:"service"`
			Source  struct {
				File string
				Line int
			} `json:"source"`
		}
		if err := decoder.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		if entry.Level != want.level || entry.Message != want.message || entry.Service != "ingestor" || !strings.HasSuffix(entry.Source.File, "logger_test.go") || entry.Source.Line <= 0 {
			t.Fatalf("entry=%+v", entry)
		}
	}
	previous := slog.Default()
	if err := New(Config{Format: "invalid"}); err == nil || slog.Default() != previous {
		t.Fatal("invalid config changed logger")
	}
}
func TestTextAndConcurrentLogging(t *testing.T) {
	preserveDefault(t)
	var output bytes.Buffer
	if err := New(Config{Format: Text, Output: &output}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				Info(context.Background(), "received %d", n)
			}
		}()
	}
	wg.Wait()
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 80 {
		t.Fatalf("lines=%d", len(lines))
	}
	for _, line := range lines {
		if !strings.Contains(line, "level=INFO") || !strings.Contains(line, "msg=\"received ") {
			t.Fatalf("corrupt line=%q", line)
		}
	}
}
