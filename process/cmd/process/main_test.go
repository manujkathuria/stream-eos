package main

import "testing"

func TestParseOffset(t *testing.T) {
	for input, want := range map[string]int64{"earliest": -2, "latest": -1, "0": 0, "42": 42} {
		got, err := parseOffset(input)
		if err != nil || got != want {
			t.Fatalf("%s: %d %v", input, got, err)
		}
	}
	for _, input := range []string{"", "-1", "other", "9223372036854775808"} {
		if _, err := parseOffset(input); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
}
