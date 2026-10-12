package websocket

import "testing"

func TestSubscriptionTransitions(t *testing.T) {
	s := subscriptions{instrumentCount: 400}
	if s.apply([]byte(`{"a":"subscribe","v":[9910001,9]}`)) == "" || s.subscribed[0] {
		t.Fatal("atomic validation")
	}
	if s.apply([]byte(`{"a":"mode","v":["full",[9910001]]}`)) == "" {
		t.Fatal("requires subscription")
	}
	for _, raw := range []string{`{"a":"subscribe","v":[9910001,9910001]}`, `{"a":"mode","v":["full",[9910001]]}`} {
		if e := s.apply([]byte(raw)); e != "" {
			t.Fatal(e)
		}
	}
	if !s.full[0] {
		t.Fatal("not full")
	}
	if s.apply([]byte(`{"a":"unsubscribe","v":[9910001]}`)) != "" || s.full[0] || s.subscribed[0] {
		t.Fatal("unsubscribe")
	}
}

func TestSubscriptionRejectsUnconfiguredInstrument(t *testing.T) {
	s := subscriptions{instrumentCount: 10}
	if s.apply([]byte(`{"a":"subscribe","v":[9910001,9912561]}`)) == "" {
		t.Fatal("accepted eleventh instrument")
	}
	if s.subscribed[0] {
		t.Fatal("partially applied invalid subscription")
	}
	if err := s.apply([]byte(`{"a":"subscribe","v":[9912305]}`)); err != "" {
		t.Fatalf("tenth instrument rejected: %s", err)
	}
}
