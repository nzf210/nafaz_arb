package analysis

import (
	"context"
	"errors"
	"testing"
)

type testStrategy struct {
	name string
	signals []Signal
	err error
}

func (s testStrategy) Name() string { return s.name }
func (s testStrategy) Analyze(context.Context, Snapshot) ([]Signal, error) {
	return s.signals, s.err
}

func TestEngineOrdersSignalsAndIsolatesFailures(t *testing.T) {
	engine, err := NewEngine(
		testStrategy{name: "momentum", signals: []Signal{{Instrument: "BTCUSDT", Venue: "okx", Side: SideLong, Confidence: .7}}},
		testStrategy{name: "broken", err: errors.New("feed unavailable")},
		testStrategy{name: "mean-reversion", signals: []Signal{{Instrument: "ETHUSDT", Venue: "mexc", Side: SideShort, Confidence: .9}}},
	)
	if err != nil { t.Fatal(err) }
	signals, failures := engine.Analyze(context.Background(), Snapshot{})
	if len(signals) != 2 { t.Fatalf("signals=%d, want 2", len(signals)) }
	if len(failures) != 1 { t.Fatalf("failures=%d, want 1", len(failures)) }
	if signals[0].Instrument != "ETHUSDT" || signals[0].Strategy != "mean-reversion" {
		t.Fatalf("unexpected ranking: %+v", signals)
	}
}

func TestEngineRejectsDuplicateStrategy(t *testing.T) {
	_, err := NewEngine(
		testStrategy{name: "momentum"},
		testStrategy{name: "MOMENTUM"},
	)
	if err == nil { t.Fatal("expected duplicate strategy error") }
}

func TestEngineRejectsInvalidSignal(t *testing.T) {
	engine, err := NewEngine(testStrategy{name: "bad", signals: []Signal{{
		Instrument: "BTCUSDT", Venue: "okx", Side: SideLong, Confidence: 1.5,
	}}})
	if err != nil { t.Fatal(err) }
	signals, failures := engine.Analyze(context.Background(), Snapshot{})
	if len(signals) != 0 || len(failures) != 1 {
		t.Fatalf("signals=%d failures=%d, want 0 and 1", len(signals), len(failures))
	}
}
