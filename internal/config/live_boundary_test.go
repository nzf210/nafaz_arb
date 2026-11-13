package config

import (
	"strings"
	"testing"
)

func TestLiveModeRejectsScannerPrivatePositionChecks(t *testing.T) {
	cfg := Config{
		Mode: "LIVE",
		APIAuthToken: strings.Repeat("x", 32),
		CheckOpenPositions: true,
		AllocationPerPair: 0.5,
		CapitalTotal: 22.2,
	}
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "check_open_positions must be false") {
		t.Fatalf("validate() error=%v, want LIVE private-position boundary error", err)
	}
}

func TestLiveModeAllowsExecutorOwnedPositionChecks(t *testing.T) {
	cfg := Config{
		Mode: "LIVE",
		APIAuthToken: strings.Repeat("x", 32),
		CheckOpenPositions: false,
		AllocationPerPair: 0.5,
		CapitalTotal: 22.2,
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() unexpectedly rejected executor-owned position checks: %v", err)
	}
}
