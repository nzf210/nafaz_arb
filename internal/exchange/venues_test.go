package exchange

import (
	"context"
	"testing"
)

func TestVenueRegistryAndModules(t *testing.T) {
	adapter := NewLiveAdapter()
	reg := adapter.venues()

	// Verify Spot Venues
	for _, vName := range []string{"indodax", "tokocrypto", "mexc"} {
		spotMod, ok := reg.GetSpot(vName)
		if !ok || spotMod == nil {
			t.Fatalf("expected spot venue %s registered", vName)
		}
		if spotMod.Venue() != vName {
			t.Errorf("expected venue name %s, got %s", vName, spotMod.Venue())
		}
	}

	// Verify Perp Venues
	for _, vName := range []string{"okx", "mexc"} {
		perpMod, ok := reg.GetPerp(vName)
		if !ok || perpMod == nil {
			t.Fatalf("expected perp venue %s registered", vName)
		}
		if perpMod.Venue() != vName {
			t.Errorf("expected venue name %s, got %s", vName, perpMod.Venue())
		}
	}

	// Verify FX Venue implementations
	ctx := context.Background()
	indodaxFX := adapter.indodaxVenue()
	if indodaxFX.Venue() != "indodax" {
		t.Errorf("expected indodax venue")
	}

	tokoFX := adapter.tokocryptoVenue()
	if tokoFX.Venue() != "tokocrypto" {
		t.Errorf("expected tokocrypto venue")
	}

	// Call with empty baseURL should return error gracefully without panic
	adapter.tokocryptoBaseURL = ""
	_, err := tokoFX.FetchUSDTIDRRate(ctx)
	if err == nil {
		t.Errorf("expected error when tokocryptoBaseURL is empty")
	}
}
