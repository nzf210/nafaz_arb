package scanner

import (
	"encoding/json"
	"testing"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

func TestActivePositionExposures(t *testing.T) {
	got, err := activePositionExposures([]domain.ActivePosition{{
		LocalExchange: "tokocrypto", GlobalExchange: "okx",
		Symbol: "toko:btc_idr", Qty: 0.25,
	}})
	if err != nil { t.Fatal(err) }
	if len(got) != 1 || got[0].BaseAsset != "BTC" || got[0].QuoteAsset != "IDR" ||
		got[0].Exchange != "tokocrypto" || !got[0].Managed || got[0].Kind != "POSITION" {
		t.Fatalf("unexpected exposure: %+v", got)
	}
}

func TestActivePositionExposuresFailClosedOnUnknownPair(t *testing.T) {
	_, err := activePositionExposures([]domain.ActivePosition{{LocalExchange:"indodax",Symbol:"BTC",Qty:1}})
	if err == nil { t.Fatal("expected incomplete active position identity to fail") }
}

func TestActiveOrderExposures(t *testing.T) {
	records := []json.RawMessage{
		json.RawMessage(`{"exchange":"okx","market_type":"PERPETUAL","symbol":"BTC-USDT-SWAP","status":"partially_filled","remaining_quantity":0.4,"recovery_status":"ADOPTED"}`),
		json.RawMessage(`{"exchange":"okx","market_type":"PERPETUAL","symbol":"ETH-USDT-SWAP","status":"filled","remaining_quantity":0,"recovery_status":"MANAGED"}`),
		json.RawMessage(`{"exchange":"mexc","market_type":"PERPETUAL","symbol":"mexc:SOL_USDT","status":"mystery","remaining_quantity":1,"recovery_status":"UNVERIFIED"}`),
	}
	got, err := activeOrderExposures(records)
	if err != nil { t.Fatal(err) }
	if len(got) != 2 { t.Fatalf("got %d active orders, want 2",len(got)) }
	if got[0].Kind!="OPEN_ORDER" || !got[0].Managed || got[0].Quantity!=0.4 { t.Fatalf("unexpected managed order: %+v",got[0]) }
	if got[1].Managed { t.Fatalf("unverified order must not be treated as managed: %+v",got[1]) }
}

func TestActiveOrderExposuresFailClosedOnUnknownQuantity(t *testing.T) {
	_, err := activeOrderExposures([]json.RawMessage{json.RawMessage(`{"exchange":"okx","symbol":"BTC-USDT-SWAP","status":"open"}`)})
	if err == nil { t.Fatal("expected unknown order quantity to fail") }
}
