package exchange

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestVenueBookSupervisorBuffersUntilSnapshotBootstrap(t *testing.T) {
	var snapshotReady atomic.Bool
	gotUpdate := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil { return }
		defer conn.CloseNow()
		_ = conn.Write(r.Context(), websocket.MessageText, []byte("depth"))
		<-r.Context().Done()
	}))
	defer server.Close()
	supervisor := newVenueBookSupervisor("test",
		func(string) string { return "ws" + strings.TrimPrefix(server.URL, "http") },
		func(string) []byte { return nil },
		func(ctx context.Context, _ string) error {
			select { case <-ctx.Done(): return ctx.Err(); case <-time.After(40*time.Millisecond): snapshotReady.Store(true); return nil }
		},
		func(_ string, payload []byte) error {
			if !snapshotReady.Load() { t.Error("depth update applied before snapshot bootstrap") }
			if string(payload) != "depth" { t.Errorf("unexpected payload %q", payload) }
			gotUpdate <- struct{}{}
			return nil
		}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	supervisor.Start(ctx)
	supervisor.SetSymbols([]string{"BTCUSDT"})
	select {
	case <-gotUpdate:
	case <-time.After(2*time.Second): t.Fatal("buffered depth update was not replayed after snapshot")
	}
	cancel()
	supervisor.Stop()
}


func TestTokocryptoSupervisorUsesDocumentedRawStreamPath(t *testing.T) {
	supervisor := (&LiveAdapter{}).newTokocryptoSupervisor()
	if got, want := supervisor.wsURL("BTCUSDT"), "wss://stream-cloud.tokocrypto.site/stream/ws/btcusdt@depth"; got != want {
		t.Fatalf("Tokocrypto raw stream URL = %q, want %q", got, want)
	}
}


func TestTokocryptoNextMeSupervisorUsesDedicatedHostAndSubscription(t *testing.T) {
	adapter := &LiveAdapter{tokocryptoSymbolTypes: map[string]int{"BTCIDR": 3}}
	supervisor := adapter.newTokocryptoSupervisor()
	if got, want := supervisor.wsURL("BTCIDR"), "wss://stream-toko.2meta.app"; got != want {
		t.Fatalf("NextMe WebSocket URL = %q, want %q", got, want)
	}
	var frame struct {
		Method string `json:"method"`
		Params []string `json:"params"`
		ID int `json:"id"`
	}
	if err := json.Unmarshal(supervisor.subscribe("BTCIDR"), &frame); err != nil { t.Fatal(err) }
	if frame.Method != "SUBSCRIBE" || len(frame.Params) != 1 || frame.Params[0] != "btcidr@depth" || frame.ID != 1 {
		t.Fatalf("unexpected NextMe subscribe frame: %+v", frame)
	}
}
