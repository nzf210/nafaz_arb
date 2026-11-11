package exchange

import (
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// websocketURL converts an https base URL into its wss equivalent.
func websocketURL(base, path string) string {
	u := strings.TrimSuffix(base, "/")
	u = strings.Replace(u, "https://", "wss://", 1)
	u = strings.Replace(u, "http://", "ws://", 1)
	return u + path
}

// indodaxDepthStream subscribes to the Indodax public depth channel. Indodax
// needs an explicit subscribe frame per pair, which we cannot enumerate up front,
// so the stream is left read-only: it only consumes whatever the venue pushes.
// Venues that require per-symbol subscription get no coverage here and keep using
// REST, which is exactly the fallback contract this package promises.
func indodaxDepthStream(baseURL string) WSStream {
	return WSStream{
		Name: "indodax-depth",
		URL:  websocketURL(baseURL, "/ws"),
		Handle: func(msg []byte) (domain.OrderBook, bool, error) {
			book, err := decodeIndodaxDepth("", msg, time.Now())
			return book, false, err
		},
	}
}

// mexcSpotDepthStream follows MEXC spot's per-symbol depth feed. Like Indodax it
// needs explicit subscriptions, so it is read-only and mostly relies on REST.
func mexcSpotDepthStream(baseURL string) WSStream {
	return WSStream{
		Name: "mexc-spot-depth",
		URL:  websocketURL(baseURL, "/ws"),
		Handle: func(msg []byte) (domain.OrderBook, bool, error) {
			book, err := decodeMEXCSpotDepth("", msg, time.Now())
			return book, false, err
		},
	}
}
