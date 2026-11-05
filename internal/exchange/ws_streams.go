package exchange

import (
	"encoding/json"
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

// okxBookStream subscribes to the aggregate BBO/TBT channel for all instruments
// at once, so no per-symbol bookkeeping is needed on our side.
func okxBookStream(baseURL string) WSStream {
	return WSStream{
		Name: "okx-books",
		URL:  websocketURL(baseURL, "/ws/v5/public"),
		Subscribe: func() ([][]byte, error) {
			frame, err := jsonFrame(map[string]any{
				"op": "subscribe",
				"args": []map[string]string{
					{"channel": "books5", "instId": "ANY"},
				},
			})
			if err != nil {
				return nil, err
			}
			return [][]byte{frame}, nil
		},
		Handle: func(msg []byte) (domain.OrderBook, bool, error) {
			var probe struct {
				Event string `json:"event"`
				Arg   struct {
					InstID string `json:"instId"`
				} `json:"arg"`
				Data []struct {
					Bids [][]string `json:"bids"`
					Asks [][]string `json:"asks"`
				} `json:"data"`
			}
			if err := json.Unmarshal(msg, &probe); err != nil {
				return domain.OrderBook{}, true, err
			}
			if probe.Event != "" || len(probe.Data) == 0 || probe.Arg.InstID == "" {
				return domain.OrderBook{}, true, nil
			}
			book, err := decodeOKXBook(probe.Arg.InstID, msg, 1.0, time.Now())
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
