package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// WSStream describes one subscription: where to connect, what to send once
// connected, and how to turn a raw frame into a book update.
type WSStream struct {
	// Name is used in logs only.
	Name string
	// URL is the websocket endpoint.
	URL string
	// Subscribe returns frames to send on (re)connect. Empty means a read-only
	// stream that needs no subscription message.
	Subscribe func() ([][]byte, error)
	// Handle decodes one frame. Returning skip=true means "not a book update",
	// which is normal for heartbeat or acknowledgement frames.
	Handle func(msg []byte) (book domain.OrderBook, skip bool, err error)
	// OnConnected runs after subscriptions are sent and before the read loop; it must return quickly.
	OnConnected func(ctx context.Context)
	KeepAliveInterval time.Duration
}

// WSClient owns the websocket connections and the book cache they feed.
// Every stream reconnects with exponential backoff; because REST remains the
// fallback path, a permanently broken websocket degrades throughput rather than
// breaking the scanner.
type WSClient struct {
	streams   []WSStream
	cache     *WSBookCache
	log       *slog.Logger
	baseDelay time.Duration
	maxDelay  time.Duration

	mu       sync.Mutex
	cancelFn context.CancelFunc
	done     chan struct{}
	// message counts and connection attempts make health visible without
	// attaching a debugger.
	connected int64
	failures  int64
}

// NewWSClient builds a client that will stream the given feeds into cache.
func NewWSClient(cache *WSBookCache, log *slog.Logger, streams ...WSStream) *WSClient {
	if log == nil {
		log = slog.Default()
	}
	return &WSClient{
		streams:   streams,
		cache:     cache,
		log:       log,
		baseDelay: 1 * time.Second,
		maxDelay:  30 * time.Second,
	}
}

// Start launches one goroutine per stream. Calling Start twice is a no-op.
func (c *WSClient) Start(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancelFn != nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.cancelFn = cancel
	c.done = make(chan struct{})

	var wg sync.WaitGroup
	for _, stream := range c.streams {
		wg.Add(1)
		go func(s WSStream) {
			defer wg.Done()
			c.runStream(runCtx, s)
		}(stream)
	}
	go func() {
		wg.Wait()
		close(c.done)
	}()
}

// Stop cancels every connection and waits for the goroutines to exit.
func (c *WSClient) Stop() {
	c.mu.Lock()
	cancel, done := c.cancelFn, c.done
	c.cancelFn = nil
	c.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// Stats reports connection attempts and accumulated failures, for diagnostics.
func (c *WSClient) Stats() (connected, failures int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected, c.failures
}

func (c *WSClient) runStream(ctx context.Context, s WSStream) {
	delay := c.baseDelay
	for ctx.Err() == nil {
		err := c.consume(ctx, s)
		if ctx.Err() != nil {
			return
		}
		c.mu.Lock()
		c.failures++
		c.mu.Unlock()
		// Record the error against the exchange so the ConnectionHealth tracker
		// reflects the broken feed. Without this the health tracker is only ever
		// populated by explicit RecordError calls that nobody makes, so
		// IsHealthy always returns false (or true by default) regardless of what
		// the websocket is actually doing.
		if c.cache != nil {
			c.cache.RecordExchangeError(streamExchange(s), err)
		}
		if err != nil {
			c.log.Warn("websocket stream ended, reconnecting", "stream", s.Name, "err", err, "retry_in", delay)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		// Back off on repeated failures, reset implicitly by a successful run.
		if delay < c.maxDelay {
			delay *= 2
			if delay > c.maxDelay {
				delay = c.maxDelay
			}
		}
	}
}

// streamExchange extracts the exchange name from a stream's name. The naming
// convention is consistent: every stream created in this package embeds the
// exchange slug (indodax-depth, okx-books, mexc-spot-depth).
func streamExchange(s WSStream) string {
	name := strings.ToLower(s.Name)
	for _, ex := range []string{"indodax", "okx", "mexc", "tokocrypto"} {
		if strings.Contains(name, ex) {
			return ex
		}
	}
	return ""
}

// consume dials once and reads until the socket fails or the context ends.
func (c *WSClient) consume(ctx context.Context, s WSStream) error {
	conn, _, err := websocket.Dial(ctx, s.URL, &websocket.DialOptions{
		HTTPClient: wsHTTPClient(),
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()

	c.mu.Lock()
	c.connected++
	c.mu.Unlock()

	// A successful dial restores the exchange's connection health so the
	// scanner resumes using this venue's feed.
	if c.cache != nil {
		c.cache.RecordReconnect(streamExchange(s), "")
	}

	if s.Subscribe != nil {
		frames, err := s.Subscribe()
		if err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}
		for _, frame := range frames {
			if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
				return fmt.Errorf("write subscribe: %w", err)
			}
		}
	}
	if s.OnConnected != nil {
		s.OnConnected(ctx)
	}
	if s.KeepAliveInterval > 0 {
		go c.keepAlive(ctx, conn, s)
	}

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		book, skip, err := s.Handle(data)
		if err != nil {
			// A malformed frame must not kill a healthy stream.
			c.log.Debug("dropping undecodable websocket frame", "stream", s.Name, "err", err)
			continue
		}
		if skip {
			continue
		}
		if c.cache != nil {
			isDuplicate, isOutOfOrder, gapDetected, err := c.cache.Store(book)
			if err != nil {
				c.log.Debug("failed to store websocket book", "stream", s.Name, "err", err)
				continue
			}
			if isDuplicate {
				c.log.Debug("duplicate sequence detected", "stream", s.Name, "symbol", book.Symbol)
			}
			if isOutOfOrder {
				c.log.Warn("out-of-order sequence detected", "stream", s.Name, "symbol", book.Symbol)
			}
			if gapDetected {
				c.log.Warn("sequence gap detected", "stream", s.Name, "symbol", book.Symbol)
			}
		}
	}
}

func (c *WSClient) keepAlive(ctx context.Context, conn *websocket.Conn, stream WSStream) {
	ticker := time.NewTicker(stream.KeepAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, stream.KeepAliveInterval)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				c.log.Warn("websocket keepalive failed", "stream", stream.Name, "err", err)
				_ = conn.CloseNow()
				return
			}
			if c.cache != nil { c.cache.TouchExchange(streamExchange(stream)) }
		}
	}
}

func jsonFrame(v any) ([]byte, error) {
	return json.Marshal(v)
}

// wsHTTPClient reuses the adapter's resolver (some Indonesian venues need DoH
// because of local DNS interception) and identifies itself like the REST client.
func wsHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	tr := &http.Transport{
		ResponseHeaderTimeout: 10 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return dialer.DialContext(ctx, network, addr)
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(resolveHost(ctx, host), port))
		},
	}
	return &http.Client{
		Transport: tr,
		// Do not set http.Client.Timeout: it applies to the lifetime of the
		// upgraded response body and can terminate a persistent WebSocket.
		// The dialer's Timeout bounds TCP establishment; the parent context
		// controls the lifetime of the socket.
		// websocket.Dial performs the upgrade itself; a redirect handler that
		// rewrites the User-Agent keeps the handshake looking like the REST calls.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			req.Header.Set(USER_AGENT, USER_AGENT_VALUE)
			return nil
		},
	}
}
