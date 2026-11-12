package redishandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	redis "github.com/redis/go-redis/v9"
)

func (c *Client) ListActivePositions(ctx context.Context) ([]domain.ActivePosition, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis is not connected")
	}
	var out []domain.ActivePosition
	var cursor uint64
	for {
		keys, nextCursor, err := c.rdb.Scan(ctx, cursor, KeyPrefixActivePos+"*", 100).Result()
		if err != nil { return nil, fmt.Errorf("scan active positions: %w", err) }
		for _, key := range keys {
			data, readErr := c.rdb.Get(ctx, key).Bytes()
			if errors.Is(readErr, redis.Nil) { continue } // closed while the scan was in progress
			if readErr != nil { return nil, fmt.Errorf("read active position %s: %w", key, readErr) }
			var pos domain.ActivePosition
			if err := json.Unmarshal(data, &pos); err != nil {
				return nil, fmt.Errorf("decode active position %s: %w", key, err)
			}
			if pos.Qty <= 0 { continue }
			if strings.TrimSpace(pos.LocalExchange) == "" ||
				strings.TrimSpace(pos.GlobalExchange) == "" ||
				strings.TrimSpace(pos.Symbol) == "" {
				return nil, fmt.Errorf("active position %s has incomplete venue identity", key)
			}
			out = append(out, pos)
		}
		cursor = nextCursor
		if cursor == 0 { break }
	}
	return out, nil
}


 
// ListActiveOrderRecords reads the executor's durable-in-Redis active order
// registry. The index is maintained by UniversalOrderRegistry and excludes
// terminal orders. An unavailable index is an error, never an empty registry.

func (c *Client) ListActiveOrderRecords(ctx context.Context) ([]json.RawMessage, error) {
	if c == nil || c.rdb == nil { return nil, errors.New("redis is not connected") }
	var out []json.RawMessage
	var cursor uint64
	for {
		fields, nextCursor, err := c.rdb.HScan(ctx, KeyOrderRecoveryIndex, cursor, "*", 100).Result()
		if err != nil { return nil, fmt.Errorf("scan active order registry index: %w", err) }
		for i := 0; i+1 < len(fields); i += 2 {
			id := fields[i]
			raw, getErr := c.rdb.Get(ctx, KeyPrefixOrderRecord+id).Bytes()
			if getErr != nil {
				return nil, fmt.Errorf("read indexed active order record %s: %w", id, getErr)
			}
			if !json.Valid(raw) { return nil, fmt.Errorf("order record %s contains invalid JSON", id) }
			out = append(out, json.RawMessage(raw))
		}
		cursor = nextCursor
		if cursor == 0 { break }
	}
	return out, nil
}

// GetOrderRecord reads one canonical local order ID, including terminal
// records retained outside the active-order index.

func (c *Client) GetOrderRecord(ctx context.Context, localOrderID string) (json.RawMessage, bool, error) {
	if c == nil || c.rdb == nil { return nil, false, errors.New("redis is not connected") }
	localOrderID = strings.TrimSpace(localOrderID)
	if localOrderID == "" { return nil, false, errors.New("local order ID is required") }
	raw, err := c.rdb.Get(ctx, KeyPrefixOrderRecord+localOrderID).Bytes()
	if err == redis.Nil { return nil, false, nil }
	if err != nil { return nil, false, fmt.Errorf("read order record %s: %w", localOrderID, err) }
	if !json.Valid(raw) { return nil, false, fmt.Errorf("order record %s contains invalid JSON", localOrderID) }
	return json.RawMessage(raw), true, nil
}

// ListOrphanOrderRecords returns every indexed exchange order that recovery
// could not safely identify. Redis errors are surfaced to avoid false-empty UI.

func (c *Client) ListOrphanOrderRecords(ctx context.Context) ([]json.RawMessage, error) {
	if c == nil || c.rdb == nil { return nil, errors.New("redis is not connected") }
	ids, err := c.rdb.HKeys(ctx, KeyOrphanOrderIndex).Result()
	if err != nil { return nil, fmt.Errorf("read orphan order index: %w", err) }
	out := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		raw, getErr := c.rdb.Get(ctx, KeyPrefixOrphanOrder+id).Bytes()
		if getErr != nil {
			if getErr == redis.Nil { continue }
			return nil, fmt.Errorf("read orphan order %s: %w", id, getErr)
		}
		if !json.Valid(raw) { return nil, fmt.Errorf("orphan order %s contains invalid JSON", id) }
		out = append(out, json.RawMessage(raw))
	}
	return out, nil
}

// GetPendingOrderRecovery returns order IDs and reasons currently blocked by
// recovery. It is read-only and does not change the recovery gate.

func (c *Client) GetPendingOrderRecovery(ctx context.Context) (map[string]string, error) {
	if c == nil || c.rdb == nil { return nil, errors.New("redis is not connected") }
	values, err := c.rdb.HGetAll(ctx, KeyPendingOrderRecovery).Result()
	if err != nil { return nil, fmt.Errorf("read pending order recovery: %w", err) }
	return values, nil
}

// PublishForceClose broadcasts a force-close command to the Executor via Redis Pub/Sub.
