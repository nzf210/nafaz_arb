package redishandler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func (c *Client) RequestRecoveryRetry(ctx context.Context) (RecoveryRetryReply, error) {
	return c.requestRecoveryAction(ctx, "retry", "", "")
}

// RequestOrphanResolution asks the executor to verify and resolve one orphan.

func (c *Client) RequestOrphanResolution(ctx context.Context, orphanID, pairTag string) (RecoveryRetryReply, error) {
	orphanID = strings.TrimSpace(orphanID)
	if orphanID == "" || len(orphanID) > 256 {
		return RecoveryRetryReply{}, errors.New("orphan id tidak valid")
	}
	return c.requestRecoveryAction(ctx, "resolve_orphan", orphanID, strings.TrimSpace(pairTag))
}

func (c *Client) requestRecoveryAction(ctx context.Context, action, orphanID, pairTag string) (RecoveryRetryReply, error) {
	if c == nil || c.rdb == nil {
		return RecoveryRetryReply{}, errors.New("redis is not connected")
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return RecoveryRetryReply{}, fmt.Errorf("generate recovery request id: %w", err)
	}
	requestID := hex.EncodeToString(idBytes)
	request := map[string]string{"request_id": requestID, "action": action}
	if orphanID != "" { request["orphan_id"] = orphanID }
	if pairTag != "" { request["pair_tag"] = pairTag }
	payload, err := json.Marshal(request)
	if err != nil {
		return RecoveryRetryReply{}, fmt.Errorf("marshal recovery request: %w", err)
	}
	if err := c.rdb.Publish(ctx, ChannelRecoveryRequest, payload).Err(); err != nil {
		return RecoveryRetryReply{}, fmt.Errorf("publish recovery request: %w", err)
	}
	key := KeyPrefixRecoveryReply + requestID
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(25 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case <-ctx.Done():
			return RecoveryRetryReply{}, ctx.Err()
		case <-timeout.C:
			return RecoveryRetryReply{}, errors.New("executor tidak merespons permintaan reconciliation dalam 25 detik")
		case <-ticker.C:
			raw, err := c.rdb.Get(ctx, key).Result()
			if err == redis.Nil { continue }
			if err != nil { return RecoveryRetryReply{}, fmt.Errorf("read recovery response: %w", err) }
			var reply RecoveryRetryReply
			if err := json.Unmarshal([]byte(raw), &reply); err != nil {
				return RecoveryRetryReply{}, fmt.Errorf("decode recovery response: %w", err)
			}
			if reply.RequestID != requestID || reply.Action != action {
				return RecoveryRetryReply{}, errors.New("recovery response request identity mismatch")
			}
			return reply, nil
		}
	}
}
// RequestLiveBalances asks the running Executor to fetch fresh balances from both exchanges.

func (c *Client) RequestLiveBalances(ctx context.Context) (string, error) {
	if c == nil || c.rdb == nil { return "", errors.New("redis is not connected") }
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil { return "", fmt.Errorf("generate balance request id: %w", err) }
	requestID := hex.EncodeToString(idBytes)
	payload, err := json.Marshal(map[string]string{"request_id": requestID})
	if err != nil { return "", fmt.Errorf("marshal balance request: %w", err) }
	if err := c.rdb.Publish(ctx, ChannelBalanceRequest, payload).Err(); err != nil { return "", fmt.Errorf("publish balance request: %w", err) }
	key := KeyPrefixBalanceReply + requestID
	ticker := time.NewTicker(200 * time.Millisecond); defer ticker.Stop()
	timeout := time.NewTimer(15 * time.Second); defer timeout.Stop()
	for {
		select {
		case <-ctx.Done(): return "", ctx.Err()
		case <-timeout.C: return "", errors.New("executor tidak merespons permintaan saldo dalam 15 detik")
		case <-ticker.C:
			raw, err := c.rdb.Get(ctx, key).Result()
			if err == redis.Nil { continue }
			if err != nil { return "", fmt.Errorf("read balance response: %w", err) }
			return raw, nil
		}
	}
}
