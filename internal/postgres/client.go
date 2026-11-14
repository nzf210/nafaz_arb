package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type Client struct {
	db *sql.DB
}

// NewFromDSN initializes a PostgreSQL connection pool.
func NewFromDSN(dsn string) (*Client, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, nil
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Client{db: db}, nil
}

func (c *Client) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

// LoadStrategyConfig loads strategy configuration from public.strategy_configs.
func (c *Client) LoadStrategyConfig(ctx context.Context, name string) (map[string]interface{}, error) {
	if c == nil || c.db == nil {
		return nil, fmt.Errorf("postgres client not initialized")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "default"
	}
	var raw []byte
	err := c.db.QueryRowContext(ctx, "SELECT config FROM public.strategy_configs WHERE name = $1 LIMIT 1", name).Scan(&raw)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("unmarshal strategy config: %w", err)
	}
	return result, nil
}

// SaveStrategyConfig upserts strategy configuration into public.strategy_configs.
func (c *Client) SaveStrategyConfig(ctx context.Context, name string, configData map[string]interface{}, executionMode string) error {
	if c == nil || c.db == nil {
		return fmt.Errorf("postgres client not initialized")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "default"
	}
	if executionMode == "" {
		executionMode = "TAKER_TAKER"
	}
	raw, err := json.Marshal(configData)
	if err != nil {
		return fmt.Errorf("marshal strategy config: %w", err)
	}
	query := `
		INSERT INTO public.strategy_configs (name, is_enabled, execution_mode, config, updated_at)
		VALUES ($1, true, $2, $3, NOW())
		ON CONFLICT (name) DO UPDATE
		SET config = EXCLUDED.config, execution_mode = EXCLUDED.execution_mode, updated_at = NOW()
	`
	_, err = c.db.ExecContext(ctx, query, name, executionMode, raw)
	if err != nil {
		return fmt.Errorf("upsert strategy_configs: %w", err)
	}
	return nil
}

// PositionOrderFills reads orders and fills for a position.
func (c *Client) PositionOrderFills(ctx context.Context, positionID string) (map[string][]json.RawMessage, error) {
	if c == nil || c.db == nil {
		return nil, fmt.Errorf("postgres client not initialized")
	}
	positionID = strings.TrimSpace(positionID)
	if positionID == "" {
		return nil, fmt.Errorf("position_id is required")
	}

	ordersQuery := `
		SELECT json_build_object(
			'id', id, 'position_id', position_id, 'pair_key', pair_key,
			'exchange_name', exchange_name, 'market_type', market_type, 'leg', leg,
			'exchange_order_id', exchange_order_id, 'client_order_id', client_order_id,
			'symbol', symbol, 'side', side, 'status', status,
			'requested_qty', requested_qty, 'filled_qty', filled_qty,
			'remaining_qty', remaining_qty, 'limit_price', limit_price,
			'average_fill_price', average_fill_price, 'reduce_only', reduce_only,
			'submitted_at', submitted_at, 'completed_at', completed_at
		)::text
		FROM public.orders
		WHERE position_id = $1
		ORDER BY submitted_at ASC
		LIMIT 500
	`
	rows, err := c.db.QueryContext(ctx, ordersQuery, positionID)
	if err != nil {
		return nil, fmt.Errorf("query orders: %w", err)
	}
	defer rows.Close()

	var orders []json.RawMessage
	for rows.Next() {
		var str string
		if err := rows.Scan(&str); err == nil {
			orders = append(orders, json.RawMessage(str))
		}
	}

	fillsQuery := `
		SELECT json_build_object(
			'id', f.id, 'order_id', f.order_id, 'exchange_name', f.exchange_name,
			'exchange_fill_id', f.exchange_fill_id, 'qty', f.qty, 'price', f.price,
			'quote_qty', f.quote_qty, 'fee_amount', f.fee_amount, 'fee_asset', f.fee_asset,
			'fee_verified', f.fee_verified, 'filled_at', f.filled_at
		)::text
		FROM public.order_fills f
		JOIN public.orders o ON o.id = f.order_id
		WHERE o.position_id = $1
		ORDER BY f.filled_at ASC
		LIMIT 2000
	`
	fillRows, err := c.db.QueryContext(ctx, fillsQuery, positionID)
	if err != nil {
		return nil, fmt.Errorf("query fills: %w", err)
	}
	defer fillRows.Close()

	var fills []json.RawMessage
	for fillRows.Next() {
		var str string
		if err := fillRows.Scan(&str); err == nil {
			fills = append(fills, json.RawMessage(str))
		}
	}

	return map[string][]json.RawMessage{
		"orders": orders,
		"fills":  fills,
	}, nil
}
