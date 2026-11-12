package analysis

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/shopspring/decimal"
)

// CrossVenueSpotStrategy finds inventory-based spot price dislocations after
// both venues' taker fees and a configured slippage reserve. It is research-only:
// the strategy never transfers funds or submits orders.
type CrossVenueSpotStrategy struct {
	MinNetReturnPct float64
	MaxCandidates   int
}

func (s CrossVenueSpotStrategy) Name() string { return "cross_venue_spot_dislocation" }

func (s CrossVenueSpotStrategy) Analyze(ctx context.Context, snapshot Snapshot) ([]Signal, error) {
	if err := ctx.Err(); err != nil { return nil, err }
	if snapshot.At.IsZero() { return nil, fmt.Errorf("snapshot timestamp is required") }
	if math.IsNaN(snapshot.EstimatedSlippagePct) || math.IsInf(snapshot.EstimatedSlippagePct, 0) || snapshot.EstimatedSlippagePct < 0 {
		return nil, fmt.Errorf("estimated slippage must be finite and non-negative")
	}
	maxAge := snapshot.MaxBookAge
	if maxAge <= 0 { maxAge = 3 * time.Second }

	marketsByPair := make(map[string][]domain.Market)
	for _, market := range snapshot.Markets {
		if !market.Active || !strings.EqualFold(market.MarketType, "SPOT") ||
			strings.TrimSpace(market.Exchange) == "" ||
			strings.TrimSpace(market.Symbol) == "" ||
			strings.TrimSpace(market.BaseAsset) == "" ||
			strings.TrimSpace(market.QuoteAsset) == "" {
			continue
		}
		key := strings.ToUpper(market.BaseAsset) + "/" + strings.ToUpper(market.QuoteAsset)
		marketsByPair[key] = append(marketsByPair[key], market)
	}

	signals := make([]Signal, 0)
	pairs := make([]string, 0, len(marketsByPair))
	for pair := range marketsByPair { pairs = append(pairs, pair) }
	sort.Strings(pairs)
	for _, pair := range pairs {
		markets := marketsByPair[pair]
		if len(markets) < 2 { continue }
		for i := 0; i < len(markets); i++ {
			for j := i + 1; j < len(markets); j++ {
				if err := ctx.Err(); err != nil { return signals, err }
				if strings.EqualFold(markets[i].Exchange, markets[j].Exchange) { continue }
				forward, ok := s.evaluateDirection(snapshot, markets[i], markets[j], pair, maxAge)
				if ok { signals = append(signals, forward) }
				reverse, ok := s.evaluateDirection(snapshot, markets[j], markets[i], pair, maxAge)
				if ok { signals = append(signals, reverse) }
			}
		}
	}
	sort.SliceStable(signals, func(i, j int) bool {
		return signals[i].ExpectedReturn > signals[j].ExpectedReturn
	})
	if s.MaxCandidates > 0 && len(signals) > s.MaxCandidates {
		signals = signals[:s.MaxCandidates]
	}
	return signals, nil
}

// evaluateDirection buys on buyMarket and sells already-held inventory on
// sellMarket. No transfer or funding assumption is made.
func (s CrossVenueSpotStrategy) evaluateDirection(
	snapshot Snapshot, buyMarket, sellMarket domain.Market, pair string, maxAge time.Duration,
) (Signal, bool) {
	buyBook, buyOK := freshSpotBook(snapshot, buyMarket, maxAge)
	sellBook, sellOK := freshSpotBook(snapshot, sellMarket, maxAge)
	if !buyOK || !sellOK || len(buyBook.Asks) == 0 || len(sellBook.Bids) == 0 { return Signal{}, false }
	buyAsk, sellBid := buyBook.Asks[0], sellBook.Bids[0]
	if buyAsk.Price <= 0 || sellBid.Price <= 0 || buyAsk.Quantity <= 0 || sellBid.Quantity <= 0 ||
		buyAsk.Price >= sellBid.Price { return Signal{}, false }

	buyFees, buyFeeOK := snapshot.Fees[FeeKey(buyMarket.Exchange, buyMarket.Symbol)]
	sellFees, sellFeeOK := snapshot.Fees[FeeKey(sellMarket.Exchange, sellMarket.Symbol)]
	if !buyFeeOK || !sellFeeOK ||
		!finiteNonNegative(buyFees.TakerPct) || !finiteNonNegative(sellFees.TakerPct) {
		return Signal{}, false
	}

	buyPrice := decimal.NewFromFloat(buyAsk.Price)
	sellPrice := decimal.NewFromFloat(sellBid.Price)
	gross := sellPrice.Sub(buyPrice).Div(buyPrice).Mul(decimal.NewFromInt(100))
	fees := decimal.NewFromFloat(buyFees.TakerPct).Add(decimal.NewFromFloat(sellFees.TakerPct))
	slippage := decimal.NewFromFloat(snapshot.EstimatedSlippagePct)
	net := gross.Sub(fees).Sub(slippage)
	minimum := decimal.NewFromFloat(s.MinNetReturnPct)
	if !net.GreaterThan(minimum) { return Signal{}, false }

	quantity := math.Min(buyAsk.Quantity, sellBid.Quantity)
	if quantity <= 0 || math.IsNaN(quantity) || math.IsInf(quantity, 0) { return Signal{}, false }
	score := net.Div(net.Abs().Add(decimal.NewFromInt(1))).InexactFloat64()
	if score < 0 { score = 0 }
	if score > 1 { score = 1 }
	buyVenue, sellVenue := strings.ToLower(buyMarket.Exchange), strings.ToLower(sellMarket.Exchange)
	return Signal{
		Strategy: s.Name(), Instrument: pair, Venue: buyVenue + "->" + sellVenue,
		MarketType: "SPOT", Side: SideLong, Confidence: score,
		ExpectedReturn: net.InexactFloat64(),
		Reason: "inventory-based cross-venue spot dislocation after taker fees and slippage reserve; transfer, balance and execution risks require separate approval",
		Metadata: map[string]string{
			"buy_venue": buyVenue, "buy_symbol": buyMarket.Symbol,
			"sell_venue": sellVenue, "sell_symbol": sellMarket.Symbol,
			"buy_ask": buyPrice.String(), "sell_bid": sellPrice.String(),
			"top_quantity_base": decimal.NewFromFloat(quantity).String(),
			"gross_spread_pct": gross.String(), "fees_pct": fees.String(),
			"slippage_pct": slippage.String(),
		},
	}, true
}

func freshSpotBook(snapshot Snapshot, market domain.Market, maxAge time.Duration) (domain.OrderBook, bool) {
	book, ok := snapshot.OrderBooks[BookKey(market.Exchange, market.Symbol)]
	if !ok || !strings.EqualFold(book.Exchange, market.Exchange) ||
		!strings.EqualFold(book.Symbol, market.Symbol) || len(book.Bids) == 0 || len(book.Asks) == 0 {
		return domain.OrderBook{}, false
	}
	stamp := book.ReceivedAt
	if stamp.IsZero() { stamp = book.Timestamp }
	if stamp.IsZero() || stamp.After(snapshot.At.Add(time.Second)) ||
		snapshot.At.Sub(stamp) > maxAge {
		return domain.OrderBook{}, false
	}
	if book.Bids[0].Price <= 0 || book.Asks[0].Price <= 0 ||
		book.Bids[0].Quantity <= 0 || book.Asks[0].Quantity <= 0 ||
		book.Bids[0].Price >= book.Asks[0].Price {
		return domain.OrderBook{}, false
	}
	return book, true
}

func finiteNonNegative(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}
