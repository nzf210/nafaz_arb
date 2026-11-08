package scanner

import (
	"fmt"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/shopspring/decimal"
)

func (s *Service) calculate(sm, pm domain.Market, sb, pb domain.OrderBook, fr domain.FundingRate, now time.Time, positionReason string, isSynthetic bool, fxRate float64, requiredNotional decimal.Decimal) (domain.Opportunity, error) {
	if err := validateOrderBooks(sb, pb); err != nil {
		return domain.Opportunity{}, err
	}
	if err := validateOrderBookAge(sb, pb, now, s.cfg.MaxOrderBookAgeSeconds); err != nil {
		return domain.Opportunity{}, err
	}
	if err := validateCrossExchangeTimestampAlignment(sb, pb, time.Duration(s.cfg.MaxCrossExchangeSkewSeconds)*time.Second); err != nil {
		return domain.Opportunity{}, err
	}
	if err := validateOrderBookIntegrity(sb, pb); err != nil {
		return domain.Opportunity{}, err
	}
	if err := s.validateLiquidityVelocity(sb, pb, requiredNotional); err != nil {
		return domain.Opportunity{}, err
	}
	if err := s.validateOrderBookImbalance(sb, pb); err != nil {
		return domain.Opportunity{}, err
	}
	if err := s.validateLastTradeAge(now, sm, pm, sb, pb); err != nil {
		return domain.Opportunity{}, err
	}
	spotAsk := decimal.NewFromFloat(sb.Asks[0].Price)
	spotBid := decimal.NewFromFloat(sb.Bids[0].Price)
	perpBid := decimal.NewFromFloat(pb.Bids[0].Price)
	perpAsk := decimal.NewFromFloat(pb.Asks[0].Price)

	targetBudget := decimal.NewFromFloat(s.cfg.CapitalTotal).Mul(decimal.NewFromFloat(s.cfg.AllocationPerPair))
	perpLevFloat := s.cfg.PerpLeverage
	if perpLevFloat <= 0 {
		perpLevFloat = 1
	}
	perpLev := decimal.NewFromFloat(perpLevFloat)

	isMakerTaker := s.cfg.ExecutionMode == "MAKER_TAKER"

	var spotEntryRef decimal.Decimal
	if isMakerTaker {
		tickSize := sm.TickSize
		if tickSize <= 0 {
			tickSize = s.cfg.LocalPriceTick
		}
		if tickSize <= 0 {
			tickSize = 1.0
		}
		tickDec := decimal.NewFromFloat(tickSize)
		if isSynthetic && fxRate > 0 {
			tickDec = tickDec.Div(decimal.NewFromFloat(fxRate))
		}
		spotEntryRef = spotBid.Add(tickDec)
		if spotEntryRef.GreaterThanOrEqual(spotAsk) && spotAsk.GreaterThan(spotBid) {
			spotEntryRef = spotBid
		}
	} else {
		spotEntryRef = spotAsk
	}

	denom := spotEntryRef.Add(perpBid.Div(perpLev))
	if denom.IsZero() {
		return domain.Opportunity{}, fmt.Errorf("invalid denominator for quantity calculation")
	}
	targetQty := targetBudget.Div(denom)
	qty := targetQty

	if s.cfg.AdaptiveSizingEnabled() {
		maxExecutable := qty
		if isMakerTaker {
			// Maker-Taker: Limit Buy on Spot at spotBid, Market Short on Perp at perpEntry
			// Needs Perp Bids depth for short entry, Spot Bids depth for spot exit, Perp Asks depth for perp exit
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(pb.Bids))
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(sb.Bids))
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(pb.Asks))
		} else {
			// Taker-Taker: Market Buy on Spot, Market Short on Perp
			// Needs Spot Asks and Perp Bids for entry; Spot Bids and Perp Asks for exit
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(sb.Asks))
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(pb.Bids))
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(sb.Bids))
			maxExecutable = decimal.Min(maxExecutable, availableBookDepth(pb.Asks))
		}
		if maxExecutable.GreaterThan(decimal.Zero) && maxExecutable.LessThan(qty) {
			qty = maxExecutable
		}
	}

	minCap := s.cfg.MinAllocatedCapital
	if minCap <= 0 {
		minCap = 10.0
	}
	capitalEstimate := spotEntryRef.Mul(qty).Add(perpBid.Mul(qty).Div(perpLev))
	if capitalEstimate.LessThan(decimal.NewFromFloat(minCap)) {
		return domain.Opportunity{}, fmt.Errorf("insufficient depth for minimum capital: available %.2f, min %.2f", capitalEstimate.InexactFloat64(), minCap)
	}

	prices, err := calculateExecutionPrices(sb, pb, qty, isMakerTaker, spotEntryRef)
	if err != nil {
		return domain.Opportunity{}, err
	}

	hundred := decimal.NewFromInt(100)
	spotMakerFeePct := s.cfg.SpotMakerFeePct
	spotTakerFeePct := s.cfg.SpotTakerFeePct
	perpTakerFeePct := s.cfg.PerpTakerFeePct
	if profile, ok := s.cfg.FeeProfiles[strings.ToLower(sm.Exchange)]; ok {
		if profile.SpotMakerFeePct != nil {
			spotMakerFeePct = *profile.SpotMakerFeePct
		}
		if profile.SpotTakerFeePct != nil {
			spotTakerFeePct = *profile.SpotTakerFeePct
		}
	}
	if profile, ok := s.cfg.FeeProfiles[strings.ToLower(pm.Exchange)]; ok && profile.PerpTakerFeePct != nil {
		perpTakerFeePct = *profile.PerpTakerFeePct
	}
	spotTakerFeeRate := decimal.NewFromFloat(spotTakerFeePct).Div(hundred)
	perpTakerFeeRate := decimal.NewFromFloat(perpTakerFeePct).Div(hundred)
	// Fee-asset surcharge: model non-quote-asset fee effects (BNB discount,
	// fee billed in another asset) as a flat extra rate on notional so the
	// net profit is not overstated. 0 = disabled.
	var feeAssetSurchargeRate decimal.Decimal
	if s.cfg.FeeAssetSurchargePct > 0 {
		feeAssetSurchargeRate = decimal.NewFromFloat(s.cfg.FeeAssetSurchargePct).Div(hundred)
	}

	var spread decimal.Decimal
	var slippage decimal.Decimal
	var spotFees decimal.Decimal

	if isMakerTaker {
		// Maker-Taker: Limit Buy on Spot at spotEntryRef, Market Short on Perp at perpEntry
		spread = perpBid.Sub(spotEntryRef).Mul(qty)
		// Spot entry slippage is 0 because limit order execution is locked at spotEntryRef
		slippage = spotBid.Sub(prices.spotExit).
			Add(perpBid.Sub(prices.perpEntry)).
			Add(prices.perpExit.Sub(perpAsk)).
			Mul(qty)

		spotMakerFeeRate := decimal.NewFromFloat(spotMakerFeePct).Div(hundred)
		spotEntryFee := prices.spotEntry.Mul(qty).Mul(spotMakerFeeRate)
		spotExitFee := prices.spotExit.Mul(qty).Mul(spotTakerFeeRate)
		spotFees = spotEntryFee.Add(spotExitFee)
	} else {
		// Taker-Taker (default)
		spread = perpBid.Sub(spotAsk).Mul(qty)
		slippage = prices.spotEntry.Sub(spotAsk).
			Add(spotBid.Sub(prices.spotExit)).
			Add(perpBid.Sub(prices.perpEntry)).
			Add(prices.perpExit.Sub(perpAsk)).
			Mul(qty)
		spotFees = prices.spotEntry.Add(prices.spotExit).Mul(qty).Mul(spotTakerFeeRate)
	}

	perpFees := prices.perpEntry.Add(prices.perpExit).Mul(qty).Mul(perpTakerFeeRate)
	if !feeAssetSurchargeRate.IsZero() {
		// Surcharge applies to the round-trip notional of both legs.
		surcharge := prices.spotEntry.Add(prices.spotExit).Add(prices.perpEntry).Add(prices.perpExit).Mul(qty).Mul(feeAssetSurchargeRate)
		spotFees = spotFees.Add(surcharge)
	}
	fees := spotFees.Add(perpFees)

	settlements := calculateSettlements(s.cfg.EstimatedHoldingHours, fr.IntervalHours, fr.NextFundingTime, now)
	frRate := decimal.NewFromFloat(fr.Rate)
	settlementsDec := decimal.NewFromInt(int64(settlements))
	funding := prices.perpEntry.Mul(qty).Mul(frRate).Mul(settlementsDec) // positive rate: short receives

	net := spread.Add(funding).Sub(fees).Sub(slippage)
	capital := prices.spotEntry.Mul(qty).Add(prices.perpEntry.Mul(qty).Div(perpLev))
	if capital.LessThanOrEqual(decimal.Zero) {
		return domain.Opportunity{}, fmt.Errorf("invalid allocated capital")
	}

	capacityPct := 100.0
	if targetBudget.GreaterThan(decimal.Zero) {
		pct := capital.Div(targetBudget).Mul(hundred).InexactFloat64()
		if pct < 100.0 {
			capacityPct = pct
		}
	}

	roi := net.Div(capital).Mul(hundred)
	netFloat := net.InexactFloat64()
	roiFloat := roi.InexactFloat64()

	status := "PROFITABLE_IN_RANGE"
	switch {
	case net.LessThanOrEqual(decimal.Zero):
		status = "NOT_PROFITABLE"
	case roiFloat < s.cfg.MinNetROIPct:
		status = "BELOW_MIN_TARGET"
	case roiFloat > s.cfg.MaxNetROIPct:
		status = "ABOVE_MAX_TARGET"
	}

	minAcceptablePerpPrice := calculateMinAcceptablePerpPrice(minAcceptablePerpPriceParams{
		spotEntry:         prices.spotEntry,
		perpExit:          prices.perpExit,
		qty:               qty,
		spotFees:          spotFees,
		perpTakerFeeRate:  perpTakerFeeRate,
		frRate:            frRate,
		settlementsDec:    settlementsDec,
		slippage:          slippage,
		minNetProfitQuote: s.cfg.MinNetProfitQuote,
	})

	execMode := s.cfg.ExecutionMode
	if execMode == "" {
		execMode = "TAKER_TAKER"
	}

	ttlSeconds := s.cfg.MaxOrderTTLSeconds
	if ttlSeconds <= 0 {
		ttlSeconds = 30
	}

	targetQuote := pm.QuoteAsset
	if !isSynthetic {
		targetQuote = sm.QuoteAsset
	}

	op := domain.Opportunity{
		OpportunityKey:         fmt.Sprintf("%s/%s:%s:%s:%s:%s", sm.BaseAsset, targetQuote, strings.ToLower(sm.Exchange), sm.Symbol, strings.ToLower(pm.Exchange), pm.Symbol),
		SpotExchange:           sm.Exchange,
		PerpExchange:           pm.Exchange,
		BaseAsset:              sm.BaseAsset,
		QuoteAsset:             targetQuote,
		SpotSymbol:             sm.Symbol,
		PerpSymbol:             pm.Symbol,
		ExecutionMode:          execMode,
		SpotEntryPrice:         prices.spotEntry.InexactFloat64(),
		SpotExitPrice:          prices.spotExit.InexactFloat64(),
		PerpEntryPrice:         prices.perpEntry.InexactFloat64(),
		PerpExitPrice:          prices.perpExit.InexactFloat64(),
		Quantity:               qty.InexactFloat64(),
		SpreadPnL:              spread.InexactFloat64(),
		FundingPnL:             funding.InexactFloat64(),
		TradingFees:            fees.InexactFloat64(),
		Slippage:               slippage.InexactFloat64(),
		NetProfit:              netFloat,
		AllocatedCapital:       capital.InexactFloat64(),
		NetROIPct:              roiFloat,
		ProfitabilityStatus:    status,
		LastScannedAt:          now,
		SpotTargetLimitPrice:   prices.spotEntry.InexactFloat64(),
		MinAcceptablePerpPrice: minAcceptablePerpPrice.InexactFloat64(),
		SpotQueueDepth:         sb.Bids[0].Quantity,
		MaxOrderTTLSeconds:     ttlSeconds,
		IsSynthetic:            isSynthetic,
		SyntheticFXRate:        fxRate,
		OriginalSpotQuote:      sm.QuoteAsset,
		CapacityPct:            capacityPct,
	}
	s.evaluateOpportunity(&op, positionReason, perpBid.InexactFloat64(), spotEntryRef.InexactFloat64(), fr.Rate)
	return op, nil
}
