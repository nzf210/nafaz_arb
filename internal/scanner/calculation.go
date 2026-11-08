package scanner

import (
	"fmt"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/shopspring/decimal"
)

type calculationInput struct {
	spot            domain.Market
	perp            domain.Market
	spotBook        domain.OrderBook
	perpBook        domain.OrderBook
	funding         domain.FundingRate
	now             time.Time
	positionReason  string
	isSynthetic     bool
	fxRate          float64
	requiredNotional decimal.Decimal
}

type sizingResult struct {
	spotAsk      decimal.Decimal
	spotBid      decimal.Decimal
	perpBid      decimal.Decimal
	perpAsk      decimal.Decimal
	targetBudget decimal.Decimal
	perpLeverage decimal.Decimal
	spotEntryRef decimal.Decimal
	qty          decimal.Decimal
	isMakerTaker bool
	prices       executionPrices
}

type feeRates struct {
	spotMakerRate         decimal.Decimal
	spotTakerRate         decimal.Decimal
	perpTakerRate         decimal.Decimal
	feeAssetSurchargeRate decimal.Decimal
}

type pnlResult struct {
	spread                 decimal.Decimal
	slippage               decimal.Decimal
	spotFees               decimal.Decimal
	perpFees               decimal.Decimal
	fees                   decimal.Decimal
	funding                decimal.Decimal
	net                    decimal.Decimal
	capital                decimal.Decimal
	capacityPct            float64
	roi                    decimal.Decimal
	frRate                 decimal.Decimal
	settlementsDec         decimal.Decimal
	minAcceptablePerpPrice decimal.Decimal
}

func (s *Service) calculate(sm, pm domain.Market, sb, pb domain.OrderBook, fr domain.FundingRate, now time.Time, positionReason string, isSynthetic bool, fxRate float64, requiredNotional decimal.Decimal) (domain.Opportunity, error) {
	input := calculationInput{
		spot: sm, perp: pm, spotBook: sb, perpBook: pb, funding: fr,
		now: now, positionReason: positionReason, isSynthetic: isSynthetic,
		fxRate: fxRate, requiredNotional: requiredNotional,
	}
	if err := s.validateCalculationInput(input); err != nil {
		return domain.Opportunity{}, err
	}
	sizing, err := s.calculateSizing(input)
	if err != nil {
		return domain.Opportunity{}, err
	}
	fees := s.resolveFeeRates(input)
	pnl, err := s.calculatePnL(input, sizing, fees)
	if err != nil {
		return domain.Opportunity{}, err
	}
	op := s.buildOpportunity(input, sizing, pnl)
	s.evaluateOpportunity(&op, input.positionReason, sizing.perpBid.InexactFloat64(), sizing.spotEntryRef.InexactFloat64(), input.funding.Rate)
	return op, nil
}

func (s *Service) validateCalculationInput(input calculationInput) error {
	if err := validateOrderBooks(input.spotBook, input.perpBook); err != nil {
		return err
	}
	if err := validateOrderBookAge(input.spotBook, input.perpBook, input.now, s.cfg.MaxOrderBookAgeSeconds); err != nil {
		return err
	}
	maxSkew := time.Duration(s.cfg.MaxCrossExchangeSkewSeconds) * time.Second
	if err := validateCrossExchangeTimestampAlignment(input.spotBook, input.perpBook, maxSkew); err != nil {
		return err
	}
	if err := validateOrderBookIntegrity(input.spotBook, input.perpBook); err != nil {
		return err
	}
	if err := s.validateLiquidityVelocity(input.spotBook, input.perpBook, input.requiredNotional); err != nil {
		return err
	}
	if err := s.validateOrderBookImbalance(input.spotBook, input.perpBook); err != nil {
		return err
	}
	return s.validateLastTradeAge(input.now, input.spot, input.perp, input.spotBook, input.perpBook)
}

func (s *Service) calculateSizing(input calculationInput) (sizingResult, error) {
	sb, pb := input.spotBook, input.perpBook
	result := sizingResult{
		spotAsk: decimal.NewFromFloat(sb.Asks[0].Price),
		spotBid: decimal.NewFromFloat(sb.Bids[0].Price),
		perpBid: decimal.NewFromFloat(pb.Bids[0].Price),
		perpAsk: decimal.NewFromFloat(pb.Asks[0].Price),
		targetBudget: decimal.NewFromFloat(s.cfg.CapitalTotal).Mul(decimal.NewFromFloat(s.cfg.AllocationPerPair)),
		isMakerTaker: s.cfg.ExecutionMode == "MAKER_TAKER",
	}
	leverage := s.cfg.PerpLeverage
	if leverage <= 0 {
		leverage = 1
	}
	result.perpLeverage = decimal.NewFromFloat(leverage)
	result.spotEntryRef = resolveSpotEntryReference(input, result, s.cfg.LocalPriceTick)
	qty, err := s.calculateTargetQuantity(input, result)
	if err != nil {
		return sizingResult{}, err
	}
	result.qty = qty

	minimumCapital := s.cfg.MinAllocatedCapital
	if minimumCapital <= 0 {
		minimumCapital = 10
	}
	estimatedCapital := result.spotEntryRef.Mul(qty).Add(result.perpBid.Mul(qty).Div(result.perpLeverage))
	if estimatedCapital.LessThan(decimal.NewFromFloat(minimumCapital)) {
		return sizingResult{}, fmt.Errorf(
			"insufficient depth for minimum capital: available %.2f, min %.2f",
			estimatedCapital.InexactFloat64(), minimumCapital,
		)
	}
	prices, err := calculateExecutionPrices(sb, pb, qty, result.isMakerTaker, result.spotEntryRef)
	if err != nil {
		return sizingResult{}, err
	}
	result.prices = prices
	return result, nil
}

func resolveSpotEntryReference(input calculationInput, sizing sizingResult, configuredTick float64) decimal.Decimal {
	if !sizing.isMakerTaker {
		return sizing.spotAsk
	}
	tickSize := input.spot.TickSize
	if tickSize <= 0 {
		tickSize = configuredTick
	}
	if tickSize <= 0 {
		tickSize = 1
	}
	tick := decimal.NewFromFloat(tickSize)
	if input.isSynthetic && input.fxRate > 0 {
		tick = tick.Div(decimal.NewFromFloat(input.fxRate))
	}
	entry := sizing.spotBid.Add(tick)
	if entry.GreaterThanOrEqual(sizing.spotAsk) && sizing.spotAsk.GreaterThan(sizing.spotBid) {
		return sizing.spotBid
	}
	return entry
}

func (s *Service) calculateTargetQuantity(input calculationInput, sizing sizingResult) (decimal.Decimal, error) {
	denominator := sizing.spotEntryRef.Add(sizing.perpBid.Div(sizing.perpLeverage))
	if denominator.IsZero() {
		return decimal.Zero, fmt.Errorf("invalid denominator for quantity calculation")
	}
	qty := sizing.targetBudget.Div(denominator)
	if !s.cfg.AdaptiveSizingEnabled() {
		return qty, nil
	}
	maxExecutable := s.maxExecutableQuantity(input, sizing, qty)
	if maxExecutable.GreaterThan(decimal.Zero) && maxExecutable.LessThan(qty) {
		qty = maxExecutable
	}
	return qty, nil
}

func (s *Service) maxExecutableQuantity(input calculationInput, sizing sizingResult, qty decimal.Decimal) decimal.Decimal {
	maxExecutable := qty
	if sizing.isMakerTaker {
		maxExecutable = decimal.Min(maxExecutable, availableBookDepth(input.perpBook.Bids))
		maxExecutable = decimal.Min(maxExecutable, availableBookDepth(input.spotBook.Bids))
		maxExecutable = decimal.Min(maxExecutable, availableBookDepth(input.perpBook.Asks))
		return maxExecutable
	}
	maxExecutable = decimal.Min(maxExecutable, availableBookDepth(input.spotBook.Asks))
	maxExecutable = decimal.Min(maxExecutable, availableBookDepth(input.perpBook.Bids))
	maxExecutable = decimal.Min(maxExecutable, availableBookDepth(input.spotBook.Bids))
	return decimal.Min(maxExecutable, availableBookDepth(input.perpBook.Asks))
}

func (s *Service) resolveFeeRates(input calculationInput) feeRates {
	hundred := decimal.NewFromInt(100)
	spotMakerPct := s.cfg.SpotMakerFeePct
	spotTakerPct := s.cfg.SpotTakerFeePct
	perpTakerPct := s.cfg.PerpTakerFeePct
	if profile, ok := s.cfg.FeeProfiles[strings.ToLower(input.spot.Exchange)]; ok {
		if profile.SpotMakerFeePct != nil {
			spotMakerPct = *profile.SpotMakerFeePct
		}
		if profile.SpotTakerFeePct != nil {
			spotTakerPct = *profile.SpotTakerFeePct
		}
	}
	if profile, ok := s.cfg.FeeProfiles[strings.ToLower(input.perp.Exchange)]; ok && profile.PerpTakerFeePct != nil {
		perpTakerPct = *profile.PerpTakerFeePct
	}
	rates := feeRates{
		spotMakerRate: decimal.NewFromFloat(spotMakerPct).Div(hundred),
		spotTakerRate: decimal.NewFromFloat(spotTakerPct).Div(hundred),
		perpTakerRate: decimal.NewFromFloat(perpTakerPct).Div(hundred),
	}
	if s.cfg.FeeAssetSurchargePct > 0 {
		rates.feeAssetSurchargeRate = decimal.NewFromFloat(s.cfg.FeeAssetSurchargePct).Div(hundred)
	}
	return rates
}

func (s *Service) calculatePnL(input calculationInput, sizing sizingResult, rates feeRates) (pnlResult, error) {
	prices, qty := sizing.prices, sizing.qty
	result := pnlResult{}
	if sizing.isMakerTaker {
		result.spread = sizing.perpBid.Sub(sizing.spotEntryRef).Mul(qty)
		result.slippage = sizing.spotBid.Sub(prices.spotExit).
			Add(sizing.perpBid.Sub(prices.perpEntry)).
			Add(prices.perpExit.Sub(sizing.perpAsk)).
			Mul(qty)
		spotEntryFee := prices.spotEntry.Mul(qty).Mul(rates.spotMakerRate)
		spotExitFee := prices.spotExit.Mul(qty).Mul(rates.spotTakerRate)
		result.spotFees = spotEntryFee.Add(spotExitFee)
	} else {
		result.spread = sizing.perpBid.Sub(sizing.spotAsk).Mul(qty)
		result.slippage = prices.spotEntry.Sub(sizing.spotAsk).
			Add(sizing.spotBid.Sub(prices.spotExit)).
			Add(sizing.perpBid.Sub(prices.perpEntry)).
			Add(prices.perpExit.Sub(sizing.perpAsk)).
			Mul(qty)
		result.spotFees = prices.spotEntry.Add(prices.spotExit).Mul(qty).Mul(rates.spotTakerRate)
	}
	result.perpFees = prices.perpEntry.Add(prices.perpExit).Mul(qty).Mul(rates.perpTakerRate)
	if !rates.feeAssetSurchargeRate.IsZero() {
		surcharge := prices.spotEntry.Add(prices.spotExit).Add(prices.perpEntry).Add(prices.perpExit).
			Mul(qty).Mul(rates.feeAssetSurchargeRate)
		result.spotFees = result.spotFees.Add(surcharge)
	}
	result.fees = result.spotFees.Add(result.perpFees)
	settlements := calculateSettlements(s.cfg.EstimatedHoldingHours, input.funding.IntervalHours, input.funding.NextFundingTime, input.now)
	result.frRate = decimal.NewFromFloat(input.funding.Rate)
	result.settlementsDec = decimal.NewFromInt(int64(settlements))
	result.funding = prices.perpEntry.Mul(qty).Mul(result.frRate).Mul(result.settlementsDec)
	result.net = result.spread.Add(result.funding).Sub(result.fees).Sub(result.slippage)
	result.capital = prices.spotEntry.Mul(qty).Add(prices.perpEntry.Mul(qty).Div(sizing.perpLeverage))
	if result.capital.LessThanOrEqual(decimal.Zero) {
		return pnlResult{}, fmt.Errorf("invalid allocated capital")
	}
	result.capacityPct = calculateCapacityPct(result.capital, sizing.targetBudget)
	result.roi = result.net.Div(result.capital).Mul(decimal.NewFromInt(100))
	result.minAcceptablePerpPrice = calculateMinAcceptablePerpPrice(minAcceptablePerpPriceParams{
		spotEntry: prices.spotEntry, perpExit: prices.perpExit, qty: qty,
		spotFees: result.spotFees, perpTakerFeeRate: rates.perpTakerRate,
		frRate: result.frRate, settlementsDec: result.settlementsDec,
		slippage: result.slippage, minNetProfitQuote: s.cfg.MinNetProfitQuote,
	})
	return result, nil
}

func calculateCapacityPct(capital, targetBudget decimal.Decimal) float64 {
	if !targetBudget.GreaterThan(decimal.Zero) {
		return 100
	}
	pct := capital.Div(targetBudget).Mul(decimal.NewFromInt(100)).InexactFloat64()
	if pct >= 100 {
		return 100
	}
	return pct
}

func (s *Service) buildOpportunity(input calculationInput, sizing sizingResult, pnl pnlResult) domain.Opportunity {
	status := "PROFITABLE_IN_RANGE"
	roiFloat := pnl.roi.InexactFloat64()
	switch {
	case pnl.net.LessThanOrEqual(decimal.Zero):
		status = "NOT_PROFITABLE"
	case roiFloat < s.cfg.MinNetROIPct:
		status = "BELOW_MIN_TARGET"
	case roiFloat > s.cfg.MaxNetROIPct:
		status = "ABOVE_MAX_TARGET"
	}

	executionMode := s.cfg.ExecutionMode
	if executionMode == "" {
		executionMode = "TAKER_TAKER"
	}
	orderTTL := s.cfg.MaxOrderTTLSeconds
	if orderTTL <= 0 {
		orderTTL = 30
	}
	targetQuote := input.perp.QuoteAsset
	if !input.isSynthetic {
		targetQuote = input.spot.QuoteAsset
	}

	prices := sizing.prices
	return domain.Opportunity{
		OpportunityKey:         fmt.Sprintf("%s/%s:%s:%s:%s:%s", input.spot.BaseAsset, targetQuote, strings.ToLower(input.spot.Exchange), input.spot.Symbol, strings.ToLower(input.perp.Exchange), input.perp.Symbol),
		SpotExchange:           input.spot.Exchange,
		PerpExchange:           input.perp.Exchange,
		BaseAsset:              input.spot.BaseAsset,
		QuoteAsset:             targetQuote,
		SpotSymbol:              input.spot.Symbol,
		PerpSymbol:              input.perp.Symbol,
		ExecutionMode:           executionMode,
		SpotEntryPrice:          prices.spotEntry.InexactFloat64(),
		SpotExitPrice:           prices.spotExit.InexactFloat64(),
		PerpEntryPrice:          prices.perpEntry.InexactFloat64(),
		PerpExitPrice:           prices.perpExit.InexactFloat64(),
		Quantity:                sizing.qty.InexactFloat64(),
		SpreadPnL:               pnl.spread.InexactFloat64(),
		FundingPnL:              pnl.funding.InexactFloat64(),
		TradingFees:             pnl.fees.InexactFloat64(),
		Slippage:                pnl.slippage.InexactFloat64(),
		NetProfit:               pnl.net.InexactFloat64(),
		AllocatedCapital:        pnl.capital.InexactFloat64(),
		NetROIPct:               roiFloat,
		ProfitabilityStatus:     status,
		LastScannedAt:           input.now,
		SpotTargetLimitPrice:    prices.spotEntry.InexactFloat64(),
		MinAcceptablePerpPrice:  pnl.minAcceptablePerpPrice.InexactFloat64(),
		SpotQueueDepth:          input.spotBook.Bids[0].Quantity,
		MaxOrderTTLSeconds:      orderTTL,
		IsSynthetic:             input.isSynthetic,
		SyntheticFXRate:         input.fxRate,
		OriginalSpotQuote:       input.spot.QuoteAsset,
		CapacityPct:             pnl.capacityPct,
	}
}
