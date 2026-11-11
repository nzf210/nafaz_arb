package scanner

import (
	"fmt"
	"strings"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
	"github.com/nafaz/spot-perp-scanner/internal/redishandler"
	"github.com/shopspring/decimal"
)

type opportunityCalculationInput struct {
	spotMarket       domain.Market
	perpMarket       domain.Market
	spotBook         domain.OrderBook
	perpBook         domain.OrderBook
	funding          domain.FundingRate
	now              time.Time
	positionReason   string
	isSynthetic      bool
	fxRate           float64
	requiredNotional decimal.Decimal
	dynamicProfiles  []*redishandler.DynamicFeeProfile
}

type opportunitySizing struct {
	targetBudget decimal.Decimal
	perpLeverage decimal.Decimal
	spotEntryRef decimal.Decimal
	quantity     decimal.Decimal
	makerTaker   bool
}

type opportunityFees struct {
	spotMakerPct  float64
	spotTakerPct  float64
	perpTakerPct  float64
	source        string
	verified      bool
	fetchedAtMS   int64
	spotTakerRate decimal.Decimal
	perpTakerRate decimal.Decimal
	surchargeRate decimal.Decimal
}

type opportunityProfit struct {
	spread                 decimal.Decimal
	slippage               decimal.Decimal
	fees                   decimal.Decimal
	funding                decimal.Decimal
	net                    decimal.Decimal
	capital                decimal.Decimal
	capacityPct            float64
	roi                    decimal.Decimal
	status                 string
	minAcceptablePerpPrice decimal.Decimal
}

func (s *Service) validateCalculationInput(input opportunityCalculationInput) error {
	if err := validateOrderBooks(input.spotBook, input.perpBook); err != nil {
		return err
	}
	if err := validateOrderBookAge(
		input.spotBook, input.perpBook, input.now, s.cfg.MaxOrderBookAgeSeconds,
	); err != nil {
		return err
	}
	if err := validateCrossExchangeTimestampAlignment(
		input.spotBook, input.perpBook,
		time.Duration(s.cfg.MaxCrossExchangeSkewSeconds)*time.Second,
	); err != nil {
		return err
	}
	if err := validateOrderBookIntegrity(input.spotBook, input.perpBook); err != nil {
		return err
	}
	if err := s.validateLiquidityVelocity(
		input.spotBook, input.perpBook, input.requiredNotional,
	); err != nil {
		return err
	}
	if err := s.validateOrderBookImbalance(input.spotBook, input.perpBook); err != nil {
		return err
	}
	return s.validateLastTradeAge(
		input.now, input.spotMarket, input.perpMarket,
		input.spotBook, input.perpBook,
	)
}

func (s *Service) opportunitySpotEntryReference(
	input opportunityCalculationInput,
	sm domain.Market,
	spotBid, spotAsk decimal.Decimal,
	makerTaker bool,
) decimal.Decimal {
	if !makerTaker {
		return spotAsk
	}
	tickSize := sm.TickSize
	if tickSize <= 0 {
		tickSize = s.cfg.LocalPriceTick
	}
	if tickSize <= 0 {
		tickSize = 1.0
	}
	tick := decimal.NewFromFloat(tickSize)
	if input.isSynthetic && input.fxRate > 0 {
		tick = tick.Div(decimal.NewFromFloat(input.fxRate))
	}
	entry := spotBid.Add(tick)
	if entry.GreaterThanOrEqual(spotAsk) && spotAsk.GreaterThan(spotBid) {
		return spotBid
	}
	return entry
}

func (s *Service) opportunityExecutableDepth(
	spotBook, perpBook domain.OrderBook, makerTaker bool,
) decimal.Decimal {
	depth := decimal.Zero
	if makerTaker {
		depth = availableBookDepth(perpBook.Bids)
		depth = decimal.Min(depth, availableBookDepth(spotBook.Bids))
		depth = decimal.Min(depth, availableBookDepth(perpBook.Asks))
		return depth
	}
	depth = availableBookDepth(spotBook.Asks)
	depth = decimal.Min(depth, availableBookDepth(perpBook.Bids))
	depth = decimal.Min(depth, availableBookDepth(spotBook.Bids))
	return decimal.Min(depth, availableBookDepth(perpBook.Asks))
}

func (s *Service) calculateOpportunitySizing(
	input opportunityCalculationInput,
) (opportunitySizing, error) {
	sm, sb, pb := input.spotMarket, input.spotBook, input.perpBook
	targetBudget := decimal.NewFromFloat(s.cfg.CapitalTotal).
		Mul(decimal.NewFromFloat(s.cfg.AllocationPerPair))
	perpLevFloat := s.cfg.PerpLeverage
	if perpLevFloat <= 0 {
		perpLevFloat = 1
	}
	perpLev := decimal.NewFromFloat(perpLevFloat)
	makerTaker := s.cfg.ExecutionMode == "MAKER_TAKER"
	spotAsk := decimal.NewFromFloat(sb.Asks[0].Price)
	spotBid := decimal.NewFromFloat(sb.Bids[0].Price)
	perpBid := decimal.NewFromFloat(pb.Bids[0].Price)
	spotEntryRef := s.opportunitySpotEntryReference(
		input, sm, spotBid, spotAsk, makerTaker,
	)
	denom := spotEntryRef.Add(perpBid.Div(perpLev))
	if denom.IsZero() {
		return opportunitySizing{}, fmt.Errorf(
			"invalid denominator for quantity calculation",
		)
	}
	qty := targetBudget.Div(denom)
	if s.cfg.AdaptiveSizingEnabled() {
		maxExecutable := s.opportunityExecutableDepth(sb, pb, makerTaker)
		if maxExecutable.GreaterThan(decimal.Zero) && maxExecutable.LessThan(qty) {
			qty = maxExecutable
		}
	}
	minCap := s.cfg.MinAllocatedCapital
	if minCap <= 0 {
		minCap = 10.0
	}
	capitalEstimate := spotEntryRef.Mul(qty).Add(
		perpBid.Mul(qty).Div(perpLev),
	)
	if capitalEstimate.LessThan(decimal.NewFromFloat(minCap)) {
		return opportunitySizing{}, fmt.Errorf(
			"insufficient depth for minimum capital: available %.2f, min %.2f",
			capitalEstimate.InexactFloat64(), minCap,
		)
	}
	return opportunitySizing{
		targetBudget: targetBudget,
		perpLeverage: perpLev,
		spotEntryRef: spotEntryRef,
		quantity:     qty,
		makerTaker:   makerTaker,
	}, nil
}

func (s *Service) configuredOpportunityFeeRates(
	spotMarket, perpMarket domain.Market,
) (float64, float64, float64) {
	spotMakerPct := s.cfg.SpotMakerFeePct
	spotTakerPct := s.cfg.SpotTakerFeePct
	perpTakerPct := s.cfg.PerpTakerFeePct
	if profile, ok := s.cfg.FeeProfiles[strings.ToLower(spotMarket.Exchange)]; ok {
		if profile.SpotMakerFeePct != nil {
			spotMakerPct = *profile.SpotMakerFeePct
		}
		if profile.SpotTakerFeePct != nil {
			spotTakerPct = *profile.SpotTakerFeePct
		}
	}
	if profile, ok := s.cfg.FeeProfiles[strings.ToLower(perpMarket.Exchange)]; ok &&
		profile.PerpTakerFeePct != nil {
		perpTakerPct = *profile.PerpTakerFeePct
	}
	return spotMakerPct, spotTakerPct, perpTakerPct
}

func applyDynamicOpportunityFeeProfiles(
	input opportunityCalculationInput,
	spotMakerPct, spotTakerPct, perpTakerPct *float64,
) (string, bool, int64) {
	spotDynamic := len(input.dynamicProfiles) > 0 &&
		input.dynamicProfiles[0] != nil
	perpDynamic := len(input.dynamicProfiles) > 1 &&
		input.dynamicProfiles[1] != nil
	var fetchedAtMS int64
	if spotDynamic {
		*spotMakerPct = input.dynamicProfiles[0].MakerFeePct
		*spotTakerPct = input.dynamicProfiles[0].TakerFeePct
		fetchedAtMS = input.dynamicProfiles[0].FetchedAtMS
	}
	if perpDynamic {
		*perpTakerPct = input.dynamicProfiles[1].TakerFeePct
		if input.dynamicProfiles[1].FetchedAtMS > fetchedAtMS {
			fetchedAtMS = input.dynamicProfiles[1].FetchedAtMS
		}
	}
	if spotDynamic && perpDynamic {
		return "EXCHANGE_API", true, fetchedAtMS
	}
	if spotDynamic || perpDynamic {
		return "PARTIAL_EXCHANGE_API", false, fetchedAtMS
	}
	return "CONFIG", false, fetchedAtMS
}

func (s *Service) resolveOpportunityFees(
	input opportunityCalculationInput,
) opportunityFees {
	spotMakerPct, spotTakerPct, perpTakerPct := s.configuredOpportunityFeeRates(
		input.spotMarket, input.perpMarket,
	)
	feeSource, feeVerified, fetchedAtMS := applyDynamicOpportunityFeeProfiles(
		input, &spotMakerPct, &spotTakerPct, &perpTakerPct,
	)
	hundred := decimal.NewFromInt(100)
	fees := opportunityFees{
		spotMakerPct:  spotMakerPct,
		spotTakerPct:  spotTakerPct,
		perpTakerPct:  perpTakerPct,
		source:        feeSource,
		verified:      feeVerified,
		fetchedAtMS:   fetchedAtMS,
		spotTakerRate: decimal.NewFromFloat(spotTakerPct).Div(hundred),
		perpTakerRate: decimal.NewFromFloat(perpTakerPct).Div(hundred),
	}
	if s.cfg.FeeAssetSurchargePct > 0 {
		fees.surchargeRate = decimal.NewFromFloat(
			s.cfg.FeeAssetSurchargePct,
		).Div(hundred)
	}
	return fees
}

func (s *Service) calculateOpportunityProfit(
	input opportunityCalculationInput, sizing opportunitySizing,
	prices executionPrices, fees opportunityFees,
) (opportunityProfit, error) {
	sb, pb, fr := input.spotBook, input.perpBook, input.funding
	qty := sizing.quantity
	spotAsk := decimal.NewFromFloat(sb.Asks[0].Price)
	spotBid := decimal.NewFromFloat(sb.Bids[0].Price)
	perpBid := decimal.NewFromFloat(pb.Bids[0].Price)
	perpAsk := decimal.NewFromFloat(pb.Asks[0].Price)
	var spread, slippage, spotFees decimal.Decimal
	if sizing.makerTaker {
		spread = perpBid.Sub(sizing.spotEntryRef).Mul(qty)
		slippage = spotBid.Sub(prices.spotExit).
			Add(perpBid.Sub(prices.perpEntry)).
			Add(prices.perpExit.Sub(perpAsk)).
			Mul(qty)
		spotMakerRate := decimal.NewFromFloat(fees.spotMakerPct).Div(
			decimal.NewFromInt(100),
		)
		spotEntryFee := prices.spotEntry.Mul(qty).Mul(spotMakerRate)
		spotExitFee := prices.spotExit.Mul(qty).Mul(fees.spotTakerRate)
		spotFees = spotEntryFee.Add(spotExitFee)
	} else {
		spread = perpBid.Sub(spotAsk).Mul(qty)
		slippage = prices.spotEntry.Sub(spotAsk).
			Add(spotBid.Sub(prices.spotExit)).
			Add(perpBid.Sub(prices.perpEntry)).
			Add(prices.perpExit.Sub(perpAsk)).
			Mul(qty)
		spotFees = prices.spotEntry.Add(prices.spotExit).
			Mul(qty).Mul(fees.spotTakerRate)
	}
	perpFees := prices.perpEntry.Add(prices.perpExit).
		Mul(qty).Mul(fees.perpTakerRate)
	if !fees.surchargeRate.IsZero() {
		surcharge := prices.spotEntry.Add(prices.spotExit).
			Add(prices.perpEntry).Add(prices.perpExit).
			Mul(qty).Mul(fees.surchargeRate)
		spotFees = spotFees.Add(surcharge)
	}
	totalFees := spotFees.Add(perpFees)
	settlements := calculateSettlements(
		s.cfg.EstimatedHoldingHours, fr.IntervalHours, fr.NextFundingTime, input.now,
	)
	frRate := decimal.NewFromFloat(fr.Rate)
	settlementsDec := decimal.NewFromInt(int64(settlements))
	funding := prices.perpEntry.Mul(qty).Mul(frRate).Mul(settlementsDec)
	net := spread.Add(funding).Sub(totalFees).Sub(slippage)
	capital := prices.spotEntry.Mul(qty).Add(
		prices.perpEntry.Mul(qty).Div(sizing.perpLeverage),
	)
	if capital.LessThanOrEqual(decimal.Zero) {
		return opportunityProfit{}, fmt.Errorf("invalid allocated capital")
	}
	capacityPct := 100.0
	if sizing.targetBudget.GreaterThan(decimal.Zero) {
		pct := capital.Div(sizing.targetBudget).Mul(
			decimal.NewFromInt(100),
		).InexactFloat64()
		if pct < 100.0 {
			capacityPct = pct
		}
	}
	roi := net.Div(capital).Mul(decimal.NewFromInt(100))
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
	minAcceptablePerpPrice := calculateMinAcceptablePerpPrice(
		minAcceptablePerpPriceParams{
			spotEntry:         prices.spotEntry,
			perpExit:          prices.perpExit,
			qty:               qty,
			spotFees:          spotFees,
			perpTakerFeeRate:  fees.perpTakerRate,
			frRate:            frRate,
			settlementsDec:    settlementsDec,
			slippage:          slippage,
			minNetProfitQuote: s.cfg.MinNetProfitQuote,
		},
	)
	return opportunityProfit{
		spread: spread, slippage: slippage, fees: totalFees, funding: funding,
		net: net, capital: capital, capacityPct: capacityPct, roi: roi,
		status: status, minAcceptablePerpPrice: minAcceptablePerpPrice,
	}, nil
}

func (s *Service) buildOpportunity(
	input opportunityCalculationInput, sizing opportunitySizing,
	prices executionPrices, fees opportunityFees, profit opportunityProfit,
) domain.Opportunity {
	sm, pm, sb := input.spotMarket, input.perpMarket,
		input.spotBook
	execMode := s.cfg.ExecutionMode
	if execMode == "" {
		execMode = "TAKER_TAKER"
	}
	ttlSeconds := s.cfg.MaxOrderTTLSeconds
	if ttlSeconds <= 0 {
		ttlSeconds = 30
	}
	targetQuote := pm.QuoteAsset
	if !input.isSynthetic {
		targetQuote = sm.QuoteAsset
	}
	spotEntryFeePct := fees.spotTakerPct
	if sizing.makerTaker {
		spotEntryFeePct = fees.spotMakerPct
	}
	return domain.Opportunity{
		OpportunityKey:         fmt.Sprintf("%s/%s:%s:%s", sm.BaseAsset, targetQuote, sm.Symbol, pm.Symbol),
		SpotEntryFeePct:        spotEntryFeePct,
		SpotExitFeePct:         fees.spotTakerPct,
		PerpEntryFeePct:        fees.perpTakerPct,
		PerpExitFeePct:         fees.perpTakerPct,
		FeeSource:              fees.source,
		FeeVerified:            fees.verified,
		FeeFetchedAtMS:         fees.fetchedAtMS,
		FeeAssetSurchargePct:   s.cfg.FeeAssetSurchargePct,
		LocalExchange:          sm.Exchange,
		GlobalExchange:         pm.Exchange,
		BaseAsset:              sm.BaseAsset,
		QuoteAsset:             targetQuote,
		SpotSymbol:             sm.Symbol,
		PerpSymbol:              pm.Symbol,
		ExecutionMode:          execMode,
		SpotEntryPrice:         prices.spotEntry.InexactFloat64(),
		SpotExitPrice:          prices.spotExit.InexactFloat64(),
		PerpEntryPrice:         prices.perpEntry.InexactFloat64(),
		PerpExitPrice:          prices.perpExit.InexactFloat64(),
		Quantity:               sizing.quantity.InexactFloat64(),
		SpreadPnL:              profit.spread.InexactFloat64(),
		FundingPnL:             profit.funding.InexactFloat64(),
		TradingFees:            profit.fees.InexactFloat64(),
		Slippage:               profit.slippage.InexactFloat64(),
		NetProfit:              profit.net.InexactFloat64(),
		AllocatedCapital:       profit.capital.InexactFloat64(),
		NetROIPct:              profit.roi.InexactFloat64(),
		ProfitabilityStatus:    profit.status,
		LastScannedAt:          input.now,
		SpotTargetLimitPrice:   prices.spotEntry.InexactFloat64(),
		MinAcceptablePerpPrice: profit.minAcceptablePerpPrice.InexactFloat64(),
		SpotQueueDepth:         sb.Bids[0].Quantity,
		MaxOrderTTLSeconds:     ttlSeconds,
		IsSynthetic:            input.isSynthetic,
		SyntheticFXRate:        input.fxRate,
		OriginalSpotQuote:      sm.QuoteAsset,
		CapacityPct:            profit.capacityPct,
	}
}

func (s *Service) calculate(
	input opportunityCalculationInput,
) (domain.Opportunity, error) {
	if err := s.validateCalculationInput(input); err != nil {
		return domain.Opportunity{}, err
	}
	sizing, err := s.calculateOpportunitySizing(input)
	if err != nil {
		return domain.Opportunity{}, err
	}
	prices, err := calculateExecutionPrices(
		input.spotBook, input.perpBook, sizing.quantity,
		sizing.makerTaker, sizing.spotEntryRef,
	)
	if err != nil {
		return domain.Opportunity{}, err
	}
	fees := s.resolveOpportunityFees(input)
	profit, err := s.calculateOpportunityProfit(input, sizing, prices, fees)
	if err != nil {
		return domain.Opportunity{}, err
	}
	op := s.buildOpportunity(input, sizing, prices, fees, profit)
	s.evaluateOpportunity(
		&op, input.positionReason,
		decimal.NewFromFloat(input.perpBook.Bids[0].Price).InexactFloat64(),
		sizing.spotEntryRef.InexactFloat64(), input.funding.Rate,
	)
	return op, nil
}
