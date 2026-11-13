package scanner

import "github.com/shopspring/decimal"

type AggressiveSpreadInput struct {
	LocalBestBidIDR       decimal.Decimal
	LocalTickSizeIDR      decimal.Decimal
	GlobalBestBidUSDT     decimal.Decimal
	UsdtIdrBid            decimal.Decimal
	LocalMakerFeePct      decimal.Decimal
	GlobalTakerFeePct     decimal.Decimal
	CurrentFundingRatePct decimal.Decimal
	MinutesToNextFunding  int
}

type AggressiveSpreadResult struct {
	TargetLimitBuyIDR               decimal.Decimal
	EntrySpreadPct                  decimal.Decimal
	MinEntrySpreadPct               decimal.Decimal
	CancelIfGlobalBidDropsBelowUSDT decimal.Decimal
	IsEligible                      bool
}

func EvaluateAggressiveLayer2Spread(in AggressiveSpreadInput) AggressiveSpreadResult {
	one := decimal.NewFromInt(1)
	hundred := decimal.NewFromInt(100)
	minSpreadFloorPct := decimal.RequireFromString("0.08")
	targetLimitBuyIDR := in.LocalBestBidIDR.Add(in.LocalTickSizeIDR)
	globalBestBidIDR := in.GlobalBestBidUSDT.Mul(in.UsdtIdrBid)
	if globalBestBidIDR.LessThanOrEqual(decimal.Zero) || in.UsdtIdrBid.LessThanOrEqual(decimal.Zero) {
		return AggressiveSpreadResult{IsEligible: false}
	}
	entrySpreadPct := globalBestBidIDR.Sub(targetLimitBuyIDR).Div(globalBestBidIDR).Mul(hundred)
	subsidyMultiplier := decimal.RequireFromString("0.35")
	if in.MinutesToNextFunding <= 30 {
		subsidyMultiplier = decimal.RequireFromString("0.65")
	}
	fundingSubsidyPct := in.CurrentFundingRatePct.Mul(subsidyMultiplier)
	rawMinSpreadPct := in.LocalMakerFeePct.Add(in.GlobalTakerFeePct).Sub(fundingSubsidyPct)
	minEntrySpreadPct := decimal.Max(rawMinSpreadPct, minSpreadFloorPct)
	denom := in.UsdtIdrBid.Mul(one.Sub(minEntrySpreadPct.Div(hundred)))
	cancelThresholdUSDT := decimal.Zero
	if denom.GreaterThan(decimal.Zero) {
		cancelThresholdUSDT = targetLimitBuyIDR.Div(denom)
	}
	return AggressiveSpreadResult{
		TargetLimitBuyIDR:               targetLimitBuyIDR,
		EntrySpreadPct:                  entrySpreadPct,
		MinEntrySpreadPct:               minEntrySpreadPct,
		CancelIfGlobalBidDropsBelowUSDT: cancelThresholdUSDT,
		IsEligible:                      entrySpreadPct.GreaterThanOrEqual(minEntrySpreadPct),
	}
}
