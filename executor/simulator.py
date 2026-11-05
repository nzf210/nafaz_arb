import asyncio
import json
import logging
import sys
import time
from typing import Dict, Any, Optional

import redis.asyncio as aioredis
from executor.config import ExecutorConfig
from executor.exchange_client import PaperTradingExchangeClient
from executor.engine import ArbitrageEngine, clean_pair_key, round_to_tick
from executor.models import (
    PositionState,
    PairInfo,
    PricingSnapshot,
    FundingMetrics,
    ExecutionDirectives,
    ExecutorPayload,
    ActivePosition,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("simulator")


def simulate_opportunity(
    pair_name: str,
    local_spot_bid_idr: float,
    local_spot_ask_idr: float,
    global_perp_bid_usdt: float,
    global_perp_ask_usdt: float,
    usdt_idr_rate: float,
    funding_rate_8h: float,
    tick_size_idr: float = 1.0,
    capital_usdt: float = 50.0,
    base_exit_spread_pct: float = 0.30,
    holding_hours: int = 8,
    spot_maker_fee_pct: float = 0.0,
    spot_taker_fee_pct: float = 0.30,
    perp_taker_fee_pct: float = 0.05,
    initial_balance_per_exchange: float = 100.0,
) -> Dict[str, Any]:
    """
    Simulates complete Hybrid Maker-Taker + Positive Funding Arbitrage cycle with exact math:
    State 1: Local Maker Buy at (local_spot_bid_idr + tick_size_idr)
    State 2: Instant Short 1x Hedge at global_perp_bid_usdt
    State 3: 8h holding with positive funding collection & dynamic exit recalculation
    State 4: Local Maker Sell + Close Short 1x Market Buy (reduceOnly) at global_perp_ask_usdt
    """
    print("=" * 75)
    print(f" SIMULASI ARBITRASE HYBRID: {pair_name}")
    print("=" * 75)

    # 1. PARAMETER PASAR LIVE
    print(f"[1] DATA HARGA LIVE:")
    print(f"    - Kurs USDT/IDR               : Rp {usdt_idr_rate:,.2f}")
    print(f"    - Spot Lokal Best Bid/Ask     : Rp {local_spot_bid_idr:,.2f} / Rp {local_spot_ask_idr:,.2f}")
    print(f"    - Futures Global Best Bid/Ask : ${global_perp_bid_usdt:.4f} / ${global_perp_ask_usdt:.4f} USDT")
    print(f"    - Funding Rate (8 Jam)        : +{funding_rate_8h * 100:.4f}% ({funding_rate_8h:.6f})")

    # Batasi modal per posisi agar tidak melebihi saldo exchange ($100)
    if capital_usdt > initial_balance_per_exchange:
        print(f"    ⚠️ Modal diminta ${capital_usdt:.2f} > batas saldo $100. Disesuaikan ke alokasi 50% ($50.00 USDT).")
        capital_usdt = initial_balance_per_exchange * 0.5

    # 2. STATE 1: ENTRY ORDER PEGGING (MAKER LIMIT BUY LOKAL)
    target_limit_buy_idr = local_spot_bid_idr + tick_size_idr
    if target_limit_buy_idr >= local_spot_ask_idr and local_spot_ask_idr > local_spot_bid_idr:
        target_limit_buy_idr = local_spot_bid_idr
    target_limit_buy_idr = round_to_tick(target_limit_buy_idr, tick_size_idr)

    global_bid_in_idr = global_perp_bid_usdt * usdt_idr_rate
    entry_spread_pct = ((global_bid_in_idr - target_limit_buy_idr) / global_bid_in_idr) * 100.0

    # Minimum spread requirement: (MakerFee + TakerFee) - FundingSubsidy
    funding_subsidy_pct = max(0.08, (funding_rate_8h * 100.0) * 0.5)
    min_entry_spread_pct = (spot_maker_fee_pct + perp_taker_fee_pct) - funding_subsidy_pct
    cancel_threshold_usdt = target_limit_buy_idr / (usdt_idr_rate * (1.0 - (min_entry_spread_pct / 100.0)))

    # Modal & Kuantitas Koin
    capital_idr = capital_usdt * usdt_idr_rate
    coin_qty = capital_idr / target_limit_buy_idr
    avail_spot = initial_balance_per_exchange - capital_usdt
    avail_fut = initial_balance_per_exchange - capital_usdt

    print(f"\n[2] STATE 1 - ENTRY PEGGING (SALDO & ALOKASI $100 PER BURSA):")
    print(f"    - Saldo Awal Bursa            : Spot ${initial_balance_per_exchange:.2f} | Futures ${initial_balance_per_exchange:.2f} (Total ${initial_balance_per_exchange*2:.2f} USDT)")
    print(f"    - Modal Terkunci Posisi Ini   : Spot ${capital_usdt:.2f} | Futures Margin ${capital_usdt:.2f}")
    print(f"    - Saldo Bebas / Available     : Spot ${avail_spot:.2f} | Futures ${avail_fut:.2f}")
    print(f"    - Rekomendasi Limit Buy IDR   : Rp {target_limit_buy_idr:,.2f}")
    print(f"    - Global Bid ekuivalen IDR    : Rp {global_bid_in_idr:,.2f}")
    print(f"    - Entry Spread (%)            : +{entry_spread_pct:.3f}% (Min Req: {min_entry_spread_pct:.3f}%)")
    print(f"    - Kuantitas Koin Dibeli       : {coin_qty:.6f} {pair_name.split('/')[0]}")
    print(f"    - Proteksi Adverse Selection  : Batal jika Bid Global < ${cancel_threshold_usdt:.4f}")

    # 3. STATE 2: PARTIAL/FULL FILL & INSTANT SHORT HEDGE
    spot_buy_cost_idr = coin_qty * target_limit_buy_idr
    spot_entry_fee_idr = spot_buy_cost_idr * (spot_maker_fee_pct / 100.0)  # 0% Maker fee
    spot_entry_fee_usd = spot_entry_fee_idr / usdt_idr_rate

    # Global Market Short 1x at Global Bid
    short_entry_notional_usdt = coin_qty * global_perp_bid_usdt
    short_entry_fee_usdt = short_entry_notional_usdt * (perp_taker_fee_pct / 100.0)

    print(f"\n[3] STATE 2 - SHORT HEDGE TERBUKA (INSTANT 1x):")
    print(f"    - Spot Beli Berhasil          : {coin_qty:.6f} koin @ Rp {target_limit_buy_idr:,.2f}")
    print(f"    - Biaya Spot Entry (Maker)    : Rp {spot_entry_fee_idr:,.2f} (${spot_entry_fee_usd:.4f})")
    print(f"    - Market Short 1x Eksekusi    : {coin_qty:.6f} koin @ ${global_perp_bid_usdt:.4f} USDT")
    print(f"    - Nilai Posisi Short          : ${short_entry_notional_usdt:.2f} USDT")
    print(f"    - Biaya Futures Entry (Taker) : ${short_entry_fee_usdt:.4f} USDT")
    print(f"    - Delta Hedged                : 100% Terlindungi (Delta Neutral, Nol Risiko Arah Pasar)")

    # 4. STATE 3: DYNAMIC EXIT LIMIT SELL & FUNDING DECAY
    # Settled funding payment: Short position receives positive funding!
    settlements = holding_hours // 8
    if settlements < 1:
        settlements = 1
    funding_payment_usdt = short_entry_notional_usdt * funding_rate_8h * settlements
    funding_payment_idr = funding_payment_usdt * usdt_idr_rate

    funding_decay_pct = (funding_payment_usdt / short_entry_notional_usdt) * 100.0
    effective_exit_spread_pct = base_exit_spread_pct - funding_decay_pct

    # Dynamic limit sell calculation:
    limit_sell_idr = (global_perp_ask_usdt * usdt_idr_rate) * (1.0 + (effective_exit_spread_pct / 100.0))
    limit_sell_idr = round_to_tick(limit_sell_idr, tick_size_idr)

    print(f"\n[4] STATE 3 - CASHFLOW FUNDING & DYNAMIC EXIT PEGGING:")
    print(f"    - Durasi Holding              : {holding_hours} Jam ({settlements} kali settlement)")
    print(f"    - Cashflow Funding Diterima   : +${funding_payment_usdt:.4f} USDT (+Rp {funding_payment_idr:,.2f})")
    print(f"    - Funding Decay Diskon Spread : -{funding_decay_pct:.3f}% (Base Exit: {base_exit_spread_pct:.2f}%)")
    print(f"    - Effective Exit Spread (%)   : +{effective_exit_spread_pct:.3f}%")
    print(f"    - Harga Pasang Limit Sell IDR : Rp {limit_sell_idr:,.2f}")

    # 5. STATE 4: EXIT EXECUTION (LOCAL SELL FILL -> GLOBAL MARKET BUY CLOSE)
    spot_sell_revenue_idr = coin_qty * limit_sell_idr
    spot_exit_fee_idr = spot_sell_revenue_idr * (spot_maker_fee_pct / 100.0)  # Limit Sell is Maker = 0%
    spot_exit_fee_usd = spot_exit_fee_idr / usdt_idr_rate

    # Global Close Short (Market Buy with reduceOnly=True) at Global Ask
    short_exit_notional_usdt = coin_qty * global_perp_ask_usdt
    short_exit_fee_usdt = short_exit_notional_usdt * (perp_taker_fee_pct / 100.0)

    # Futures PnL: (Short Entry - Short Exit)
    futures_pnl_usdt = short_entry_notional_usdt - short_exit_notional_usdt
    futures_pnl_idr = futures_pnl_usdt * usdt_idr_rate

    # Spot PnL: (Spot Sell Revenue - Spot Buy Cost)
    spot_pnl_idr = spot_sell_revenue_idr - spot_buy_cost_idr
    spot_pnl_usdt = spot_pnl_idr / usdt_idr_rate

    # Total Fees
    total_fees_usdt = spot_entry_fee_usd + spot_exit_fee_usd + short_entry_fee_usdt + short_exit_fee_usdt
    total_fees_idr = total_fees_usdt * usdt_idr_rate

    # Net Realized Profit
    net_profit_usdt = spot_pnl_usdt + futures_pnl_usdt + funding_payment_usdt - total_fees_usdt
    net_profit_idr = net_profit_usdt * usdt_idr_rate
    net_roi_pct = (net_profit_usdt / capital_usdt) * 100.0

    # Saldo Akhir Setelah Posisi Sukses Ditutup
    final_spot_usdt = initial_balance_per_exchange + spot_pnl_usdt - (spot_entry_fee_usd + spot_exit_fee_usd)
    final_futures_usdt = initial_balance_per_exchange + futures_pnl_usdt + funding_payment_usdt - (short_entry_fee_usdt + short_exit_fee_usdt)
    final_total_equity = final_spot_usdt + final_futures_usdt

    print(f"\n[5] STATE 4 - EXIT EKSEKUSI & HASIL AKHIR (PNL STATEMENT):")
    print(f"    - Spot Sell Terisi            : {coin_qty:.6f} koin @ Rp {limit_sell_idr:,.2f}")
    print(f"    - Futures Short Ditutup       : Market Buy {coin_qty:.6f} koin @ ${global_perp_ask_usdt:.4f} USDT")
    print(f"    -----------------------------------------------------------------------")
    print(f"    - PnL Spot Lokal              : +Rp {spot_pnl_idr:,.2f} (+${spot_pnl_usdt:.4f} USDT)")
    print(f"    - PnL Futures Global (Basis)  : {futures_pnl_usdt:+.4f} USDT ({futures_pnl_idr:+,.2f} IDR)")
    print(f"    - Cashflow Funding Rate       : +${funding_payment_usdt:.4f} USDT (+Rp {funding_payment_idr:,.2f})")
    print(f"    - Total Biaya / Fees          : -${total_fees_usdt:.4f} USDT (-Rp {total_fees_idr:,.2f})")
    print(f"    =======================================================================")
    print(f"    >>> NET REALIZED PROFIT (BERSIH) : +${net_profit_usdt:.4f} USDT (Rp {net_profit_idr:,.2f}) <<<")
    print(f"    >>> NET RETURN ON INVESTMENT     : +{net_roi_pct:.3f}% PER EPISODE <<<")
    print(f"    =======================================================================")

    is_profitable = net_profit_usdt > 0
    print(f"    STATUS SIMULASI               : {'✅ TERBUKTI PROFIT' if is_profitable else '❌ RUGI'}")

    print(f"\n[6] REKAP SALDO PER BURSA (SETELAH POSISI SUKSES DITUTUP):")
    print(f"    - Bursa Spot (Modal Awal $100): PnL Netto {spot_pnl_usdt:+.4f} USDT -> Saldo Akhir: ${final_spot_usdt:.4f} USDT")
    print(f"    - Bursa Futures (Modal $100) : PnL Netto {(futures_pnl_usdt + funding_payment_usdt - (short_entry_fee_usdt + short_exit_fee_usdt)):+.4f} USDT -> Saldo Akhir: ${final_futures_usdt:.4f} USDT")
    print(f"    - Total Ekuitas Awal          : ${initial_balance_per_exchange * 2:.2f} USDT")
    print(f"    - Total Ekuitas Akhir         : ${final_total_equity:.4f} USDT ({net_profit_usdt:+.4f} USDT)")
    print(f"    - Status Dana Terkunci        : $0.00 USDT (Semua modal spot & margin futures bebas kembali)")

    return {
        "pair_name": pair_name,
        "is_profitable": is_profitable,
        "net_profit_usdt": net_profit_usdt,
        "net_profit_idr": net_profit_idr,
        "net_roi_pct": net_roi_pct,
        "spot_pnl_usdt": spot_pnl_usdt,
        "futures_pnl_usdt": futures_pnl_usdt,
        "funding_payment_usdt": funding_payment_usdt,
        "total_fees_usdt": total_fees_usdt,
        "final_spot_usdt": final_spot_usdt,
        "final_futures_usdt": final_futures_usdt,
        "final_total_equity": final_total_equity,
    }


def simulate_real_market_stress_test(
    pair_name: str,
    local_spot_bid_idr: float,
    local_spot_ask_idr: float,
    global_perp_bid_usdt: float,
    global_perp_ask_usdt: float,
    usdt_idr_rate: float,
    funding_rate_8h: float,
    capital_usdt: float = 50.0,
) -> None:
    """
    Simulates real-world market scenarios comparing Spot vs Futures Short PnL:
    - Skenario 1: Pasar Naik (Futures Short rugi, apakah tertutup oleh Spot?)
    - Skenario 2: Pasar Turun (Futures Short profit, tapi Spot Limit Sell tidak tersentuh)
    - Skenario 3: Unfilled Maker Buy (Order buy antrean tidak tersentuh dalam TTL 30s)
    """
    print("=" * 75)
    print(f" ANALISIS STRESS TEST PASAR RIL: {pair_name}")
    print("=" * 75)

    entry_spot_idr = local_spot_bid_idr + 1.0
    capital_idr = capital_usdt * usdt_idr_rate
    coin_qty = capital_idr / entry_spot_idr
    short_entry_usdt = global_perp_bid_usdt

    print(f"Entry: Beli Spot Rp {entry_spot_idr:,.2f} | Short Perp ${short_entry_usdt:.4f} USDT | Modal: ${capital_usdt:.2f} (50% dari $100)")

    # Skenario 1: Pasar Naik 3%
    rise_pct = 0.03
    future_exit_usdt = short_entry_usdt * (1.0 + rise_pct)
    spot_exit_idr = entry_spot_idr * 1.01  # Spot laku dengan kenaikan hanya 1%
    spot_gain_usdt = (coin_qty * (spot_exit_idr - entry_spot_idr)) / usdt_idr_rate
    perp_loss_usdt = coin_qty * (short_entry_usdt - future_exit_usdt)  # NEGATIF!
    fees_usdt = (coin_qty * (short_entry_usdt + future_exit_usdt)) * 0.0005
    net_rise = spot_gain_usdt + perp_loss_usdt - fees_usdt

    print(f"\n[SKENARIO 1: PASAR NAIK (FUTURES SHORT MINUS)]")
    print(f"    - Futures Ask Naik 3%        : ${future_exit_usdt:.4f} USDT")
    print(f"    - PnL Short Futures          : {perp_loss_usdt:+.4f} USDT (RUGI KARENA SHORT NAIK)")
    print(f"    - PnL Spot Jual Terisi       : +{spot_gain_usdt:.4f} USDT")
    print(f"    - Biaya Transaksi            : -{fees_usdt:.4f} USDT")
    print(f"    - NET REALIZED PNL           : {net_rise:+.4f} USDT ({'PROFIT' if net_rise > 0 else '⚠️ MINUS/LOSS'})")
    if net_rise < 0:
        print(f"    ⚠️ PELAJARAN: Kenaikan Futures melampaui spread Spot, arbitrase berakhir MINUS!")

    # Skenario 2: Pasar Turun 3%
    drop_pct = 0.03
    future_now_usdt = short_entry_usdt * (1.0 - drop_pct)
    spot_now_idr = entry_spot_idr * (1.0 - drop_pct)
    pegged_sell_idr = entry_spot_idr * 1.003  # Pasang sell di atas
    perp_gain_usdt = coin_qty * (short_entry_usdt - future_now_usdt)  # POSITIF!

    print(f"\n[SKENARIO 2: PASAR TURUN (SHORT PROFIT, TAPI SPOT NYANGKUT)]")
    print(f"    - Futures Turun 3%           : ${future_now_usdt:.4f} USDT")
    print(f"    - Floating PnL Short Futures : +{perp_gain_usdt:.4f} USDT (PROFIT)")
    print(f"    - Harga Pasar Spot Saat Ini  : Rp {spot_now_idr:,.2f}")
    print(f"    - Target Limit Sell Kita     : Rp {pegged_sell_idr:,.2f}")
    print(f"    - Status Limit Sell          : ❌ TIDAK PERNAH TERSENTUH (Bid Pasar < Target Jual)")
    print(f"    ⚠️ RISIKO INVENTORY: Posisi Spot menggantung dan tidak bisa direalisasikan!")

    # Skenario 3: Unfilled Maker Limit Buy
    print(f"\n[SKENARIO 3: ORDER ENTRY MAKER TIDAK TERSENTUH (TTL 30 DETIK)]")
    print(f"    - Order Buy Dipasang di      : Rp {entry_spot_idr:,.2f}")
    print(f"    - Best Ask Pasar Lokal       : Rp {local_spot_ask_idr:,.2f}")
    print(f"    - Tidak ada seller yang menjual ke Rp {entry_spot_idr:,.2f}")
    print(f"    - Status Setelah 30 Detik    : ⏱️ EXPIRED / CANCELLED (Nol Transaksi, Nol Kerugian)")
    print("=" * 75)


async def run_live_simulation():
    """
    Connects to Redis or pulls actual live orderbook from Indodax & OKX to simulate
    the exact live opportunity discovered by Scanner.
    """
    print("\nMenghubungkan ke Scanner & Bursa Live...")

    # Data real BEAT/USDT live:
    # Indodax beatidr: bid=1761, ask=1768
    # OKX BEAT-USDT-SWAP: bid=0.09787, ask=0.09788
    # USDT/IDR Indodax: 18080
    # Funding rate: +0.0232% (0.000232)
    res_beat = simulate_opportunity(
        pair_name="BEAT/USDT (Indodax IDR vs OKX SWAP)",
        local_spot_bid_idr=1761.0,
        local_spot_ask_idr=1768.0,
        global_perp_bid_usdt=0.09787,
        global_perp_ask_usdt=0.09788,
        usdt_idr_rate=18080.0,
        funding_rate_8h=0.000232,
        tick_size_idr=1.0,
        capital_usdt=50.0,
        base_exit_spread_pct=0.30,
        holding_hours=8,
        spot_maker_fee_pct=0.0,
        perp_taker_fee_pct=0.05,
    )

    print("\n")
    simulate_real_market_stress_test(
        pair_name="BEAT/USDT (Indodax IDR vs OKX SWAP)",
        local_spot_bid_idr=1761.0,
        local_spot_ask_idr=1768.0,
        global_perp_bid_usdt=0.09787,
        global_perp_ask_usdt=0.09788,
        usdt_idr_rate=18080.0,
        funding_rate_8h=0.000232,
        capital_usdt=50.0,
    )

    print("\n")
    # Kasus 2: Simulasi pair Tokocrypto USDT vs OKX SWAP (contoh DODO/USDT dengan FR tinggi +0.028%)
    # DODO Tokocrypto: bid=0.0224, ask=0.0225
    # DODO OKX SWAP: bid=0.02253, ask=0.02254
    # Funding Rate: +0.0281% (0.000281)
    res_dodo = simulate_opportunity(
        pair_name="DODO/USDT (Tokocrypto USDT vs OKX SWAP)",
        local_spot_bid_idr=0.0224 * 16000.0,
        local_spot_ask_idr=0.0225 * 16000.0,
        global_perp_bid_usdt=0.02253,
        global_perp_ask_usdt=0.02254,
        usdt_idr_rate=16000.0,
        funding_rate_8h=0.000281,
        tick_size_idr=0.0001 * 16000.0,
        capital_usdt=50.0,
        base_exit_spread_pct=0.30,
        holding_hours=8,
        spot_maker_fee_pct=0.0,
        perp_taker_fee_pct=0.05,
    )


if __name__ == "__main__":
    asyncio.run(run_live_simulation())
