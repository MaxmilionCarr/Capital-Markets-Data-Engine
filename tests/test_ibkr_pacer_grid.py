import os
import time
from dataclasses import dataclass
from datetime import datetime
from itertools import product
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from data_providers.clients.websockets.IBKR_client import (
    IBKRProvider,
    IBKRConfig,
    _HistPacer,
)

# -------------------------------------------------------
# Config
# -------------------------------------------------------

SYMBOL = "AAPL"
EXCHANGE = "NASDAQ"

START_DATE = datetime(2024, 7, 1, 9, 30)
END_DATE = datetime(2025, 7, 1, 16, 0)  # ~6 months

COOLDOWN_SECONDS = 60  # delay between bulk runs (not per-request pacing)

OUT_DIR = "ibkr_pacer_grid_results"
OUT_CSV = os.path.join(OUT_DIR, "grid_results.csv")
OUT_BEST_CSV = os.path.join(OUT_DIR, "grid_results_best.csv")


# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

def sleep_with_bar(seconds: int, desc: str = "Cooldown") -> None:
    for _ in tqdm(
        range(seconds),
        desc=desc,
        bar_format="{l_bar}{bar}| {remaining}s",
        leave=False,
    ):
        time.sleep(1)


def expected_5min_bars(start: datetime, end: datetime) -> int:
    """
    Rough estimate: ~78 bars per trading day.
    (This is intentionally loose; we score on completeness vs this baseline.)
    """
    days = (end.date() - start.date()).days
    return max(days, 1) * 78


def score_run(elapsed_s: float, requests: int, df: pd.DataFrame) -> float:
    """
    Lower score = better.
    Penalize missing data heavily; lightly penalize request count.
    """
    if df is None or df.empty:
        return float("inf")

    uniq = int(df["datetime"].nunique())
    expected = expected_5min_bars(START_DATE, END_DATE)
    completeness = uniq / max(expected, 1)

    penalty = 0.0
    if completeness < 0.98:
        penalty += (0.98 - completeness) * 10000.0

    return float(elapsed_s) + 0.05 * float(requests) + penalty


def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def save_results_csv(results: List[Dict[str, Any]], csv_path: str) -> pd.DataFrame:
    df = pd.DataFrame(results)
    # Make sure columns are in a nice order if present
    preferred = [
        "ok",
        "score",
        "elapsed",
        "requests",
        "rows",
        "unique_dt",
        "completeness",
        "min_interval",
        "max_10min",
        "adapt",
        "error",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]
    df.to_csv(csv_path, index=False)
    return df


def print_report(df: pd.DataFrame, top_n: int = 10) -> None:
    good = df[df.get("ok", False) == True].copy()
    if good.empty:
        print("\nNo successful runs.\n")
        return

    good = good.sort_values("score", ascending=True)

    print("\n================ BEST CONFIGS ================\n")
    for _, r in good.head(top_n).iterrows():
        print(
            f"score={r['score']:8.2f} | "
            f"time={r['elapsed']:6.1f}s | "
            f"req={int(r['requests']):4d} | "
            f"uniq={int(r['unique_dt']):5d} | "
            f"comp={r['completeness']*100:6.2f}% | "
            f"min={r['min_interval']} "
            f"max10m={r['max_10min']} "
            f"adapt={r['adapt']}"
        )
    print("\n=============================================\n")


# -------------------------------------------------------
# Grid Search
# -------------------------------------------------------

def run_grid_search() -> List[Dict[str, Any]]:
    grid = {
        "min_interval": [0.5],
        "max_10min": [45],
        "adapt": [1.2],
        "stop": [2.0, 3.0, 4.0]
    }

    keys = list(grid.keys())
    combos = list(product(*[grid[k] for k in keys]))

    results: List[Dict[str, Any]] = []

    print(f"Running {len(combos)} configs...\n")

    outer = tqdm(combos, desc="Grid Search", unit="run")

    for vals in outer:
        # bulk delay between runs so you don't hammer IBKR across many runs
        if COOLDOWN_SECONDS > 0:
            sleep_with_bar(COOLDOWN_SECONDS, desc="Cooldown")

        params = dict(zip(keys, vals))
        outer.set_postfix(params)

        pacer = _HistPacer(
            min_interval_s=params["min_interval"],
            max_10min=params["max_10min"],
            adapt_threshold_s=params["adapt"],
            stop_threshold_s=params["stop"],
        )

        cfg = IBKRConfig(pacer=pacer)  # assumes IBKRConfig supports pacer=...
        provider = IBKRProvider(cfg)

        provider.connect()

        try:
            t0 = time.perf_counter()

            df = provider.get_equity_prices(
                symbol=SYMBOL,
                exchange_name=EXCHANGE,
                start_date=START_DATE,
                end_date=END_DATE,
                bar_size="5 mins",
            )

            elapsed = time.perf_counter() - t0

            # NOTE: This is rolling 10-min count in your current pacer. Better is pacer.total_requests.
            reqs = len(getattr(pacer, "_req_times", []))

            uniq = int(df["datetime"].nunique()) if df is not None and not df.empty else 0
            expected = expected_5min_bars(START_DATE, END_DATE)
            completeness = uniq / max(expected, 1)

            score = score_run(elapsed, reqs, df)

            results.append(
                {
                    **params,
                    "elapsed": round(elapsed, 2),
                    "requests": int(reqs),
                    "rows": int(len(df)) if df is not None else 0,
                    "unique_dt": int(uniq),
                    "completeness": float(completeness),
                    "score": round(score, 2),
                    "ok": True,
                    "error": "",
                }
            )

            outer.write(
                f"OK | {elapsed:.1f}s | rows={len(df)} | uniq={uniq} | comp={completeness*100:.2f}%"
            )

        except Exception as e:
            results.append(
                {
                    **params,
                    "elapsed": None,
                    "requests": None,
                    "rows": 0,
                    "unique_dt": 0,
                    "completeness": 0.0,
                    "score": float("inf"),
                    "ok": False,
                    "error": repr(e),
                }
            )
            outer.write(f"FAIL: {e}")

        finally:
            provider.disconnect()

    return results


# -------------------------------------------------------
# Entry
# -------------------------------------------------------

if __name__ == "__main__":
    safe_mkdir(OUT_DIR)

    results = run_grid_search()

    df_all = save_results_csv(results, OUT_CSV)

    # Save best-only CSV too
    df_best = df_all[df_all.get("ok", False) == True].sort_values("score", ascending=True)
    df_best.to_csv(OUT_BEST_CSV, index=False)

    print(f"\nSaved results to:\n  {OUT_CSV}\n  {OUT_BEST_CSV}\n")

    print_report(df_all, top_n=10)
