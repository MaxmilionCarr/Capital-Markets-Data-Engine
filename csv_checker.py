import os
import math
import argparse
import pandas as pd


PRICE_COLS_DEFAULT = ["open", "high", "low", "close", "volume"]


def _read_price_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    df = pd.read_csv(path)

    # Accept either "date" or "datetime"
    dt_col = None
    for c in ["datetime", "date", "timestamp", "time"]:
        if c in df.columns:
            dt_col = c
            break
    if dt_col is None:
        raise ValueError(f"{path}: expected a datetime column like 'date' or 'datetime'")

    # Parse datetime
    df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
    df = df.dropna(subset=[dt_col]).copy()

    # Set index on datetime
    df = df.set_index(dt_col).sort_index()

    # Drop obvious junk index column if present
    if "index" in df.columns:
        # keep it only if it looks meaningful; otherwise drop
        if pd.api.types.is_numeric_dtype(df["index"]) and df["index"].is_monotonic_increasing:
            df = df.drop(columns=["index"])

    # Standardize column names (lower)
    df.columns = [c.strip().lower() for c in df.columns]

    return df


def _safe_corr(a: pd.Series, b: pd.Series) -> float | None:
    if a is None or b is None:
        return None
    if len(a) < 2:
        return None
    if a.nunique() <= 1 or b.nunique() <= 1:
        return None
    return float(a.corr(b))


def _mae(x: pd.Series) -> float:
    return float(x.abs().mean()) if len(x) else float("nan")


def _rmse(x: pd.Series) -> float:
    if not len(x):
        return float("nan")
    return float(math.sqrt((x * x).mean()))


def compare_prices(
    path_a: str,
    path_b: str,
    cols: list[str],
    out_csv: str,
    tolerance: float,
):
    a = _read_price_csv(path_a)
    b = _read_price_csv(path_b)

    # Keep only requested columns that exist in both
    cols = [c.lower() for c in cols]
    missing_a = [c for c in cols if c not in a.columns]
    missing_b = [c for c in cols if c not in b.columns]
    if missing_a:
        raise ValueError(f"{path_a}: missing columns {missing_a}")
    if missing_b:
        raise ValueError(f"{path_b}: missing columns {missing_b}")

    a = a[cols].copy()
    b = b[cols].copy()

    # Align on timestamps
    merged = a.join(b, how="inner", lsuffix="_a", rsuffix="_b")
    if merged.empty:
        raise ValueError("No overlapping timestamps between the two CSVs")

    # Coverage stats
    a_ts = a.index
    b_ts = b.index
    overlap_ts = merged.index

    coverage = {
        "rows_a": len(a),
        "rows_b": len(b),
        "rows_overlap": len(merged),
        "overlap_pct_of_a": len(overlap_ts) / max(len(a_ts), 1),
        "overlap_pct_of_b": len(overlap_ts) / max(len(b_ts), 1),
        "min_ts_overlap": str(overlap_ts.min()),
        "max_ts_overlap": str(overlap_ts.max()),
    }

    # Per-column diffs
    report_rows = []
    summary_rows = []

    for c in cols:
        xa = merged[f"{c}_a"].astype(float)
        xb = merged[f"{c}_b"].astype(float)
        diff = xa - xb
        absdiff = diff.abs()

        exact_match_rate = float((absdiff == 0).mean())
        within_tol_rate = float((absdiff <= tolerance).mean())

        col_summary = {
            "column": c,
            "mae": _mae(diff),
            "rmse": _rmse(diff),
            "max_abs": float(absdiff.max()),
            "mean_diff": float(diff.mean()),
            "median_diff": float(diff.median()),
            "exact_match_rate": exact_match_rate,
            "within_tolerance_rate": within_tol_rate,
            "corr": _safe_corr(xa, xb),
        }
        summary_rows.append(col_summary)

        # Add detailed rows for biggest differences
        topk = merged[[f"{c}_a", f"{c}_b"]].copy()
        topk["absdiff"] = absdiff
        topk = topk.sort_values("absdiff", ascending=False).head(25)

        for ts, r in topk.iterrows():
            report_rows.append(
                {
                    "timestamp": ts,
                    "column": c,
                    "a": r[f"{c}_a"],
                    "b": r[f"{c}_b"],
                    "absdiff": r["absdiff"],
                    "diff": r[f"{c}_a"] - r[f"{c}_b"],
                }
            )

    summary_df = pd.DataFrame(summary_rows).sort_values("column")
    report_df = pd.DataFrame(report_rows).sort_values(["column", "absdiff"], ascending=[True, False])

    # Save
    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    report_df.to_csv(out_csv, index=False)

    # Print summary
    print("\n================= COVERAGE =================")
    for k, v in coverage.items():
        if isinstance(v, float):
            print(f"{k:18s}: {v:.4f}")
        else:
            print(f"{k:18s}: {v}")

    print("\n============== COLUMN SUMMARY ==============")
    print(summary_df.to_string(index=False))

    print(f"\nDetailed differences saved to: {out_csv}")

    return coverage, summary_df, report_df


def main():
    ap = argparse.ArgumentParser(description="Compare two OHLCV price CSVs (aligned by timestamp).")
    ap.add_argument("csv_a", help="First CSV (baseline)")
    ap.add_argument("csv_b", help="Second CSV (comparison)")
    ap.add_argument("--cols", nargs="+", default=PRICE_COLS_DEFAULT, help="Columns to compare")
    ap.add_argument("--tolerance", type=float, default=0.0, help="Abs tolerance for 'match' checks")
    ap.add_argument("--out", default="price_csv_comparison_report.csv", help="Output report CSV")
    args = ap.parse_args()

    compare_prices(
        path_a=args.csv_a,
        path_b=args.csv_b,
        cols=args.cols,
        out_csv=args.out,
        tolerance=args.tolerance,
    )


if __name__ == "__main__":
    main()
