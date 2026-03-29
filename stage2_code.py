# -*- coding: utf-8 -*-
"""
STAGE 2 (DELTA FILTER + QC SAVE + APPEND MASTERS) — KEEP-FIRST OVERRIDE

Goal:
- Reduce dataset BEFORE LLM (Stage 3)
- Run on DELTA only (new rows from Stage 1 this week)
- Save BOTH kept and removed rows:
    - delta outputs (this run)
    - master outputs (append over time for QC)

Logic:
1) If title matches KEEP patterns => KEEP (override everything)
2) Else if title matches EXCLUDE patterns => REMOVE
3) Else => KEEP

INPUT (DELTA):
- listings_delta.csv

OUTPUTS:
Delta:
- stage2_filtered_kept_delta.csv
- stage2_filtered_removed_delta.csv

Masters (append, never overwrite):
- stage2_kept_master.csv
- stage2_removed_master.csv
"""

from __future__ import annotations

import os
import re
import pandas as pd
from pathlib import Path

# ============================
# CONFIG
# ============================
# Base directory (same folder as script)
BASE_DIR = Path(__file__).resolve().parent

# Recommended project structure:
# INPUT folder contains Stage 1 delta file
# OUTPUT_STAGE2 stores results

OUTPUT_DIR = BASE_DIR / "OUTPUT_STAGE2"

OUTPUT_DIR.mkdir(exist_ok=True)

# IMPORTANT:
# Make sure Stage 1 outputs file with this name:
IN_CSV = Path(__file__).resolve().parent / "OUTPUT_STAGE1" / "listings_2022tocurr_new_delta.csv"

OUT_KEPT_DELTA = OUTPUT_DIR / "stage2_filtered_kept_delta.csv"
OUT_REMOVED_DELTA = OUTPUT_DIR / "stage2_filtered_removed_delta.csv"

OUT_KEPT_MASTER = OUTPUT_DIR / "stage2_kept_master.csv"
OUT_REMOVED_MASTER = OUTPUT_DIR / "stage2_removed_master.csv"

# ============================
# 1) KEEP-FIRST (Exit-ish titles)
# Anything matching these is ALWAYS kept.
# ============================
KEEP_PATTERNS = [
    # generic exit language
    r"\bexit(s|ing)?\b",
    r"\bexiting\b",
    r"\bwithdraw(s|ing|al)?\b",
    r"\bpull(s|ing)?\s+back\b",
    r"\bpullback\b",
    r"\bleave(s|ing)?\b",
    r"\bdrop(s|ping)?\b",
    r"\bterminate(s|d|ing)?\b",
    r"\btermination\b",
    r"\bcontract(s)?\s+(end|ends|ended|expir(e|es|ed|ing))\b",
    r"\bset\s+to\s+expire\b",
    r"\bexpir(e|es|ed|ing)\b",

    # network / contracting terms
    r"\bout[-\s]?of[-\s]?network\b",
    r"\bgoing\s+out[-\s]?of[-\s]?network\b",
    r"\bin[-\s]?network\b",
    r"\bnetwork\b.*\b(exit|termination|terminate|split|drop|leave)\b",
    r"\bsplit(s)?\b",
    r"\bpart\s+ways\b",
    r"\bcontracting\b",

    # market exit types you explicitly want
    r"\bACA\b|\bAffordable Care Act\b",
    r"\bMedicare Advantage\b|\bMA\b",
    r"\bMedicaid\b",
    r"\bSNP\b|\bspecial\s+needs\b",
    r"\bexchange\b",  # ACA exchange exit headlines

    # verbs often used in Beckers headlines
    r"\bto\s+exit\b",
    r"\bto\s+leave\b",
    r"\bto\s+drop\b",
]

KEEP_RE = re.compile("|".join(KEEP_PATTERNS), flags=re.IGNORECASE)


# ============================
# 2) EXCLUDE (Noise titles)
# Only applied if KEEP didn't match.
# Keep this list conservative.
# ============================
EXCLUDE_PATTERNS = [
    # Finance / performance
    r"\boperating margin\b",
    r"\boperating gain\b",
    r"\bearnings\b|\brevenue\b|\bprofit(ability)?\b|\bnet income\b|\bEBITDA\b",
    r"\bquarterly\b|\bquarter\b|\bQ[1-4]\b|\bdividend\b",
    r"\bguidance\b|\boutlook\b|\bforecast\b",

    # Exec moves
    r"\bappoint(ed|ment)\b|\bpromot(ed|ion)\b|\bretire(s|d|ment)?\b|\bboard\b",

    # Tech / product launches
    r"\bnew tool\b|\blaunch(es|ed)?\b|\bpartnership\b|\bproduct\b",
    r"\bnew designation\b",

    # Credit ratings
    r"\bcredit rating\b|\brating(s)?\b|\bupgrade\b|\bdowngrade\b|\bMoody'?s\b|\bFitch\b|\bS&P\b",

    # Fines / settlements
    r"\bfined\b|\bpenalt(y|ies)\b|\bsettlement\b",

    # Research / surveys / reports
    r"\bstudy\b|\bfindings\b|\breport finds\b|\bsurvey\b",

    # M&A
    r"\bacquir(ed|es|ing)\b",
    r"\baccquire(d|s|ing)?\b",

    # Policy / govt (non-exit)
    r"\bbill\b",
    r"\bfederal support\b",
    r"\bbudget\b",
    r"\btrump\b",

    # Drug coverage / benefits (your big false positives)
    r"\bdrug(s)?\b.*\bcoverage\b|\bdrug coverage\b",
    r"\bGLP-1\b|\bweight loss\b|\bOzempic\b|\bWegovy\b|\bZepbound\b|\bMounjaro\b",
    r"\bformulary\b",
    r"\bprior authorization\b|\bprior auth\b",
    r"\bpharmacy benefit\b",
    r"\bPBM\b|\bCaremark\b|\bExpress Scripts\b|\bOptumRx\b",

    # Events
    r"\bwebinar\b|\bevent\b|\bconference\b",
]

EXCLUDE_RE = re.compile("|".join(EXCLUDE_PATTERNS), flags=re.IGNORECASE)


# ============================
# HELPERS
# ============================
def append_csv(path: str, df_new: pd.DataFrame):
    if df_new.empty:
        return
    exists = os.path.exists(path)
    df_new.to_csv(path, mode="a", header=not exists, index=False, encoding="utf-8-sig")


def first_match(regex: re.Pattern, text: str) -> str:
    if not isinstance(text, str):
        return ""
    m = regex.search(text)
    return m.group(0) if m else ""


# ============================
# MAIN
# ============================
def main():
    df = pd.read_csv(IN_CSV)

    if "title" not in df.columns:
        raise ValueError("Input CSV must contain a 'title' column.")

    # KEEP-first
    df["keep_reason"] = df["title"].apply(lambda t: first_match(KEEP_RE, t))
    df["is_keep_override"] = df["keep_reason"] != ""

    # EXCLUDE only if not kept
    df["excluded_reason"] = ""
    mask_check_exclude = ~df["is_keep_override"]
    df.loc[mask_check_exclude, "excluded_reason"] = df.loc[mask_check_exclude, "title"].apply(lambda t: first_match(EXCLUDE_RE, t))

    df["__exclude__"] = (df["excluded_reason"] != "") & (~df["is_keep_override"])

    total = len(df)
    removed = int(df["__exclude__"].sum())
    kept = total - removed
    kept_by_override = int(df["is_keep_override"].sum())

    print(f"\n[stage2-delta] total={total} removed={removed} kept={kept}")
    print(f"[stage2-delta] kept_by_keep_override={kept_by_override}")
    print(f"[stage2-delta] input (delta) = {IN_CSV}")

    df_removed = df[df["__exclude__"]].drop(columns=["__exclude__"])
    df_kept = df[~df["__exclude__"]].drop(columns=["__exclude__"])

    # Save delta outputs (overwrite each run)
    df_kept.to_csv(OUT_KEPT_DELTA, index=False, encoding="utf-8-sig")
    df_removed.to_csv(OUT_REMOVED_DELTA, index=False, encoding="utf-8-sig")

    # Append into master QC files (never overwrite)
    append_csv(OUT_KEPT_MASTER, df_kept)
    append_csv(OUT_REMOVED_MASTER, df_removed)

    print(f"\n[done] kept DELTA      -> {OUT_KEPT_DELTA} ({len(df_kept)} rows)")
    print(f"[done] removed DELTA   -> {OUT_REMOVED_DELTA} ({len(df_removed)} rows)")
    print(f"[done] kept MASTER     -> {OUT_KEPT_MASTER} (appended {len(df_kept)} rows)")
    print(f"[done] removed MASTER  -> {OUT_REMOVED_MASTER} (appended {len(df_removed)} rows)")

    print("\n--- Sample REMOVED titles (this run) ---")
    if not df_removed.empty:
        print(df_removed[["title", "excluded_reason"]].head(20).to_string(index=False))
    else:
        print("(none removed)")

    print("\n--- Sample KEEP-override titles (this run) ---")
    df_keepov = df[df["is_keep_override"]]
    if not df_keepov.empty:
        print(df_keepov[["title", "keep_reason"]].head(20).to_string(index=False))
    else:
        print("(none kept by override)")


if __name__ == "__main__":
    main()
