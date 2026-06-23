#!/usr/bin/env python3
"""

File Name    : s07_priority_score.py
Author       : Bryndis Yngvadottir
Created On   : 23/06/2026
Last Modified: 23/06/2026

Description:
Compute a variant priority score and tier from the selected-VEP-columns TSV
produced by s06_select_vep_cols.py.

The score is a continuous 0-100 value built from four weighted components:
  1. IMPACT      - VEP consequence severity (HIGH/MODERATE/LOW/MODIFIER)
  2. ClinVar     - clinical significance (pathogenic ... benign)
  3. In silico   - pathogenicity predictors (REVEL, CADD_PHRED, AlphaMissense)
  4. Rarity      - population allele frequency (gnomAD POPMAX, MAX_AF fallback)

Each component is normalised to 0-1, multiplied by its weight (weights sum to
the max score), and the four contributions are summed. The per-component
contributions are written out alongside the total so the score is auditable.

A discrete tier (1 = highest priority, 4 = lowest) is also derived, combining
ClinVar status, IMPACT and allele frequency. Tiers are coarse filters; the
numeric score ranks finely within them.

All weights and thresholds live in the accompanying config file
(s07_priority_score_config.json) so the scheme can be tuned without editing code.

This step is pure Python/pandas and does NOT require bcftools or a conda env.

Usage:
python s07_priority_score.py -p <project> -i <base_dir> --config <config_file>

<project>:     project name, used in file names and logs
<base_dir>:    path to base directory (same layout as previous steps)
<config_file>: JSON config with weights and thresholds

Input : <base_dir>/<project>/output/s6_select_vep_cols.tsv
Output: <base_dir>/<project>/output/s7_priority_score.tsv

Dependencies:
pandas

"""

import sys
import re
import time
import json
import logging
from pathlib import Path
from datetime import datetime
import argparse

import pandas as pd

# Import shared utils
from utils import format_runtime


# ----------------------------
# Missing-value handling
# ----------------------------
_MISSING = {"", ".", "na", "nan", "none", "null"}


def parse_float(value):
    """Parse a VEP field into a float, returning None for missing/unparseable.

    Handles '&'-joined multi-values (keeps the max) and embedded scores such
    as SIFT/PolyPhen 'deleterious(0.02)' by extracting the first number.
    """
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() in _MISSING:
        return None

    candidates = []
    for part in re.split(r"[&,;|]", s):
        m = re.search(r"-?\d+(\.\d+)?([eE][-+]?\d+)?", part)
        if m:
            candidates.append(float(m.group()))
    if not candidates:
        return None
    return max(candidates)


def is_missing(value):
    return value is None or str(value).strip().lower() in _MISSING


# ----------------------------
# Component scorers (each returns a 0-1 normalised value)
# ----------------------------
def score_impact(impact, cfg):
    if is_missing(impact):
        return 0.0
    table = cfg["impact_scores"]
    # IMPACT may be '&'-joined across consequences; take the most severe.
    best = 0.0
    for token in re.split(r"[&,]", str(impact)):
        best = max(best, table.get(token.strip().upper(), 0.0))
    return best


def classify_clinvar(clnsig):
    """Map a ClinVar CLNSIG string onto a coarse category, or None."""
    if is_missing(clnsig):
        return None
    s = str(clnsig).lower()
    if "conflicting" in s:
        return "conflicting"
    if re.search(r"\bpathogenic\b", s):          # standalone 'Pathogenic'
        return "pathogenic"
    if "likely_pathogenic" in s or "likely pathogenic" in s:
        return "likely_pathogenic"
    if "uncertain" in s:
        return "uncertain"
    if "likely_benign" in s or "likely benign" in s:
        return "likely_benign"
    if re.search(r"\bbenign\b", s):
        return "benign"
    return None


def score_clinvar(clnsig, cfg):
    category = classify_clinvar(clnsig)
    if category is None:
        return 0.0
    return cfg["clinvar_scores"].get(category, 0.0)


def score_insilico(row, cfg):
    """Mean of the available, normalised in-silico predictors (0-1)."""
    components = cfg["insilico"]["components"]
    cadd_cap = cfg["insilico"]["cadd_phred_cap"]
    values = []
    for field, weight in components.items():
        raw = parse_float(row.get(field))
        if raw is None:
            continue
        if field == "vep_CADD_PHRED":
            norm = min(raw / cadd_cap, 1.0)
        else:  # REVEL / AlphaMissense already 0-1
            norm = min(max(raw, 0.0), 1.0)
        values.append(weight * norm)
    if not values:
        return 0.0
    return sum(values) / len(values)


def get_af(row, cfg):
    """Population AF: preferred field, falling back to the secondary field."""
    af = parse_float(row.get(cfg["rarity_af_field"]))
    if af is None:
        af = parse_float(row.get(cfg.get("rarity_fallback_af_field")))
    return af


def score_rarity(af, cfg):
    """Bin the allele frequency. Missing AF == novel (highest rarity)."""
    if af is None:
        af = 0.0
    for b in cfg["rarity_bins"]:
        if af <= b["max_af"]:
            return b["score"]
    return cfg["rarity_bins"][-1]["score"]


# ----------------------------
# Tier assignment
# ----------------------------
def assign_tier(row, cfg):
    t = cfg["tiers"]
    impact = str(row.get("vep_IMPACT", "")).upper()
    clinvar = classify_clinvar(row.get("vep_clinvar_clnsig"))
    af = get_af(row, cfg)
    af_val = af if af is not None else 0.0

    revel = parse_float(row.get("vep_REVEL"))
    cadd = parse_float(row.get("vep_CADD_PHRED"))
    am_pred = str(row.get("vep_AlphaMissense_pred", "")).strip().lower()

    # Benign ClinVar overrides everything -> lowest priority.
    if t.get("benign_demote", True) and clinvar in {"benign", "likely_benign"}:
        return 4

    # Tier 1: clinically pathogenic, or HIGH impact and rare.
    if clinvar in {"pathogenic", "likely_pathogenic"}:
        return 1
    if "HIGH" in impact and af_val <= t["tier1_high_impact_max_af"]:
        return 1

    # Tier 2: predicted-damaging (or HIGH impact) and not common.
    damaging = (
        (revel is not None and revel >= t["tier2_revel_min"])
        or (cadd is not None and cadd >= t["tier2_cadd_min"])
        or (am_pred == t["tier2_alphamissense_pred"])
        or ("HIGH" in impact)
    )
    if damaging and af_val <= t["tier2_max_af"]:
        return 2

    # Tier 3: moderate/high impact, not common.
    if ("HIGH" in impact or "MODERATE" in impact) and af_val <= t["tier3_max_af"]:
        return 3

    return 4


# ----------------------------
# Scoring driver
# ----------------------------
def compute_scores(df, cfg):
    """Return df with priority_* columns appended."""
    w = cfg["weights"]
    max_score = sum(w.values())

    impact_c, clinvar_c, insilico_c, rarity_c = [], [], [], []
    totals, tiers = [], []

    for _, row in df.iterrows():
        ic = w["impact"] * score_impact(row.get("vep_IMPACT"), cfg)
        cc = w["clinvar"] * score_clinvar(row.get("vep_clinvar_clnsig"), cfg)
        sc = w["insilico"] * score_insilico(row, cfg)
        rc = w["rarity"] * score_rarity(get_af(row, cfg), cfg)

        impact_c.append(round(ic, 2))
        clinvar_c.append(round(cc, 2))
        insilico_c.append(round(sc, 2))
        rarity_c.append(round(rc, 2))
        totals.append(round(ic + cc + sc + rc, 2))
        tiers.append(assign_tier(row, cfg))

    df = df.copy()
    df["priority_impact"] = impact_c
    df["priority_clinvar"] = clinvar_c
    df["priority_insilico"] = insilico_c
    df["priority_rarity"] = rarity_c
    df["priority_score"] = totals
    df["priority_tier"] = tiers

    logging.info(f"Max possible score: {max_score}")
    return df


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Compute variant priority score and tier from the s06 VEP-columns TSV"
    )
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-b", "--base-dir", "-i", dest="base_dir", required=True,
                        help="Base directory for file inputs and outputs (canonical: --base-dir/-b; -i kept for compatibility)")
    parser.add_argument("--config", required=True,
                        help="JSON config file with weights and thresholds")
    args = parser.parse_args()

    script_name = Path(sys.argv[0]).stem
    project = str(args.project)
    base_dir = Path(args.base_dir)
    input_dir = base_dir / project / "output"
    output_dir = base_dir / project / "output"
    config_file = Path(args.config)

    if not input_dir.is_dir():
        print(f"Error: Input directory '{input_dir}' does not exist.")
        sys.exit(1)

    if not config_file.is_file():
        sys.exit(f"Error: Config file '{config_file}' does not exist.")

    try:
        with open(config_file) as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        sys.exit(f"Error: Failed to parse JSON config file '{config_file}': {e}")

    # ----------------------------
    # Setup logging
    # ----------------------------
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()
    log_dir = output_dir / "logs"

    if not log_dir.is_dir():
        print(f"Log directory '{log_dir}' does not exist. Creating it...")
        log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"{script_name}_{project}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logging.info("# --- Compute priority score ---")
    logging.info(f"Project: {project}")
    logging.info(f"Timestamp: {timestamp}")
    logging.info(f"Config: {config_file}")
    logging.info(f"Input_dir: {input_dir}")
    logging.info(f"Output_dir: {output_dir}")
    logging.info("# Processing files")

    # ----------------------------
    # Load input TSV
    # ----------------------------
    in_tsv = input_dir / "s6_select_vep_cols.tsv"
    if not in_tsv.is_file():
        logging.error(f"Input TSV not found: {in_tsv}")
        sys.exit(1)

    # Read everything as string so VEP fields are parsed explicitly downstream.
    df = pd.read_csv(in_tsv, sep="\t", dtype=str, keep_default_na=False)
    logging.info(f"Loaded {len(df)} rows from {in_tsv.name}")

    # ----------------------------
    # Score and write
    # ----------------------------
    scored = compute_scores(df, cfg)
    # Highest priority first: tier ascending, then score descending.
    scored = scored.sort_values(
        by=["priority_tier", "priority_score"], ascending=[True, False]
    )

    out_tsv = output_dir / "s7_priority_score.tsv"
    scored.to_csv(out_tsv, sep="\t", index=False)

    # Quick tier breakdown for the log.
    tier_counts = scored["priority_tier"].value_counts().sort_index()
    for tier, count in tier_counts.items():
        logging.info(f"Tier {tier}: {count} rows")

    duration = time.time() - start_time
    logging.info(f"# Wrote {len(scored)} scored rows -> {out_tsv.name}")
    logging.info(f"# Runtime: {format_runtime(duration)}")
    logging.info("# --- End of run ---")

    print(f"Priority scoring complete. Output: {out_tsv}")
    print(f"Log written to {log_file}")


if __name__ == "__main__":
    main()
