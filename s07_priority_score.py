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
  3. In silico   - pathogenicity predictors (REVEL, CADD_PHRED, AlphaMissense,
                   MetaRNN, ClinPred), lifted by the strongest SpliceAI delta
                   score so splice variants the missense predictors miss still score
  4. Rarity      - population allele frequency (gnomAD POPMAX, MAX_AF fallback)

Each component is normalised to 0-1, multiplied by its weight (weights sum to
the max score), and the four contributions are summed. The per-component
contributions are written out alongside the total so the score is auditable.

A discrete tier (1 = highest priority, 4 = lowest) is also derived, combining
ClinVar status, IMPACT, allele frequency and additional signals: strong SpliceAI
(splice) and BayesDel (damaging) promote a variant, while a HIGH-impact loss-of-
function that ALoFT confidently predicts "Tolerant" is demoted out of the top tiers.
All of these signals degrade gracefully - if a field is absent (e.g. not a LoF
variant), it simply has no effect. Tiers are coarse filters; the numeric score
ranks finely within them.

All weights and thresholds live in the accompanying config file
(s07_priority_score_config.json) so the scheme can be tuned without editing code.

This step is pure Python/pandas and does NOT use bcftools or `conda run`. It does
need pandas, so run it from an environment that has pandas installed (e.g. the
pipeline's env_sarek): either activate the env first, or invoke it via
`conda run -n env_sarek python s07_priority_score.py ...`.

Usage:
python s07_priority_score.py -p <project> -i <base_dir> --config <config_file>

<project>:     project name, used in file names and logs
<base_dir>:    path to base directory (same layout as previous steps)
<config_file>: JSON config with weights and thresholds

Input : <base_dir>/<project>/output/s6_select_vep_cols.tsv
Output: <base_dir>/<project>/output/s7_priority_score.tsv
        <base_dir>/<project>/output/s7_priority_score.xlsx

Dependencies:
pandas, openpyxl (for the .xlsx output)

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


def pred_tokens(value):
    """Split a categorical dbNSFP field into a set of non-missing, upper-cased tokens.

    dbNSFP packs one value per transcript/isoform into a single field, '&'-joined
    (its native ';' is illegal in a VCF INFO field, so VEP remaps it), with '.' for
    isoforms it has no call for - e.g. 'LP&.&LP&P'. Matching a category therefore
    means testing membership in the token set, not string-equality against the cell.
    """
    if value is None:
        return set()
    toks = set()
    for part in re.split(r"[&,;|]", str(value)):
        p = part.strip()
        if p and p.lower() not in _MISSING:
            toks.add(p.upper())
    return toks


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


def _best_spliceai(row, cfg):
    """Strongest SpliceAI delta score (0-1) across the configured DS fields, or 0."""
    best = 0.0
    for f in cfg.get("insilico", {}).get("splice_fields", []):
        v = parse_float(row.get(f))
        if v is not None:
            best = max(best, min(max(v, 0.0), 1.0))
    return best


def score_insilico(row, cfg):
    """In-silico component (0-1).

    Mean of the available, normalised missense predictors, lifted by the strongest
    SpliceAI delta score via max() so a splice-affecting variant (which the
    missense predictors typically miss) still scores, without diluting a strong
    missense signal. Missing predictors are simply skipped; if none are present
    the component is 0 (or the SpliceAI value, if any).
    """
    components = cfg["insilico"]["components"]
    cadd_cap = cfg["insilico"]["cadd_phred_cap"]
    values = []
    for field, weight in components.items():
        raw = parse_float(row.get(field))
        if raw is None:
            continue
        if field == "vep_CADD_PHRED":
            norm = min(raw / cadd_cap, 1.0)
        else:  # REVEL / AlphaMissense / MetaRNN / ClinPred already 0-1
            norm = min(max(raw, 0.0), 1.0)
        values.append(weight * norm)
    mean_missense = sum(values) / len(values) if values else 0.0

    return max(mean_missense, _best_spliceai(row, cfg))


def get_af(row, cfg):
    """Population AF: the preferred field, then each fallback field in order.

    rarity_fallback_af_field may be a single field name or a list, letting the
    rarity signal degrade gracefully across sources - e.g. gnomAD v4 (all variant
    classes) -> dbNSFP gnomAD4.1 (coding SNVs) -> VEP MAX_AF (cache).
    """
    fields = [cfg["rarity_af_field"]]
    fb = cfg.get("rarity_fallback_af_field")
    if isinstance(fb, (list, tuple)):
        fields += list(fb)
    elif fb:
        fields.append(fb)
    for f in fields:
        af = parse_float(row.get(f))
        if af is not None:
            return af
    return None


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
    spliceai = _best_spliceai(row, cfg)

    # dbNSFP categorical predictors are '&'-joined across transcripts/isoforms (with
    # '.' for isoforms with no call), so match on the SET of non-missing tokens
    # rather than string-equality against the whole cell. AlphaMissense/BayesDel use
    # short codes (e.g. LP/P, D), configured via the tier2_* keys.
    am_tokens = pred_tokens(row.get("vep_AlphaMissense_pred"))
    bayesdel_tokens = pred_tokens(row.get("vep_BayesDel_addAF_pred"))

    am_targets = t.get("tier2_alphamissense_pred", [])
    if isinstance(am_targets, str):
        am_targets = [am_targets]
    am_targets = {str(x).upper() for x in am_targets}
    am_hit = bool(am_tokens & am_targets)

    bayesdel_target = t.get("tier2_bayesdel_pred")
    bayesdel_hit = bool(bayesdel_target) and str(bayesdel_target).upper() in bayesdel_tokens

    # ALoFT loss-of-function check: for a LoF variant, dbNSFP's ALoFT predicts
    # Tolerant / Recessive / Dominant (with a High/Low confidence). A HIGH-impact LoF
    # that ALoFT confidently calls Tolerant - with no conflicting Recessive/Dominant
    # isoform - is unlikely to be a true damaging LoF, so it should not earn the
    # HIGH-impact boost into Tiers 1-2; it still reaches Tier 3 (raw impact) for
    # review rather than being dropped. Non-LoF variants (ALoFT empty) are untouched.
    # (This covers the LoF-quality role while LOFTEE is not enabled.)
    aloft_tokens = pred_tokens(row.get("vep_Aloft_pred"))
    aloft_conf_tokens = pred_tokens(row.get("vep_Aloft_Confidence"))
    aloft_tolerant = "TOLERANT" in aloft_tokens and not (aloft_tokens & {"RECESSIVE", "DOMINANT"})
    high_impact = ("HIGH" in impact)
    if (t.get("aloft_tolerant_demote", True)
            and aloft_tolerant
            and "HIGH" in aloft_conf_tokens):
        high_impact = False

    # Sentinel thresholds so a missing config key simply disables that rule.
    tier1_splice = t.get("tier1_spliceai_min", 1.1)
    tier2_splice = t.get("tier2_spliceai_min", 1.1)

    # Benign ClinVar overrides everything -> lowest priority.
    if t.get("benign_demote", True) and clinvar in {"benign", "likely_benign"}:
        return 4

    # Tier 1: clinically pathogenic; HIGH impact and rare; or strong splice and rare.
    if clinvar in {"pathogenic", "likely_pathogenic"}:
        return 1
    if high_impact and af_val <= t["tier1_high_impact_max_af"]:
        return 1
    if spliceai >= tier1_splice and af_val <= t["tier1_high_impact_max_af"]:
        return 1

    # Tier 2: predicted-damaging (or HIGH impact) and not common.
    damaging = (
        (revel is not None and revel >= t["tier2_revel_min"])
        or (cadd is not None and cadd >= t["tier2_cadd_min"])
        or am_hit
        or bayesdel_hit
        or (spliceai >= tier2_splice)
        or high_impact
    )
    if damaging and af_val <= t["tier2_max_af"]:
        return 2

    # Tier 3: moderate/high impact, not common. Uses the raw impact so a HIGH-impact
    # LoF demoted from Tiers 1-2 (e.g. ALoFT Tolerant) is still surfaced here rather
    # than dropped to Tier 4.
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
    logging.info(f"# Wrote {len(scored)} scored rows -> {out_tsv.name}")

    # Also write an Excel workbook (a convenient final deliverable). Needs the
    # openpyxl engine; if it is missing, keep the TSV and warn rather than fail.
    out_xlsx = output_dir / "s7_priority_score.xlsx"
    try:
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            scored.to_excel(writer, sheet_name="priority_score", index=False)
            # Freeze the header row so it stays visible while scrolling.
            writer.sheets["priority_score"].freeze_panes = "A2"
        logging.info(f"# Wrote Excel workbook -> {out_xlsx.name}")
    except ImportError:
        out_xlsx = None
        logging.error(
            "Could not write Excel output: the 'openpyxl' engine is not installed. "
            "Install it (e.g. conda install -n env_sarek -c conda-forge openpyxl) and "
            "re-run. The TSV output was still written."
        )

    # Quick tier breakdown for the log.
    tier_counts = scored["priority_tier"].value_counts().sort_index()
    for tier, count in tier_counts.items():
        logging.info(f"Tier {tier}: {count} rows")

    duration = time.time() - start_time
    logging.info(f"# Runtime: {format_runtime(duration)}")
    logging.info("# --- End of run ---")

    print(f"Priority scoring complete. TSV: {out_tsv}")
    if out_xlsx is not None:
        print(f"                          Excel: {out_xlsx}")
    print(f"Log written to {log_file}")


if __name__ == "__main__":
    main()
