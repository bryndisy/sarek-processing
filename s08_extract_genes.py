#!/usr/bin/env python3
"""

File Name    : s08_extract_genes.py
Author       : Bryndis Yngvadottir
Created On   : 24/06/2026
Last Modified: 24/06/2026

Description:
Extract variants in a user-supplied set of genes from the prioritised table
produced by s07_priority_score.py.

Reads s7_priority_score.tsv, keeps only the rows whose gene symbol matches one of
the requested genes, and writes the subset out as TSV and Excel. Row order
(priority tier ascending, score descending) is preserved from Step 7, so the
highest-priority variants in the genes of interest are at the top.

Genes can be given on the command line (--genes, comma-separated) and/or in a file
(--gene-file, one symbol per line; blank lines and lines starting with '#' are
ignored). Both sources are combined. Matching is case-insensitive and tolerant of
'&'-joined multi-gene cells (a row matches if any of its symbols is requested).

The gene column defaults to 'vep_SYMBOL' (as exported by Step 6); override with
--gene-column if needed.

This step is pure Python/pandas and does NOT use bcftools or `conda run`. It needs
pandas (and openpyxl for the .xlsx), so run it from an environment that has them
(e.g. the pipeline's env_sarek): either activate the env first, or invoke it via
`conda run -n env_sarek python s08_extract_genes.py ...`.

Usage:
python s08_extract_genes.py -p <project> -i <base_dir> (--genes G1,G2 | --gene-file genes.txt) [--gene-column vep_SYMBOL]

<project>:     project name, used in file names and logs
<base_dir>:    path to base directory (same layout as previous steps)

Input : <base_dir>/<project>/output/s7_priority_score.tsv
Output: <base_dir>/<project>/output/s8_gene_extract.tsv
        <base_dir>/<project>/output/s8_gene_extract.xlsx

Dependencies:
pandas, openpyxl (for the .xlsx output)

"""

import sys
import re
import time
import logging
from pathlib import Path
from datetime import datetime
import argparse

import pandas as pd

# Import shared utils
from utils import format_runtime


# ----------------------------
# Gene-list handling
# ----------------------------
def load_genes(genes_arg, gene_file_arg):
    """Build the set of requested gene symbols (upper-cased) from both sources.

    Returns (requested_set, ordered_list) where ordered_list preserves the order
    the genes were given (CLI first, then file) for nicer logging.
    """
    ordered = []
    seen = set()

    def add(symbol):
        s = symbol.strip().upper()
        if s and s not in seen:
            seen.add(s)
            ordered.append(s)

    if genes_arg:
        for g in genes_arg.split(","):
            add(g)

    if gene_file_arg:
        gene_file = Path(gene_file_arg)
        if not gene_file.is_file():
            sys.exit(f"Error: Gene file '{gene_file}' does not exist.")
        with open(gene_file) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                add(line)

    return seen, ordered


def split_symbols(cell):
    """Split a gene-symbol cell into upper-cased tokens (handles '&'-joined cells)."""
    if cell is None:
        return []
    return [tok.strip().upper() for tok in re.split(r"[&,;|]", str(cell)) if tok.strip()]


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Extract variants in specific genes from the s07 prioritised table"
    )
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-b", "--base-dir", "-i", dest="base_dir", required=True,
                        help="Base directory for file inputs and outputs (canonical: --base-dir/-b; -i kept for compatibility)")
    parser.add_argument("--genes", default=None,
                        help="Comma-separated list of gene symbols to keep (e.g. BRCA1,BRCA2,TP53)")
    parser.add_argument("--gene-file", default=None,
                        help="File with one gene symbol per line ('#' comments and blank lines ignored)")
    parser.add_argument("--gene-column", default="vep_SYMBOL",
                        help="Column holding the gene symbol (default: vep_SYMBOL)")
    args = parser.parse_args()

    if not args.genes and not args.gene_file:
        parser.error("provide at least one of --genes or --gene-file")

    script_name = Path(sys.argv[0]).stem
    project = str(args.project)
    base_dir = Path(args.base_dir)
    input_dir = base_dir / project / "output"
    output_dir = base_dir / project / "output"

    if not input_dir.is_dir():
        print(f"Error: Input directory '{input_dir}' does not exist.")
        sys.exit(1)

    requested, requested_ordered = load_genes(args.genes, args.gene_file)
    if not requested:
        sys.exit("Error: No gene symbols were provided.")

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

    logging.info("# --- Extract variants in specific genes ---")
    logging.info(f"Project: {project}")
    logging.info(f"Timestamp: {timestamp}")
    logging.info(f"Input_dir: {input_dir}")
    logging.info(f"Output_dir: {output_dir}")
    logging.info(f"Gene column: {args.gene_column}")
    logging.info(f"Requested {len(requested_ordered)} gene(s): {', '.join(requested_ordered)}")
    logging.info("# Processing files")

    # ----------------------------
    # Load input TSV
    # ----------------------------
    in_tsv = input_dir / "s7_priority_score.tsv"
    if not in_tsv.is_file():
        logging.error(f"Input TSV not found: {in_tsv}")
        sys.exit(1)

    df = pd.read_csv(in_tsv, sep="\t", dtype=str, keep_default_na=False)
    logging.info(f"Loaded {len(df)} rows from {in_tsv.name}")

    if args.gene_column not in df.columns:
        logging.error(
            f"Gene column '{args.gene_column}' not found in {in_tsv.name}. "
            f"Available columns: {', '.join(df.columns)}"
        )
        sys.exit(1)

    # ----------------------------
    # Filter to requested genes
    # ----------------------------
    mask = df[args.gene_column].apply(
        lambda cell: any(tok in requested for tok in split_symbols(cell))
    )
    subset = df[mask].copy()

    # Report which requested genes were / were not found.
    found = set()
    for cell in subset[args.gene_column]:
        found.update(tok for tok in split_symbols(cell) if tok in requested)
    missing = [g for g in requested_ordered if g not in found]

    logging.info(f"Matched {len(subset)} rows across {len(found)} of {len(requested)} requested gene(s)")
    if missing:
        logging.warning(f"No variants found for {len(missing)} gene(s): {', '.join(missing)}")

    # ----------------------------
    # Write outputs (TSV + Excel)
    # ----------------------------
    out_tsv = output_dir / "s8_gene_extract.tsv"
    subset.to_csv(out_tsv, sep="\t", index=False)
    logging.info(f"# Wrote {len(subset)} rows -> {out_tsv.name}")

    out_xlsx = output_dir / "s8_gene_extract.xlsx"
    try:
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            subset.to_excel(writer, sheet_name="gene_extract", index=False)
            writer.sheets["gene_extract"].freeze_panes = "A2"
        logging.info(f"# Wrote Excel workbook -> {out_xlsx.name}")
    except ImportError:
        out_xlsx = None
        logging.error(
            "Could not write Excel output: the 'openpyxl' engine is not installed. "
            "Install it (e.g. conda install -n env_sarek -c conda-forge openpyxl) and "
            "re-run. The TSV output was still written."
        )

    duration = time.time() - start_time
    logging.info(f"# Runtime: {format_runtime(duration)}")
    logging.info("# --- End of run ---")

    print(f"Gene extraction complete. TSV: {out_tsv}")
    if out_xlsx is not None:
        print(f"                        Excel: {out_xlsx}")
    print(f"Log written to {log_file}")


if __name__ == "__main__":
    main()
