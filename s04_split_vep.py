#!/usr/bin/env python3
"""

File Name    : s04_split_vep.py
Author       : Bryndis Yngvadottir
Created On   : 24/09/2025
Last Modified: 02/10/2025

Description: 
Split VEP annotations using bcftools +split-vep (bcftools version 1.10 or higher is required for +split-vep plugin to work)
Uses bcftools from a conda environment
Checks that bcftools version is suitable for +split-vep plugin 
STEPS:
# Step 1: split-vep
# Step 2: remove CSQ
# Step 3: select transcripts (see --transcript-pick)
# Step 4: index final VCF
bcftools +split-vep will include many duplicate lines if a variant has multiple consequences, leading to expanded file sizes.
Intermediate (temp) files from steps 1 and 2 are removed unless --keep-temps is selected.

Transcript selection (--transcript-pick):
# priority  (default): keep one record per variant, choosing MANE Select > canonical > first
#                      transcript. Never drops a variant.
# canonical          : keep only transcripts flagged CANONICAL=YES (drops variants that have no
#                      canonical transcript).
# all                : keep every transcript line (no selection).
# Note: 'priority' uses the vep_MANE_SELECT field. It must be present in the +split-vep columns
# config (s04_split_vep_columns.json). If your VEP run predates MANE, remove MANE_SELECT from the
# config; selection then falls back to canonical > first transcript automatically.

Notes to user:
# User will need to modify the accompanying configuration file (s04_split_vep_columns.json) to select VEP annotation fields required

Usage:
python s04_split_vep.py -p <project> -i <base_dir> -e <conda_env> --config <config_file> [--transcript-pick priority|canonical|all] [--keep-temp]

<project>: project name, used in file names and logs
<base_dir>: path to base directory (from this step onwards this serves as the input and output directory)
<conda_env>: name of conda environment with bcftools installed
<config_file>: configuration file with required vep annotation fields


Dependencies:
conda, bcftools (version 1.10 or higher for +split-vep plugin to work)

To do:
Consider removing the .vcf option as I am not convinced it is needed, files are generally compressed .vcf.gz

"""

import sys
import gzip
import time
import logging
from pathlib import Path
from datetime import datetime
import argparse

# Import shared utils
from utils import run_command, format_runtime, check_conda_env, get_bcftools_version, load_config, cleanup_temp_files


# ----------------------------
# Transcript selection (one record per variant)
# ----------------------------
def _parse_info(info_field):
    """Parse a VCF INFO column into a dict (flags map to '')."""
    d = {}
    for kv in info_field.split(";"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            d[k] = v
        else:
            d[k] = ""
    return d


def pick_transcripts_priority(in_vcf_gz, out_vcf_plain):
    """
    Reduce a `+split-vep --duplicate` VCF to one record per variant, choosing
    the transcript by priority: MANE Select > canonical > first transcript.

    +split-vep --duplicate emits all transcript/consequence lines for a record
    consecutively, so we group on consecutive identical CHROM/POS/REF/ALT keys
    and keep the best-ranked line (ties keep the first one seen). Variants with
    no MANE/canonical transcript still keep their first transcript, so nothing
    is silently dropped.

    If vep_MANE_SELECT is not present in the file (older VEP runs), the rank
    simply falls back to canonical > first.

    Returns the number of variants written.
    """
    def rank(info):
        mane = info.get("vep_MANE_SELECT", "")
        if mane and mane not in (".", ""):
            return 0
        if info.get("vep_CANONICAL", "") == "YES":
            return 1
        return 2

    written = 0
    cur_key = best_line = best_rank = None

    with gzip.open(in_vcf_gz, "rt") as fin, open(out_vcf_plain, "w") as fout:
        for line in fin:
            if line.startswith("#"):
                fout.write(line)
                continue
            cols = line.rstrip("\n").split("\t")
            key = (cols[0], cols[1], cols[3], cols[4])
            r = rank(_parse_info(cols[7]))

            if key != cur_key:
                if best_line is not None:
                    fout.write(best_line)
                    written += 1
                cur_key, best_line, best_rank = key, line, r
            elif r < best_rank:
                best_line, best_rank = line, r

        if best_line is not None:
            fout.write(best_line)
            written += 1

    return written


# ----------------------------
# Split VEP function and process
# ----------------------------
def split_vep_pipeline(in_vcf, out_vcf, conda_env, columns, output_dir, transcript_pick="priority", keep_temp=False):
    """
    Run bcftools +split-vep and postprocess:
      1. Split CSQ into separate annotations
      2. Drop redundant CSQ field
      3. Select transcripts (see --transcript-pick)
      4. Index final VCF

    transcript_pick:
      "priority"  -> one record per variant, MANE Select > canonical > first
                     (default; never drops a variant)
      "canonical" -> keep only transcripts flagged CANONICAL=YES
                     (drops variants with no canonical transcript)
      "all"       -> keep every transcript line (no selection)

    Returns: (success: bool, temp_files: list[Path])
    """
    tmpdir = output_dir / "tmp_splitvep"
    tmpdir.mkdir(parents=True, exist_ok=True)

    tmp_split = tmpdir / "splitvep_firstsplit.vcf.gz"
    tmp_no_csq = tmpdir / "splitvep_noCSQ.vcf.gz"
    tmp_picked = tmpdir / "splitvep_picked.vcf"
    temp_files = [tmp_split, tmp_no_csq, tmp_picked, tmpdir]

    # Step 1: split-vep
    # Note: +split-vep will produce huge files with variant lines duplicated to split up all different possible consequences
    cmd_split = [
        "conda", "run", "-n", conda_env,
        "bcftools", "+split-vep", str(in_vcf),
        "--duplicate",
        "--columns", columns,
        "--annot-prefix", "vep_",
        "--output", str(tmp_split),
        "--output-type", "z"
    ]
    if not run_command(cmd_split):
        return False, temp_files

    # Step 2: remove CSQ
    cmd_rmcsq = [
        "conda", "run", "-n", conda_env,
        "bcftools", "annotate",
        "--remove", "INFO/CSQ",
        "-Oz", "-o", str(tmp_no_csq), str(tmp_split)
    ]
    if not run_command(cmd_rmcsq):
        return False, temp_files

    # Step 3: select transcripts
    if transcript_pick == "priority":
        n = pick_transcripts_priority(tmp_no_csq, tmp_picked)
        logging.info(f"Selected {n} variants (MANE Select > canonical > first transcript)")
        # Re-compress the picked plain VCF so it is bgzipped and indexable
        cmd_select = [
            "conda", "run", "-n", conda_env,
            "bcftools", "view",
            "-Oz", "-o", str(out_vcf), str(tmp_picked)
        ]
    elif transcript_pick == "canonical":
        cmd_select = [
            "conda", "run", "-n", conda_env,
            "bcftools", "view",
            "--include", "vep_CANONICAL='YES'",
            "-Oz", "-o", str(out_vcf), str(tmp_no_csq)
        ]
    else:  # "all"
        cmd_select = [
            "conda", "run", "-n", conda_env,
            "bcftools", "view",
            "-Oz", "-o", str(out_vcf), str(tmp_no_csq)
        ]
    if not run_command(cmd_select):
        return False, temp_files

    # Step 4: index final VCF
    cmd_index = [
        "conda", "run", "-n", conda_env,
        "bcftools", "index", "-t", str(out_vcf)
    ]
    if not run_command(cmd_index):
        return False, temp_files

    if keep_temp:
        logging.info(f"Keeping temp files in {tmpdir}")
    return True, temp_files


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Split VEP annotations in VCFs")
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-b", "--base-dir", "-i", dest="base_dir", required=True,
                        help="Base output directory (canonical: --base-dir/-b; -i kept for compatibility)")
    parser.add_argument("-e", "--env", required=True, help="Conda environment with bcftools (version 1.10 or higher) installed")
    parser.add_argument("--config", required=True, help="JSON config file with selected VEP fields")
    parser.add_argument(
        "--transcript-pick",
        choices=["priority", "canonical", "all"],
        default="priority",
        help="Transcript selection: 'priority' = one per variant, MANE Select > canonical > "
             "first transcript (default, never drops a variant); 'canonical' = CANONICAL=YES only "
             "(drops variants with no canonical transcript); 'all' = keep every transcript line"
    )
    parser.add_argument("--keep-temp", action="store_true", help="Keep intermediate temp files for debugging")
    args = parser.parse_args()

    # Define command line argument as variables 
    script_name = Path(sys.argv[0]).stem
    project = str(args.project)
    base_dir = Path(args.base_dir)
    input_dir = base_dir / project / "output"
    output_dir = base_dir / project / "output"
    conda_env = str(args.env)
    config_file = str(args.config)

    if not input_dir.is_dir():
        print(f"Error: Input directory '{input_dir}' does not exist.")
        sys.exit(1)

    if not output_dir.is_dir():
        print(f"Creating output directory '{output_dir}'...")
        output_dir.mkdir(parents=True, exist_ok=True)

    if not check_conda_env(conda_env):
        print(f"Error: Conda environment '{conda_env}' does not exist.")
        sys.exit(1)

    # Ensure config file exists before loading
    config_path = Path(config_file)
    if not config_path.is_file():
        sys.exit(f"Error: Config file '{config_file}' does not exist.")

    # Load config (JSON only)
    columns = load_config(config_path)

    # If columns is a list, join into a string
    if isinstance(columns, list):
        columns = ",".join(columns)

    # ----------------------------
    # Setup logging
    # ----------------------------
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()
    bcftools_version = get_bcftools_version(conda_env)
    log_dir = output_dir / "logs"

    if not log_dir.is_dir():
        print(f"Log directory '{log_dir}' does not exist. Creating it...")
        log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"{script_name}_{project}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"), # append to log file 
            logging.StreamHandler(sys.stdout) # echo to console
        ]
    )


    logging.info("# --- Split VEP ---")
    logging.info(f"Project: {project}")
    logging.info(f"Timestamp: {timestamp}")
    logging.info(f"Conda_env: {conda_env}")
    logging.info(f"bcftools_version: {bcftools_version}")
    logging.info(f"Input_dir: {input_dir}")
    logging.info(f"Output_dir: {output_dir}")
    logging.info(f"Transcript_pick: {args.transcript_pick}")
    logging.info("# Processing files")

    # ----------------------------
    # Collect VCFs
    # ----------------------------
    # Collect only the filtered VCFs from s03, ignore index files
    vcfs = [
        f for f in input_dir.glob("*filter_vcf.vcf*")
    if not (f.name.endswith(".tbi") or f.name.endswith(".csi"))
    ]

    if not vcfs:
        logging.error("No filter_vcf VCF files found in input directory.")
        sys.exit(1)

    if len(vcfs) > 1:
        logging.error(
            f"Expected a single filter_vcf VCF but found {len(vcfs)}: "
            f"{[v.name for v in vcfs]}. Outputs use a fixed name and would be "
            "overwritten; please run one project/VCF at a time."
        )
        sys.exit(1)

    success_count = fail_count = 0
    total = len(vcfs)

    for in_vcf in vcfs:
        if in_vcf.name.endswith("filter_vcf.vcf.gz"):
            out_vcf = output_dir / "s4_split_vep.vcf.gz"
        elif in_vcf.name.endswith("filter_vcf.vcf"):
            out_vcf = output_dir / "s4_split_vep.vcf"
        else:
            logging.warning(f"Skipping unexpected file: {in_vcf}")
            continue

        success, temp_files = split_vep_pipeline(in_vcf, out_vcf, conda_env, columns, output_dir, args.transcript_pick, args.keep_temp)

        if success:
            logging.info(f"{in_vcf.name} -> {out_vcf.name}")
            success_count += 1
        else:
            logging.error(f"{in_vcf.name} -> {out_vcf.name}")
            fail_count += 1

        # cleanup or keep temp files
        cleanup_temp_files(temp_files, keep_temp=args.keep_temp)

    # ----------------------------
    # Finishing 
    # ----------------------------
    duration = time.time() - start_time
    logging.info(f"# Summary: {success_count} succeeded, {fail_count} failed, {total} total")
    logging.info(f"# Runtime: {format_runtime(duration)}")
    logging.info("# --- End of run ---")

    print(f"Split VEP complete. Log written to {log_file}")

if __name__ == "__main__":
    main()
