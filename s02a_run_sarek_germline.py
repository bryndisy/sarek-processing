#!/usr/bin/env python3
"""
File Name    : s02_run_sarek_germline.py
Author       : Bryndis Yngvadottir
Created On   : 23/06/2026
Last Modified: 23/06/2026

Description:
Run the full nf-core/sarek germline pipeline (from FASTQs to annotated VCFs) for
either whole-exome (WES) or whole-genome (WGS) data, selected with --mode.

This consolidates the former s02_run_sarek_full_germline.py (WES) and
s02_run_sarek_full_wgs_germline.py (WGS) into a single script so the two cannot
drift apart. Mode-specific Nextflow resource/process settings live in external
config files (wes.config / wgs.config) selected automatically by --mode.

Differences driven by --mode:
  wes : adds '--wes true'; lighter resource config (wes.config); supports
        --intervals to restrict calling/QC to the capture-kit target regions.
  wgs : whole-genome defaults (no --wes); heavier resource config (wgs.config)
        with per-process tuning and a dedicated workDir.

All reference/VEP/dbNSFP paths come from the JSON config (s02_vep_settings_plugins_paths.json).

Usage:
python s02_run_sarek_germline.py \
  -p <project> \
  -i <input_file> \
  -o <base_dir> \
  --config <config_file> \
  --mode {wes,wgs} \
  [--intervals <targets.bed>] \
  [-e <conda_env>]

<project>:     project name; becomes the output directory and is used in file names/logs
<input_file>:  sarek FASTQ input CSV (generated in the previous step)
<base_dir>:    base directory (subdirectories created from the project name)
<config_file>: JSON config with reference databases, VEP plugins, dbNSFP settings
<mode>:        wes or wgs
<intervals>:   (WES, strongly recommended) capture-kit target BED. Without it,
               Sarek calls genome-wide even in --wes mode, adding off-target
               noise, runtime, and misleading coverage QC. Optional for WGS.
<conda_env>:   conda environment with Nextflow installed (optional)

Dependencies:
conda, nextflow

Notes to user:
# Configuration file:
# Modify s02_vep_settings_plugins_paths.json to set paths to reference databases,
# VEP plugins and dbNSFP fields. If dbNSFP is used, check whether a commercial
# licence is required; if not used, edit the config to exclude it.
# Modify wes.config / wgs.config to match your host (workDir, cacheDir, CPUs).

# VCFTOOLS_TSTV_COUNT crashes and pipeline fails:
# For some datasets sarek gets stuck at VCFTOOLS_TSTV_COUNT, normally a
# segmentation fault in vcftools 0.1.16 (old, but the default pulled by sarek)
# on some joint-called VCFs with certain headers/FORMATs.
# Solution:
# 1) disable_vcftools.config (auto-included here if present) skips this step.
# 2) For Ts/Tv, compute with bcftools stats outside Sarek:
#      bcftools stats joint_germline_recalibrated.vcf.gz | grep TSTV > out.tstv.txt

#To do:
# Consider removing the work directory automatically on a successful run.

"""

import sys
import os
import time
from datetime import datetime
from pathlib import Path
import argparse
import json
import tempfile
import logging

# Import shared utils
from utils import format_runtime, check_conda_env, run_command


# ----------------------------
# Helpers: Build commands and arguments
# ----------------------------
def build_vep_custom_args(plugins: dict) -> str:
    """
    Build --vep_custom_args from a dict like:
      {
        "REVEL": "/path/revel.tsv.gz",
        "CADD": ["/path/snv.tsv.gz", "/path/indel.tsv.gz"]
      }
    """
    parts = ["--everything", "--total_length", "--offline", "--cache"]
    for name, path in plugins.items():
        if isinstance(path, (list, tuple)):
            parts.append(f"--plugin {name},{','.join(map(str, path))}")
        else:
            parts.append(f"--plugin {name},{path}")
    return " ".join(parts)


def build_nextflow_command(
    env_name: str | None,
    mode: str,
    input_file: Path,
    outdir: Path,
    config: dict,
    mode_cfg_path: Path,
    extra_config_path: str | None,
    intervals: Path | None,
) -> list[str]:
    """Build the Nextflow command for nf-core/sarek (WES or WGS germline)."""
    vep_args = build_vep_custom_args(config["vep_plugins"])

    prefix = ["conda", "run", "-n", env_name] if env_name else []

    cmd = prefix + [
        "nextflow", "run", "nf-core/sarek", "-r", "3.5.1", "-resume",
        "-profile", "singularity",
    ]

    # Mode-specific resource/process config (wes.config or wgs.config)
    cmd += ["-c", str(mode_cfg_path)]

    # Optional config to skip the flaky VCFTOOLS_TSTV_COUNT step, if present
    if extra_config_path:
        cmd += ["-c", extra_config_path]

    # Shared pipeline args
    cmd += [
        "--input", str(input_file),
        "--outdir", str(outdir / "sarek_results"),
        "--genome", "GATK.GRCh38",
        "--step", "mapping",
        "--aligner", "bwa-mem",
        "--joint_germline", "true",
        "--tools", "haplotypecaller,vep",
        "--vep_cache", config["vep_cache"],
        "--vep_include_fasta", "true",
        "--fasta", config["fasta"],
        "--fasta_fai", config["fasta_fai"],
        "--dict", config["dict"],
        "--vep_custom_args", vep_args,
        "--vep_dbnsfp", "true",
        "--dbnsfp", config["dbnsfp"],
        "--dbnsfp_tbi", config["dbnsfp_tbi"],
        "--dbnsfp_fields", ",".join(config["dbnsfp_fields"]),
    ]

    # Mode-specific CLI flags
    if mode == "wes":
        cmd += ["--wes", "true"]

    # Target intervals: restricts calling/QC to the given BED (WES capture kit).
    if intervals:
        cmd += ["--intervals", str(intervals)]

    return cmd


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Run nf-core/sarek germline pipeline (WES or WGS) with JSON config")
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-s", "--samplesheet", "-i", "--input", dest="input", required=True,
                        help="Sarek input samplesheet CSV with FASTQs (canonical: --samplesheet/-s; -i/--input kept for compatibility)")
    parser.add_argument("-b", "--base-dir", "-o", dest="base_dir", required=True,
                        help="Base output directory (canonical: --base-dir/-b; -o kept for compatibility)")
    parser.add_argument("--config", required=True, help="JSON config with paths, VEP plugins, dbNSFP settings")
    parser.add_argument("--mode", required=True, choices=["wes", "wgs"], help="Sequencing type: wes or wgs")
    parser.add_argument("--intervals", default=None,
                        help="Target BED to restrict calling/QC (WES capture kit; strongly recommended for WES)")
    parser.add_argument("-e", "--env", default=None, help="Conda environment name containing Nextflow (optional)")
    args = parser.parse_args()

    # Normalise paths and names
    script_name = Path(sys.argv[0]).stem
    script_dir  = Path(__file__).resolve().parent
    project     = args.project
    mode        = args.mode
    input_file  = Path(args.input).resolve()
    base_dir    = Path(args.base_dir).resolve()
    output_dir  = (base_dir / project / "output").resolve()

    # Prepare directories
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir = output_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{script_name}_{project}.log"

    # Logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file, mode="a"), logging.StreamHandler(sys.stdout)],
    )

    # Better Nextflow diagnostics unless already set
    os.environ.setdefault("NXF_OPTS", "-Dnextflow.trace.stack=true")

    # Mode-specific resource config (wes.config / wgs.config), alongside this script
    mode_cfg_path = script_dir / f"{mode}.config"
    if not mode_cfg_path.is_file():
        logging.error(f"Mode config not found: {mode_cfg_path}")
        sys.exit(1)

    # Load JSON config for VEP/references
    try:
        with open(args.config) as f:
            config = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load JSON config '{args.config}': {e}")
        sys.exit(1)

    # Required keys check
    required = ["vep_cache", "fasta", "fasta_fai", "dict", "dbnsfp", "dbnsfp_tbi", "dbnsfp_fields", "vep_plugins"]
    missing = [k for k in required if k not in config]
    if missing:
        logging.error(f"Missing keys in config: {', '.join(missing)}")
        sys.exit(1)

    # Validate input file exists
    if not input_file.is_file():
        logging.error(f"Input file not found: {input_file}")
        sys.exit(1)

    # Validate / advise on intervals
    intervals = None
    if args.intervals:
        intervals = Path(args.intervals).resolve()
        if not intervals.is_file():
            logging.error(f"Intervals BED not found: {intervals}")
            sys.exit(1)
    elif mode == "wes":
        logging.warning(
            "No --intervals provided for a WES run: Sarek will use genome-wide "
            "default intervals, adding off-target calls, runtime and misleading "
            "coverage QC. Pass your capture-kit target BED via --intervals."
        )

    # If a conda env is specified, verify it exists
    conda_env = args.env
    if conda_env and not check_conda_env(conda_env):
        logging.error(f"Conda environment '{conda_env}' does not exist.")
        sys.exit(1)

    # Optional extra config (skip flaky vcftools step); include only if present
    extra_config = script_dir / "disable_vcftools.config"
    extra_config_path = str(extra_config) if extra_config.is_file() else None
    if extra_config_path is None:
        logging.warning(f"Optional config not found (continuing without it): {extra_config}")

    logging.info("# --- Run full sarek germline pipeline ---")
    logging.info(f"Project           : {project}")
    logging.info(f"Mode              : {mode}")
    logging.info(f"Timestamp         : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Sarek input file  : {input_file}")
    logging.info(f"Output dir        : {output_dir}")
    logging.info(f"Mode config       : {mode_cfg_path}")
    logging.info(f"Intervals BED     : {intervals if intervals else '(none)'}")
    logging.info(f"Using conda env   : {conda_env if conda_env else '(none)'}")
    logging.info(f"NXF_OPTS          : {os.environ.get('NXF_OPTS')}")

    start = time.time()

    cmd = build_nextflow_command(
        env_name=conda_env,
        mode=mode,
        input_file=input_file,
        outdir=output_dir,
        config=config,
        mode_cfg_path=mode_cfg_path,
        extra_config_path=extra_config_path,
        intervals=intervals,
    )

    logging.info("Nextflow command:\n" + " ".join(cmd))

    ok = run_command(cmd)  # expects True/False
    if not ok:
        logging.error("Nextflow command failed.")
        sys.exit(1)

    logging.info(f"# Runtime: {format_runtime(time.time() - start)}")
    logging.info("# --- End of run ---")
    print(f"Running sarek {mode} pipeline completed. Log written to {log_file}")


if __name__ == "__main__":
    main()
