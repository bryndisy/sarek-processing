#!/usr/bin/env python3
"""
File Name    : s02_run_sarek_full_germline.py
Author       : Bryndis Yngvadottir
Created On   : 22/09/2025
Last Modified: 02/10/2025, 22/10/2025

Description: 
Run full nf-core/sarek pipeline for germline data (from FASTQs to annotated VCFs) using a JSON configuration file for all paths, VEP plugins, and dbNSFP settings.

Usage:
python s02_run_sarek_full_germline.py \
  -p <project> \
  -i <input_file> \
  -o <base_dir> \
  -e <conda_env> \
  --config <config_file> 

<project>: project name, this will become the directory for the output and is used in file names and logs
<input_file>: path to fastq input file required for sarek (generated in the previous step)
<base_dir>: path to base directory (subdirectories will be created based on project name etc)
<config_file>: .json configuration file with reference databases, vep plugins etc
<conda_env>: name of conda environment with nextflow installed (optional)

Dependencies: 
conda, nextflow

Notes to user:
# Configuration file: 
# User will need to modify the configuration file (s02_run_sarek_full_germline.json) to set paths to reference databases, VEP plugins and columns needed for dbNSFP
# If the dbBSFP resource is used, please check whether a commercial license is required, if this is not used pleased modify the configuration file to exclude.

# VCFTOOLS_TSTV_COUNT crashes and pipeline fails: 
# For some of my datasets the sarek pipeline gets stuck at the VCFTOOLS_TSTV_COUNT step. 
# Assuming the VCF is fine, this is normally caused by a segmentation fault in vcftools version 0.1.16 (pretty old but default pulled by singularity in sarek), 
# happens with some joint-called VCFs with certain headers/FORMATs
# Solution: 
# 1) Use the disable_vcftools.config file to skip this step. 
# 2) If you need Ts/Tv numbers, compute them with bcftools stats outside Sarek 
#   bcftools stats joint_germline_recalibrated.vcf.gz | grep TSTV > joint_germline_recalibrated.tstv.txt
# 3) If you insist on keeping the step in-pipeline, try a different vcftools container for that process.
# Advice: Try running it without skipping it, but if it fails add the "-c", "disable_vcftools.config", to the main command. 

#To do: 
# Work out a fix for the VCFTOOLS_TSTV_COUNT step when it has issues
# Consider adding a removal of work directory if pipeline run is successfull


"""

import sys
import os
import time
from pathlib import Path
import argparse
import json
import tempfile
import logging

# Import shared utils
from utils import format_runtime, check_conda_env, run_command

# ----------------------------
# Helpers
# ----------------------------

def build_vep_custom_args(plugins: dict) -> str:
    """Construct the --vep_custom_args string for VEP dynamically from a dict of plugins and their file paths."""
    parts = ["--everything", "--total_length", "--offline", "--cache"]
    for name, path in plugins.items():
        if isinstance(path, (list, tuple)):
            parts.append(f"--plugin {name},{','.join(map(str, path))}")
        else:
            parts.append(f"--plugin {name},{path}")
    return " ".join(parts)

def build_nextflow_command(
    env_name: str | None,   # type: ignore
    input_file: Path,
    outdir: Path,
    config: dict,
    nextflow_cfg_path: str
) -> list[str]:
    """Build the Nextflow command for nf-core/sarek."""
    vep_args = build_vep_custom_args(config["vep_plugins"])

    base = []
    if env_name:
        base = ["conda", "run", "-n", env_name]

    cmd = base + [
        "nextflow", "run", "nf-core/sarek", "-r", "3.5.1", "-resume",
        "-profile", "singularity",
        "-c", nextflow_cfg_path,
        "-c", "disable_vcftools.config",
        "--input", str(input_file),
        "--outdir", str(outdir / "sarek_results"),
        "--genome", "GATK.GRCh38",
        "--step", "mapping",
        "--wes", "true",
        "--aligner", "bwa-mem",
        "--joint_germline", "true",
        "--vep_cache", config["vep_cache"],
        "--tools", "haplotypecaller,vep",
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
    return cmd

# ----------------------------
# Main
# ----------------------------

def main():
    parser = argparse.ArgumentParser(description="Run nf-core/sarek pipeline with JSON config")
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-i", "--input", required=True, help="Sarek FASTQ input CSV")
    parser.add_argument("-o", "--base-dir", required=True, help="Base output directory")
    parser.add_argument("--config", required=True, help="JSON config file with paths, VEP plugins, dbNSFP settings")
    parser.add_argument("-e", "--env", default=None, help="Conda environment name (optional, needed if Nextflow is installed in a conda env)")
    args = parser.parse_args()

    # Normalise paths and names
    script_name = Path(sys.argv[0]).stem
    project     = str(args.project)
    input_file  = Path(args.input).resolve()
    base_dir    = Path(args.base_dir).resolve()
    output_dir  = (base_dir / project / "output").resolve()

    # Prepare dirs early so logging can write somewhere
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir = output_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{script_name}_{project}.log"

    # Logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Load JSON config (fail loudly if malformed)
    try:
        with open(args.config) as f:
            config = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load JSON config '{args.config}': {e}")
        sys.exit(1)

    # Quick required keys check
    required_keys = [
        "vep_cache", "fasta", "fasta_fai", "dict",
        "dbnsfp", "dbnsfp_tbi", "dbnsfp_fields", "vep_plugins"
    ]
    missing = [k for k in required_keys if k not in config]
    if missing:
        logging.error(f"Missing keys in config: {', '.join(missing)}")
        sys.exit(1)

    # Validate input file exists
    if not input_file.is_file():
        logging.error(f"Input file not found: {input_file}")
        sys.exit(1)

    # Extra Nextflow diagnostics
    os.environ.setdefault("NXF_OPTS", "-Dnextflow.trace.stack=true")

    # If a conda env is specified, verify it exists (do not exit unless missing)
    conda_env = args.env
    if conda_env:
        if not check_conda_env(conda_env):
            logging.error(f"Conda environment '{conda_env}' does not exist.")
            sys.exit(1)

    # Temp Nextflow config (eg. longer Singularity pull timeout)
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
        tmp.write("singularity { pullTimeout = '60m' }")
        tmp.flush()
        nextflow_config_file = tmp.name

    # Build Nextflow command
    cmd = build_nextflow_command(
        env_name=conda_env,
        input_file=input_file,
        outdir=output_dir,
        config=config,
        nextflow_cfg_path=nextflow_config_file
    )

    logging.info("# --- Run full Sarek pipeline for germline data ---")
    logging.info(f"Project         : {project}")
    logging.info(f"Sarek input file: {input_file}")
    logging.info(f"Output dir      : {output_dir}")
    logging.info(f"Using conda env : {conda_env if conda_env else '(none)'}")
    logging.info(f"NXF_OPTS        : {os.environ.get('NXF_OPTS')}")

    start_time = time.time()

    ok = run_command(cmd)  # utils.run_command returns True/False
    if not ok:
        logging.error("Nextflow command failed.")
        # keep the tmp config for debugging
        logging.error(f"Left temporary Nextflow config at: {nextflow_config_file}")
        sys.exit(1)

    # Cleanup temp config on success
    try:
        os.unlink(nextflow_config_file)
    except Exception:
        pass

    duration = time.time() - start_time
    logging.info(f"# Runtime: {format_runtime(duration)}")
    logging.info("# --- End of run ---")
    print(f"Running Sarek completed. Log written to {log_file}")

if __name__ == "__main__":
    main()
