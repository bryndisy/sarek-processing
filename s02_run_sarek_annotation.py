#!/usr/bin/env python3
"""
File Name    : s02_run_sarek_annotation.py
Author       : Bryndis Yngvadottir
Created On   : 04/11/2025
Last Modified: 

Description: 
Run nf-core/sarek pipeline from annotation step using a JSON configuration file (s02_vep_settings_plugins_paths.json) for all paths, VEP plugins, and dbNSFP settings.

Usage:
python s02_run_sarek_annotation.py \
  -p <project> \
  -i <input_file> \
  -o <base_dir> \
  -e <conda_env> \
  --config <config_file> 

<project>: project name, this will become the directory for the output and is used in file names and logs
<input_file>: path to the sarek annotation samplesheet (VCFs to annotate)
<base_dir>: path to base directory (subdirectories will be created based on project name etc)
<config_file>: .json configuration file with reference databases, vep plugins etc
<conda_env>: name of conda environment with nextflow installed (optional)

Dependencies: 
conda, nextflow

Notes to user:
# Configuration file: 
# User will need to modify the configuration file (s02_vep_settings_plugins_paths.json) to set paths to reference databases, VEP plugins and columns needed for dbNSFP
# If the dbBSFP resource is used, please check whether a commercial license is required, if this is not used pleased modify the configuration file to exclude.

"""

import sys
import os
import time
from datetime import datetime
from pathlib import Path
import argparse
import json
import logging

# Import shared utils
from utils import format_runtime, check_conda_env, run_command

# ----------------------------
# Helpers: Build commands and arguments 
# ----------------------------

def build_vep_custom_args(plugins: dict) -> str:
    parts = ["--everything", "--total_length", "--offline", "--cache"]
    for name, path in plugins.items():
        if isinstance(path, (list, tuple)):
            parts.append(f"--plugin {name},{','.join(map(str, path))}")
        else:
            parts.append(f"--plugin {name},{path}")
    return " ".join(parts)

def build_nextflow_command(env_name, input_file: Path, outdir: Path, config: dict, nextflow_config_file: str) -> list[str]:
    vep_args = build_vep_custom_args(config["vep_plugins"])
    prefix = ["conda", "run", "-n", env_name] if env_name else []
    return prefix + [
        "nextflow", "run", "nf-core/sarek", "-r", "3.5.1", "-resume",
        "-profile", "singularity",
        "-c", nextflow_config_file,
        "--input", str(input_file),
        "--outdir", str(outdir / "sarek_results"),
        "--genome", "GATK.GRCh38",
        "--step", "annotate",
        "--vep_cache", config["vep_cache"],
        "--tools", "vep",
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


# ----------------------------
# Main
# ----------------------------

def main():
    parser = argparse.ArgumentParser(description="Run nf-core/sarek pipeline with JSON config")
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-s", "--samplesheet", "-i", "--input", dest="input", required=True,
                        help="Sarek annotation samplesheet CSV (VCFs) (canonical: --samplesheet/-s; -i/--input kept for compatibility)")
    parser.add_argument("-b", "--base-dir", "-o", dest="base_dir", required=True,
                        help="Base output directory (canonical: --base-dir/-b; -o kept for compatibility)")
    parser.add_argument("--config", required=True, help="JSON config with paths, VEP plugins, dbNSFP settings")
    parser.add_argument("-e", "--env", default=None, help="Conda environment name containing Nextflow (optional)")
    args = parser.parse_args()

    # Normalise paths and names
    script_name = Path(sys.argv[0]).stem
    script_dir  = Path(__file__).resolve().parent
    project     = args.project
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

    # Load JSON config for VEP
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

    # If a conda env is specified, verify it exists (do not exit unless missing)
    conda_env = args.env
    if conda_env and not check_conda_env(conda_env):
        logging.error(f"Conda environment '{conda_env}' does not exist.")
        sys.exit(1)

    # Nextflow resource/process config (pull timeout + VEP resources)
    nextflow_config_file = script_dir / "annotation.config"
    if not nextflow_config_file.is_file():
        logging.error(f"Nextflow config not found: {nextflow_config_file}")
        sys.exit(1)

    logging.info("# --- Run Sarek pipeline from annotation step ---")
    logging.info(f"Project         : {project}")
    logging.info(f"Sarek input file: {input_file}")
    logging.info(f"Output dir      : {output_dir}")
    logging.info(f"Using conda env : {conda_env if conda_env else '(none)'}")
    logging.info(f"VEP config      : {config}")
    logging.info(f"Nextflow config : {nextflow_config_file}")
    logging.info(f"NXF_OPTS        : {os.environ.get('NXF_OPTS')}")

    start = time.time()
    cmd = build_nextflow_command(conda_env, input_file, output_dir, config, str(nextflow_config_file))

    ok = run_command(cmd)  # expects True/False
    if not ok:
        logging.error("Nextflow command failed.")
        sys.exit(1)

    logging.info(f"# Runtime: {format_runtime(time.time() - start)}")
    logging.info("# --- End of run ---")
    print(f"Running Sarek annotation completed. Log written to {log_file}")

if __name__ == "__main__":
    main()