#!/usr/bin/env python3
"""

File Name    : s06_select_vep_cols.py
Author       : Bryndis Yngvadottir
Created On   : 01/10/2025
Last Modified: 04/11/2025

Description: 
Select desired VEP columns from annotated VCF and output as .txt 
Uses bcftools from a conda environment (user needs to create this locally, e.g. conda create -n env_bcftools -c bioconda -c conda-forge bcftools).

Note for user/User input required: 
# User will need to modify the accompanying configuration file (s06_select_vep_columns.json) to select VEP annotation fields required. 
# Note: The user can only select fields from the ones defined in the s04_split_vep_columns.json configuration file. If further columns are required, 
these need to have been added at Step4. 


Usage:
python s06_select_vep_cols.py -p <project> -i <base_dir> -e <conda_env> --config <config>

<project>: project name, used in file names and logs
<base_dir>: path to base directory
<conda_env>: name of conda environment with bcftools installed (optional) 
<config>: JSON config file with selected VEP columns


Dependencies: 
conda, bcftools

To do: 
Take out hardcoded file names 

"""

import sys
import time
import logging
from pathlib import Path
from datetime import datetime
import argparse

# Import shared utils
from utils import format_runtime, check_conda_env, get_bcftools_version


# ----------------------------
# Select annotatin columns of interest  
# ----------------------------


def select_columns(in_vcf, out_tsv, conda_env, config_file):
    """
    Extract selected fields from VCF using bcftools query.
    Fields are defined in a JSON config file.
    """
    import json, subprocess

    # Load fields from JSON config
    with open(config_file) as f:
        cfg = json.load(f)
    fields = cfg.get("fields", [])
    if not fields:
        logging.error("No fields defined in config file.")
        return False

    # Build query format string and header
    query_fmt = "[" + "\t".join(f"%{f}" for f in fields) + "\\n]"
    header = "\t".join(fields)

    # 1. Write header
    with open(out_tsv, "w") as out:
        out.write(header + "\n")

    # 2. Append bcftools output
    cmd = [
        "conda", "run", "-n", conda_env,
        "bcftools", "query",
        "-f", query_fmt,
        str(in_vcf)
    ]
    logging.info(f"CMD: {' '.join(cmd)}")
    result = subprocess.run(cmd, stdout=open(out_tsv, "a"), stderr=subprocess.PIPE, text=True)

    if result.returncode != 0:
        logging.error(f"bcftools query failed: {result.stderr}")
        return False

    return True

# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Filter VCFs on VEP IMPACT (keep MODERATE and HIGH)")
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-i", "--base-dir", required=True, help="Path to base directory (for file inputs and outputs)")
    parser.add_argument("-e", "--env", default="env_nf", help="Conda environment")
    parser.add_argument("--config", required=True, help="JSON config file with selected VEP columns")
    args = parser.parse_args()

    # Define command line input as variables 
    script_name = Path(sys.argv[0]).stem 
    project = str(args.project)
    base_dir = Path(args.base_dir)
    input_dir = base_dir / project / "output"
    output_dir = base_dir / project / "output"
    conda_env = str(args.env)
    config_file = str(args.config)

    if not base_dir.is_dir():
        print(f"Creating output directory '{base_dir}'...")
        base_dir.mkdir(parents=True, exist_ok=True)

    if not check_conda_env(conda_env):
        print(f"Error: Conda environment '{conda_env}' does not exist.")
        sys.exit(1)

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
            logging.FileHandler(log_file, mode="a"),
            logging.StreamHandler(sys.stdout)
        ]
    )


    logging.info("# --- Select VEP annotation columns ---")
    logging.info(f"Project: {project}")
    logging.info(f"Timestamp: {timestamp}")
    logging.info(f"Conda_env: {conda_env}")
    logging.info(f"bcftools_version: {bcftools_version}")
    logging.info(f"Input_dir: {input_dir}")
    logging.info(f"Output_dir: {output_dir}")
    logging.info("# Processing files")

    # ----------------------------
    # Collect VCFs 
    # ----------------------------
    # Collect only .PASS VCFs, ignore index files
    vcfs = [
        f for f in input_dir.glob("*filter_impact.vcf*")
    if not (f.name.endswith(".tbi") or f.name.endswith(".csi"))
    ]

    if not vcfs:
        logging.error("No filter_impact VCF files found in input directory.")
        sys.exit(1)

    success_count = fail_count = 0
    total = len(vcfs)

    for in_vcf in vcfs:
        if in_vcf.name.endswith("filter_impact.vcf.gz"):
            out_tsv = output_dir / "s6_select_vep_cols.tsv"
        elif in_vcf.name.endswith("filter_impact.vcf"):
            out_tsv = output_dir / "s6_select_vep_cols.tsv"
        else:
            logging.warning(f"Skipping unexpected file: {in_vcf}")
            continue
        
        success = select_columns(in_vcf, out_tsv, conda_env, config_file)

        if success:
            logging.info(f"{in_vcf.name} -> {out_tsv.name}")
            success_count += 1
        else:
            logging.error(f"{in_vcf.name} -> {out_tsv.name}")
            fail_count += 1


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
