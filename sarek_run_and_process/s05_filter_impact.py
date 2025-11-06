#!/usr/bin/env python3
"""

File Name    : s04_filter_impact.py
Author       : Bryndis Yngvadottir
Created On   : 30/09/2025
Last Modified: 01/10/2025, 04/11/2025

Description: 
Filter VCF for HIGH and MODERATE impact variants. 
Uses bcftools from a conda environment (user needs to create this locally, e.g. conda create -n env_bcftools -c bioconda -c conda-forge bcftools).

Usage:
python s04_filter_impact.py <project> <base_dir> <conda_env> 

<project>: project name, used in file names and logs
<base_dir>: path to base directory
<conda_env>: name of conda environment with bcftools installed (optional) 

Dependencies: 
conda, bcftools

"""

import sys
import subprocess
import time
import logging
from pathlib import Path
from datetime import datetime
import argparse

# Import shared utils
from utils import format_runtime, check_conda_env, get_bcftools_version


# ----------------------------
# Filter VCF in impact 
# ----------------------------
def filter_impact(in_vcf, out_vcf, conda_env):
    cmd = [
        "conda", "run", "-n", conda_env,
        "bcftools", "view", str(in_vcf),
        "--include", "vep_IMPACT='MODERATE' || vep_IMPACT='HIGH'",
        "--output", str(out_vcf),
        "--output-type", "z"
    ]
    logging.info(f"CMD: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        if out_vcf.suffix == ".gz":
            index_cmd = [
                "conda", "run", "-n", conda_env,
                "bcftools", "index", "-t", str(out_vcf)
            ]
            logging.info(f"CMD: {' '.join(index_cmd)}")
            subprocess.run(index_cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"bcftools failed on {in_vcf}: {e}")
        return False

# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Filter VCFs on VEP IMPACT (keep MODERATE and HIGH)")
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-i", "--base-dir", required=True, help="Path to base directory (for file inputs and outputs)")
    parser.add_argument("-e", "--env", default="env_nf", help="Conda environment")
    args = parser.parse_args()

    # Define command line input as variables
    script_name = Path(sys.argv[0]).stem 
    project = str(args.project)
    base_dir = Path(args.base_dir)
    input_dir = base_dir / project / "output"
    output_dir = base_dir / project / "output"
    conda_env = str(args.env)

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


    logging.info("# --- Filter by IMPACT ---")
    logging.info(f"Project: {project}")
    logging.info(f"Timestamp: {timestamp}")
    logging.info(f"Conda_env: {conda_env}")
    logging.info(f"bcftools_version: {bcftools_version}")
    logging.info(f"Output_dir: {output_dir}")
    logging.info("# Processing files")

    # ----------------------------
    # Collect VCFs 
    # ----------------------------
    # Collect only .PASS VCFs, ignore index files
    vcfs = [
        f for f in input_dir.glob("*split_vep.vcf*")
    if not (f.name.endswith(".tbi") or f.name.endswith(".csi"))
    ]

    if not vcfs:
        logging.error("No split_vep VCF files found in input directory.")
        sys.exit(1)

    success_count = fail_count = 0
    total = len(vcfs)

    for in_vcf in vcfs:
        if in_vcf.name.endswith("split_vep.vcf.gz"):
            out_vcf = output_dir / "s5_filter_impact.vcf.gz"
        elif in_vcf.name.endswith("split_vep.vcf"):
            out_vcf = output_dir / "s5_filter_impact.vcf"
        else:
            logging.warning(f"Skipping unexpected file: {in_vcf}")
            continue
        
        success = filter_impact(in_vcf, out_vcf, conda_env)

        if success:
            logging.info(f"{in_vcf.name} -> {out_vcf.name}")
            success_count += 1
        else:
            logging.error(f"{in_vcf.name} -> {out_vcf.name}")
            fail_count += 1

    # ----------------------------
    # Finishing 
    # ----------------------------
    duration = time.time() - start_time
    logging.info(f"# Summary: {success_count} succeeded, {fail_count} failed, {total} total")
    logging.info(f"# Runtime: {format_runtime(duration)}")
    logging.info("# --- End of run ---")

    print(f"Filtering on IMPACT is complete. Log written to {log_file}")

if __name__ == "__main__":
    main()
