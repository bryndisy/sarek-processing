#!/usr/bin/env python3
"""
File Name    : s03_filter_vcf_pass.py
Author       : Bryndis Yngvadottir
Created On   : 22/09/2025
Last Modified: 02/10/2025

Description: 
Filter VCFs in a directory to keep only variants with FILTER == PASS
Uses bcftools from a conda environment (user needs to create this locally, e.g. conda create -n env_bcftools -c bioconda -c conda-forge bcftools).

Usage:
python s03_filter_vcf_pass.py -p <project> -o <base_dir> -e <conda_env>

<project>: project name, this will become the directory for the output and is used in file names and logs
<base_dir>: path to base directory (paths for output directory will be created based on project name)
<conda_env>: name of conda environment with bcftools installed (optional) 

Dependencies: 
conda, bcftools 

"""
import argparse
import sys
from pathlib import Path
from datetime import datetime
import time
import logging

# Import shared utils
from utils import run_command, format_runtime, check_conda_env, get_bcftools_version



# ----------------------------
# Filter VCF on PASS and create index
# ----------------------------
def filter_vcf_bcftools(input_vcf, output_vcf, conda_env):
    """
    Filter VCF records for PASS only, write to output_vcf, and index if gzipped.
    """
    out_format = "-Oz" if output_vcf.suffix == ".gz" else "-Ov"

    cmd = [
        "conda", "run", "-n", conda_env,
        "bcftools", "view",
        "-f", "PASS",
        str(input_vcf),
        out_format,
        "-o", str(output_vcf)
    ]
    if not run_command(cmd):
        return False

    # Index gzipped VCFs
    if output_vcf.suffix == ".gz":
        index_cmd = [
            "conda", "run", "-n", conda_env,
            "bcftools", "index", "-t", str(output_vcf)
        ]
        if not run_command(index_cmd):
            return False

    return True

# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Filter VCF on PASS")
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-o", "--base-dir", required=True, help="Base output directory")
    parser.add_argument("-e", "--env", default=None, help="Conda environment (optional, needed if bcftools is installed in a conda env)")
    args = parser.parse_args()


    # Define command line argument as variables
    script_name = Path(sys.argv[0]).stem
    project = str(args.project)
    base_dir = Path(args.base_dir)
    input_dir = base_dir / project / "output" / "sarek_results" / "annotation" / "haplotypecaller" / "joint_variant_calling"
    output_dir = base_dir / project / "output"

    if not input_dir.is_dir():
        print(f"Error: Input directory '{input_dir}' does not exist.")
        sys.exit(1)

    if not output_dir.is_dir():
        print(f"Creating output directory '{output_dir}'...")
        output_dir.mkdir(parents=True, exist_ok=True)

    conda_env = args.env
    if conda_env and not check_conda_env(conda_env):
        print(f"Error: Conda environment '{conda_env}' does not exist.")
        sys.exit(1)

    # ----------------------------
    # Setup logging
    # ----------------------------
    # ----------------------------
    # Setup logging
    # ----------------------------
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()
    bcftools_version = get_bcftools_version(conda_env) if conda_env else "system"
    log_dir = base_dir / project / "output" / "logs"

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

    logging.info("# --- Filtering VCFs on PASS ---")
    logging.info(f"Project: {project}")
    logging.info(f"Timestamp: {timestamp}")
    logging.info(f"Conda_env: {conda_env or 'system'}")
    logging.info(f"bcftools_version: {bcftools_version}")
    logging.info(f"Input directory: {input_dir}")
    logging.info(f"Base directory: {base_dir}")
    logging.info(f"Output directory: {output_dir}")
    logging.info("# Processing files")

    
    # ----------------------------
    # Collect VCFs and run filtering function  
    # ----------------------------
    vcfs = list(input_dir.glob("*.vcf*"))
    if not vcfs:
        logging.error(f"No VCF files found in {input_dir}")
        sys.exit(1)

    total = len(vcfs)
    success_count, fail_count = 0, 0
    logging.info(f"Total VCF files found: {total}")

    for input_vcf in vcfs:
        if input_vcf.name.endswith(".vcf.gz"):
            output_vcf = output_dir / "s3_filter_PASS.vcf.gz"
        elif input_vcf.name.endswith(".vcf"):
            output_vcf = output_dir / "s3_filter_PASS.vcf"
        else:
            logging.warning(f"Skipping unexpected file: {input_vcf}")
            continue

        success = filter_vcf_bcftools(input_vcf, output_vcf, conda_env)
        if success:
            logging.info(f"Processed: {input_vcf.name} → {output_vcf.name}")
            success_count += 1
        else:
            logging.error(f"Failed to process: {input_vcf.name}")
            fail_count += 1

        logging.info(f"Completed: {success_count} succeeded, {fail_count} failed.")


    # ----------------------------
    # Finishing 
    # ----------------------------
    duration = time.time() - start_time
    logging.info(f"# Summary: {success_count} succeeded, {fail_count} failed, {total} total")
    logging.info(f"# Runtime: {format_runtime(duration)}")
    logging.info("# --- End of run ---")

    print(f"Filtering on PASS completed. Log written to {log_file}")

if __name__ == "__main__":
    main()