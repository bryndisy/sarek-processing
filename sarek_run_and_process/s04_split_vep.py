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
# Step 3: keep canonical transcript only
# Step 4: index final VCF
bcftools +split-vep will include many duplicate lines if a variant has multiple consequences, leading to expanded file sizes. 
Intermediate (temp) files from steps 1 and 2 are removed unless --keep-temps is selected. 

Notes to user:
# User will need to modify the accompanying configuration file (s04_split_vep_columns.json) to select VEP annotation fields required 

Usage:
python s04_split_vep.py -p <project> -i <base_dir> -e <conda_env> --config <config_file> --keep-temp (optional)

<project>: project name, used in file names and logs
<base_dir>: path to base directory (from this step onwards this serves as the input and output directory)
<conda_env>: name of conda environment with bcftools installed (optional) 
<config_file>: configuration file with required vep annotation fields


Dependencies: 
conda, bcftools (version 1.10 or higher for +split-vep plugin to work)

To do: 
Create a switch to select whether to keep only canonical transcript
Consider removing the .vcf option as I am not convinced it is needed, files are generally compressed .vcf.gz

"""

import sys
import time
import logging
from pathlib import Path
from datetime import datetime
import argparse

# Import shared utils
from utils import run_command, format_runtime, check_conda_env, get_bcftools_version, load_config, cleanup_temp_files

# ----------------------------
# Split VEP function and process 
# ----------------------------
def split_vep_pipeline(in_vcf, out_vcf, conda_env, columns, output_dir, keep_temp=False):
    """
    Run bcftools +split-vep and postprocess:
      1. Split CSQ into separate annotations
      2. Drop redundant CSQ field
      3. Keep only canonical transcripts
      4. Index final VCF

    Returns: (success: bool, temp_files: list[Path])
    """
    tmpdir = output_dir / "tmp_splitvep"
    tmpdir.mkdir(parents=True, exist_ok=True)

    tmp_split = tmpdir / "splitvep_firstsplit.vcf.gz"
    tmp_no_csq = tmpdir / "splitvep_noCSQ.vcf.gz"

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
        return False, [tmp_split, tmp_no_csq, tmpdir]

    # Step 2: remove CSQ
    cmd_rmcsq = [
        "conda", "run", "-n", conda_env,
        "bcftools", "annotate",
        "--remove", "INFO/CSQ",
        "-Oz", "-o", str(tmp_no_csq), str(tmp_split)
    ]
    if not run_command(cmd_rmcsq):
        return False, [tmp_split, tmp_no_csq, tmpdir]

    # Step 3: keep canonical transcript only
    cmd_canonical = [
        "conda", "run", "-n", conda_env,
        "bcftools", "view",
        "--include", "vep_CANONICAL='YES'",
        "-Oz", "-o", str(out_vcf), str(tmp_no_csq)
    ]
    if not run_command(cmd_canonical):
        return False, [tmp_split, tmp_no_csq, tmpdir]

    # Step 4: index final VCF
    cmd_index = [
        "conda", "run", "-n", conda_env,
        "bcftools", "index", "-t", str(out_vcf)
    ]
    if not run_command(cmd_index):
        return False, [tmp_split, tmp_no_csq, tmpdir]

    # Final return
    if keep_temp:
        logging.info(f"Keeping temp files in {tmpdir}")
        return True, [tmp_split, tmp_no_csq, tmpdir]
    else:
        return True, [tmp_split, tmp_no_csq, tmpdir]
  

# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Split VEP annotations in VCFs")
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-i", "--base-dir", required=True, help="Base output directory")
    parser.add_argument("-e", "--env", help="Conda environment (optional, needed if bcftools version 1.10 or higher is installed in a conda env)")
    parser.add_argument("--config", required=True, help="JSON config file with selected VEP fields")
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
    logging.info("# Processing files")

    # ----------------------------
    # Collect VCFs 
    # ----------------------------
    # Collect only .PASS VCFs, ignore index files
    vcfs = [
        f for f in input_dir.glob("*PASS.vcf*")
    if not (f.name.endswith(".tbi") or f.name.endswith(".csi"))
    ]

    if not vcfs:
        logging.error("No PASS VCF files found in input directory.")
        sys.exit(1)

    success_count = fail_count = 0
    total = len(vcfs)

    for in_vcf in vcfs:
        if in_vcf.name.endswith("PASS.vcf.gz"):
            out_vcf = output_dir / "s4_split_vep.vcf.gz"
        elif in_vcf.name.endswith("PASS.vcf"):
            out_vcf = output_dir / "s4_split_vep.vcf"
        else:
            logging.warning(f"Skipping unexpected file: {in_vcf}")
            continue

        success, temp_files = split_vep_pipeline(in_vcf, out_vcf, conda_env, columns, output_dir, args.keep_temp)

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
