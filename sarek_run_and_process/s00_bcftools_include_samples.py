#!/usr/bin/env python3
"""
File Name    : s00_bcftools_include_samples.py
Author       : Bryndis Yngvadottir
Created On   : 29/09/2025
Last Modified: 

Description: 
Create new VCF file that only includes samples in sample_list.txt (filters out unwanted samples)
Uses bcftools from a conda environment (user needs to create this locally, e.g. conda create -n env_bcftools -c bioconda -c conda-forge bcftools).
Inputs individual VCF 

Usage:
python s00_bcftools_include_samples.py <project> <input_vcf> <sample_list> <output_dir> <conda_env>
    <project>: project name to add to some file names
    <input_vcf>: path to VCF file
    <sample_list>: list of samples, where samples labelled with ^ should be excluded 
    <output_dir>: path to project's output directory
    <conda_env>: name of conda environment with bcftools installed 

Dependencies: 
conda, bcftools 

"""
import sys
import subprocess
from pathlib import Path
from datetime import datetime
import re
import time
import json
import logging

# ----------------------------
# Conda environment check
# ----------------------------
def check_conda_env(env_name):
    try:
        result = subprocess.run(
            ["conda", "env", "list", "--json"],
            capture_output=True,
            text=True,
            check=True
        )
        envs = json.loads(result.stdout).get("envs", [])
        return any(env_name in path for path in envs)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        logging.warning(f"Could not check conda environments ({e})")
        return False

# ----------------------------
# Tool version check
# ----------------------------
def get_bcftools_version(conda_env):
    try:
        result = subprocess.run(
            ["conda", "run", "-n", conda_env, "bcftools", "--version"],
            capture_output=True,
            text=True,
            check=True
        )
        match = re.search(r"bcftools (\d+(\.\d+)+)", result.stdout)
        return match.group(1) if match else "unknown"
    except subprocess.CalledProcessError:
        return "unknown"

# ----------------------------
# Include specific samples in VCF
# ----------------------------
def include_samples_vcf_bcftools(input_vcf, sample_file, output_vcf, conda_env):
    out_format = "-Oz" if output_vcf.suffix == ".gz" else "-Ov"
    cmd = [
        "conda", "run", "-n", conda_env,
        "bcftools", "view",
        "--samples-file", str(sample_file),
        str(input_vcf),
        out_format,
        "-o", str(output_vcf)
    ]
    logging.info(f"CMD: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        
        if output_vcf.suffix == ".gz":
            index_cmd = [
                "conda", "run", "-n", conda_env,
                "bcftools", "index", "-t", str(output_vcf)
            ]
            logging.info(f"CMD: {' '.join(index_cmd)}")
            subprocess.run(index_cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"bcftools failed on {input_vcf}: {e}")
        return False
    
# ----------------------------
# Filter VCF on PASS and create index
# ----------------------------
def filter_vcf_bcftools(input_vcf, output_vcf, conda_env):
    out_format = "-Oz" if output_vcf.suffix == ".gz" else "-Ov"
    cmd = [
        "conda", "run", "-n", conda_env,
        "bcftools", "view",
        "-f", "PASS",
        str(input_vcf),
        out_format,
        "-o", str(output_vcf)
    ]
    logging.info(f"CMD: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)

        if output_vcf.suffix == ".gz":
            index_cmd = [
                "conda", "run", "-n", conda_env,
                "bcftools", "index", "-t", str(output_vcf)
            ]
            logging.info(f"CMD: {' '.join(index_cmd)}")
            subprocess.run(index_cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"bcftools failed on {input_vcf}: {e}")
        return False


# ----------------------------
# Main
# ----------------------------
def main():
    if len(sys.argv) != 6:
        print(f"Usage: {sys.argv[0]} <project_id> <input_vcf> <sample_list.txt> <output_dir> <conda_env>")
        sys.exit(1)

    project = sys.argv[1]
    input_vcf = Path(sys.argv[2])
    sample_list = Path(sys.argv[3])
    output_dir = Path(sys.argv[4])
    conda_env = sys.argv[5]

    if not input_vcf.is_file():
        print(f"Error: Input VCF '{input_vcf}' does not exist.")
        sys.exit(1)

    if not sample_list.is_file():
        print(f"Error: Sample list '{sample_list}' does not exist.")
        sys.exit(1)

    if not output_dir.is_dir():
        print(f"Output directory '{output_dir}' does not exist. Creating it...")
        output_dir.mkdir(parents=True, exist_ok=True)

    if not check_conda_env(conda_env):
        print(f"Error: Conda environment '{conda_env}' does not exist.")
        sys.exit(1)

    log_file = output_dir / f"s00_include_samples_{project}.log"

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    bcftools_version = get_bcftools_version(conda_env)
    start_time = time.time()

    # Parse sample list into include/exclude
    included_samples = []
    for line in sample_list.read_text().splitlines():
        line = line.strip()

    logging.info("# --- Exclude samples from VCF ---")
    logging.info(f"Project: {project}")
    logging.info(f"Timestamp: {timestamp}")
    logging.info(f"Conda_env: {conda_env}")
    logging.info(f"bcftools_version: {bcftools_version}")
    logging.info(f"Input_vcf: {input_vcf}")
    logging.info(f"Sample_list: {sample_list}")
    logging.info(f"Output_dir: {output_dir}")
    logging.info(f"Included_samples ({len(included_samples)}): {', '.join(included_samples) if included_samples else 'None'}")
    
    # Define output file
    if input_vcf.suffix == ".gz":
        base = input_vcf.stem
        if base.endswith(".vcf"):
            base = base[:-4]
        out_file = output_dir / f"{base}.included_samples.vcf.gz"
    else:
        out_file = output_dir / f"{input_vcf.stem}.included_samples.vcf"


    success = include_samples_vcf_bcftools(input_vcf, sample_list, out_file, conda_env)
    if success:
        logging.info(f"{input_vcf.name} -> {out_file.name}")

    duration = time.time() - start_time
    logging.info(f"# Runtime_seconds: {duration:.2f}")
    logging.info("# --- End of run ---")

if __name__ == "__main__":
    main()
