#!/usr/bin/env python3
"""
File Name    : s01_generate_sarek_fastq_input.py
Author       : Bryndis Yngvadottir
Created On   : 22/09/2025
Last Modified: 02/10/2025, 21/10/2025

Description: 
Searches directory containing FASTQ files and generates the input .csv file required to run the nf-core/sarek pipeline.
Handles different possible naming conventions of FASTQ files, compressed and uncompressed (.fastq, .fastg.gz, .fq, .fq.gz). 
Checks for missing R2, only accepts pairs of R1 and R2 for the input and keeps tabs on any unmatched. 

Usage:
python s01_generate_sarek_fastq_input.py -p <project> -f <fastq_dir> -o <base_dir>
<project>: project name, this will become a directory for the output and is used in file names and logs 
<fastq_dir>: path to directory with FASTQ files
<base_dir>: path to base directory (subdirectories will be created based on project name etc)

"""

import os
from pathlib import Path
import re
import csv
import sys
import logging
from datetime import datetime
import time
import argparse

# Import shared utils
from utils import format_runtime

# ----------------------------
# Configuration
# ----------------------------
# Identify read 1 FASTQ files (accepts _1, _R2, compressed or uncompressed, with or without suffiex e.g. _001)
r1_patterns = [
    r"(.+)_1(_\d+)?\.f(ast)?q(\.gz)?$",
    r"(.+)_R1(_\d+)?\.f(ast)?q(\.gz)?$"
]
# Substitution to identify corresponding Read 2 FASTQ file (pairs of strings here become r1_tag and r2_tag below)
r2_subs = [
    ("_1", "_2"),
    ("_R1", "_R2")
]

# ----------------------------
# Find FASTQs in directory and match R1 and R2 mates
# ----------------------------
def collect_samples(fastq_dir):
    samples = []
    unmatched_r1 = []
    # Walk through FASTQ directory 
    for root, _, files in os.walk(fastq_dir):
        for f in files:
            path = os.path.join(root, f)
            matched = False
            # Matching against R1 patterns
            for pattern, (r1_tag, r2_tag) in zip(r1_patterns, r2_subs):
                if re.search(pattern, f):
                    matched = True
                    # Finding the matching R2 file and build expected path 
                    r2_file = f.replace(r1_tag, r2_tag)
                    r2_path = os.path.join(root, r2_file)
                    # If R2 exists
                    if os.path.exists(r2_path):
                        lane_match = re.search(r"_L(\d+)[._]", f)
                        lane = lane_match.group(1) if lane_match else "1"
                        # Build the sample name, strip the suffixes 
                        sample_name = re.sub(
                            r"(_L\d+)?(_R?1)(_00\d)?\.f(ast)?q(\.gz)?$", "", f
                        )
                        samples.append((sample_name, lane, path, r2_path))
                    # If R2 is missing, log warning about orphan R1
                    else:
                        logging.warning(f"R1 file '{f}' has no matching R2 in {root}")
                        unmatched_r1.append(f)
                    break
            # If file R1 does not match the expected pattern
            if not matched and re.search(r"(_1(_\d+)?\.f(ast)?q)|(_R1(_\d+)?\.f(ast)?q)", f):
                logging.warning(f"File '{f}' looks like R1 but does not match expected pattern")
    return samples, unmatched_r1


# ----------------------------
# Write CSV input file for nf-core/sarek
# ----------------------------
def write_csv(samples, output_file):
    patient_ids = {s: i+1 for i, s in enumerate(sorted({x[0] for x in samples}))}
    with open(output_file, "w", newline="") as out:
        writer = csv.writer(out)
        # Write header row
        writer.writerow(["patient", "sample", "lane", "fastq_1", "fastq_2"])
        # # Write each sample row 
        for sample_name, lane, r1, r2 in sorted(samples):
            writer.writerow([patient_ids[sample_name], sample_name, lane, r1, r2])
    logging.info(f"CSV written: {output_file}")
    return patient_ids


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate FASTQ inputfile for sarek")
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-f", "--fastq_dir", required=True, help="FASTQ file directory")
    parser.add_argument("-o", "--base_dir", required=True, help="Base output directory")
    args = parser.parse_args()

    # Define command line argument as variables
    script_name = Path(sys.argv[0]).stem
    project = str(args.project)
    fastq_dir = Path(args.fastq_dir)
    base_dir = Path(args.base_dir)
    output_dir = base_dir / project / "output"

    # Check on directories 
    if not fastq_dir.is_dir():
        print(f"Error: Input directory '{fastq_dir}' does not exist.")
        sys.exit(1)

    if not output_dir.is_dir():
        print(f"Output directory '{output_dir}' does not exist. Creating it...")
        output_dir.mkdir(parents=True, exist_ok=True)

    # Define files
    output_file = os.path.join(output_dir, f"sarek_fastq_input_{project}.csv")

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
            logging.FileHandler(log_file, mode="a"), # append to log file 
            logging.StreamHandler(sys.stdout) # echo to console
        ]
    )

    logging.info("# --- Generate FASTQ inputfile for sarek ---")
    logging.info(f"Project: {project}")
    logging.info(f"Timestamp: {timestamp}")
    logging.info(f"Fastq_dir: {fastq_dir}")
    logging.info(f"Output_dir: {output_dir}")
    logging.info("# Processing files")

    # ----------------------------
    # Run functions 
    # ----------------------------    

    samples, unmatched_r1 = collect_samples(fastq_dir)
    write_csv(samples, output_file)

    total_pairs = len(samples)
    total_unmatched = len(unmatched_r1)

    logging.info(f"Total paired FASTQs: {total_pairs}")
    logging.info(f"Total unmatched R1 files: {total_unmatched}")
    if total_unmatched > 0:
        for f in unmatched_r1:
            logging.warning(f"Unmatched R1 file: {f}")

    logging.info(f"Output written to: {output_file}")
    logging.info(f"Log written to: {log_file}")
   
    # ----------------------------
    # Finishing 
    # ----------------------------
    duration = time.time() - start_time
    logging.info(f"# Runtime: {format_runtime(duration)}")
    logging.info("# --- End of run ---")

    print(f"Input FASTQ files for sarek have been created. Log written to {log_file}")

if __name__ == "__main__":
    main()
