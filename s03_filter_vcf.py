#!/usr/bin/env python3
"""
File Name    : s03_filter_vcf.py
Author       : Bryndis Yngvadottir
Created On   : 22/09/2025
Last Modified: 23/06/2026

Description:
VCF-level filtering, run as sequential sub-steps:
  1. Keep only variant sites with FILTER == PASS.
  2. Mask low-confidence sample genotypes to missing (./.) where per-genotype
     read depth (FORMAT/DP) or genotype quality (FORMAT/GQ) falls below threshold,
     using bcftools +setGT.
  3. Drop sites where, after masking, no sample carries an alt allele (every
     genotype is 0/0 or ./.). Disable with --keep-no-alt-sites.

Why both here:
The PASS filter is site-level (is the variant real?). DP/GQ masking is
genotype-level (is this sample's call at that site well supported?). In a
multi-sample VCF one sample can be poorly supported while others are fine, so the
failing genotype is set to missing rather than the whole site being dropped.

Why the no-alt drop (sub-step 3):
Joint genotyping only emits sites that are variant in >=1 sample, but masking can
strip the only variant call at a site, leaving a row where every sample is 0/0 or
./.. Such sites carry no variant after QC, so they are removed by default. The test
is GT-based (GT[*]="alt") because the INFO AC/AN from joint calling are stale once
genotypes have been masked.

Default thresholds: DP >= 10 and GQ >= 20 (genotypes with DP < dp_min OR
GQ < gq_min are set to ./.). Adjust with --dp-min / --gq-min.

Uses bcftools from a conda environment (user creates this locally, e.g.
conda create -n env_bcftools -c bioconda -c conda-forge bcftools). bcftools is also
used directly from PATH if no environment is given.

Usage:
python s03_filter_vcf.py -p <project> -b <base_dir> -e <conda_env> [--dp-min 10] [--gq-min 20] [--keep-no-alt-sites]

<project>:   project name, this will become the directory for the output and is used in file names and logs
<base_dir>:  path to base directory (output paths are created from the project name)
<conda_env>: name of conda environment with bcftools installed (optional; PATH bcftools used if omitted)

Input : <base_dir>/<project>/output/sarek_results/annotation/haplotypecaller/joint_variant_calling/*.vcf.gz
Output: <base_dir>/<project>/output/s3_filter_vcf.vcf.gz

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
from utils import run_command, format_runtime, check_conda_env, get_bcftools_version, cleanup_temp_files


# ----------------------------
# Filter on PASS, then mask low-confidence genotypes, then index
# ----------------------------
def filter_and_mask(input_vcf, output_vcf, conda_env, dp_min, gq_min, tmpdir,
                    drop_no_alt=True):
    """
    1. Keep FILTER == PASS sites.
    2. Set genotypes with FORMAT/DP < dp_min OR FORMAT/GQ < gq_min to missing (./.).
    3. (default) Drop sites where no sample carries an alt allele after masking.
    4. Index the output if gzipped.

    Returns: (success: bool, temp_files: list[Path])
    """
    prefix = ["conda", "run", "-n", conda_env] if conda_env else []
    tmp_pass = tmpdir / "s3_pass.vcf.gz"
    tmp_masked = tmpdir / "s3_masked.vcf.gz"
    temp_files = [tmp_pass, tmp_masked, tmpdir]

    out_format = "z" if output_vcf.suffix == ".gz" else "v"

    # Sub-step 1: keep PASS sites
    cmd_pass = prefix + [
        "bcftools", "view", "-f", "PASS", str(input_vcf),
        "-Oz", "-o", str(tmp_pass),
    ]
    if not run_command(cmd_pass):
        return False, temp_files

    # Sub-step 2: mask low-confidence genotypes to missing (./.)
    # Per-genotype OR (single '|') so each sample is judged on its own DP/GQ.
    # If we will drop no-alt sites next, mask into a temp file; otherwise straight
    # to the final output.
    expr = f"FMT/DP<{dp_min} | FMT/GQ<{gq_min}"
    mask_out = tmp_masked if drop_no_alt else output_vcf
    cmd_mask = prefix + [
        "bcftools", "+setGT", str(tmp_pass),
        "--output-type", "z" if drop_no_alt else out_format,
        "--output", str(mask_out),
        "--",                       # separates bcftools options from plugin options
        "-t", "q",                  # target genotypes matching the include expression
        "-n", ".",                  # set them to missing (./.)
        "-i", expr,
    ]
    if not run_command(cmd_mask):
        return False, temp_files

    # Sub-step 3 (optional): drop sites with no alt-carrying genotype left.
    # Masking can strip the only variant call at a site, leaving a row where every
    # sample is 0/0 or ./.. GT-based test reflects the masked genotypes (the INFO
    # AC/AN from joint calling are stale after masking).
    if drop_no_alt:
        cmd_drop = prefix + [
            "bcftools", "view", "-i", 'GT[*]="alt"', str(tmp_masked),
            "--output-type", out_format, "--output", str(output_vcf),
        ]
        if not run_command(cmd_drop):
            return False, temp_files

    # Sub-step 4: index gzipped output
    if output_vcf.suffix == ".gz":
        cmd_index = prefix + ["bcftools", "index", "-t", str(output_vcf)]
        if not run_command(cmd_index):
            return False, temp_files

    return True, temp_files


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Filter VCF on PASS and mask low-DP/low-GQ genotypes to missing (./.)")
    parser.add_argument("-p", "--project", required=True, help="Project name")
    parser.add_argument("-b", "--base-dir", "-o", dest="base_dir", required=True,
                        help="Base output directory (canonical: --base-dir/-b; -o kept for compatibility)")
    parser.add_argument("-e", "--env", default=None,
                        help="Conda environment with bcftools (optional; PATH bcftools used if omitted)")
    parser.add_argument("--dp-min", type=int, default=10, help="Minimum FORMAT/DP to keep a genotype (default: 10)")
    parser.add_argument("--gq-min", type=int, default=20, help="Minimum FORMAT/GQ to keep a genotype (default: 20)")
    parser.add_argument("--keep-no-alt-sites", action="store_true",
                        help="Keep sites that have no alt-carrying genotype after masking "
                             "(by default such sites, e.g. all 0/0 or ./., are dropped)")
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
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()
    bcftools_version = get_bcftools_version(conda_env) if conda_env else "system"
    log_dir = output_dir / "logs"

    if not log_dir.is_dir():
        print(f"Log directory '{log_dir}' does not exist. Creating it...")
        log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"{script_name}_{project}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),  # append to log file
            logging.StreamHandler(sys.stdout)         # echo to console
        ]
    )

    logging.info("# --- Filtering VCF (PASS + genotype DP/GQ mask) ---")
    logging.info(f"Project: {project}")
    logging.info(f"Timestamp: {timestamp}")
    logging.info(f"Conda_env: {conda_env or 'system'}")
    logging.info(f"bcftools_version: {bcftools_version}")
    logging.info(f"Input directory: {input_dir}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Genotype thresholds: DP >= {args.dp_min}, GQ >= {args.gq_min} (failing genotypes set to ./.)")
    logging.info(f"Drop sites with no alt-carrying genotype after masking: {not args.keep_no_alt_sites}")
    logging.info("# Processing files")

    # ----------------------------
    # Collect VCFs and run filtering
    # ----------------------------
    vcfs = [
        f for f in input_dir.glob("*.vcf*")
        if not (f.name.endswith(".tbi") or f.name.endswith(".csi"))
    ]
    if not vcfs:
        logging.error(f"No VCF files found in {input_dir}")
        sys.exit(1)

    if len(vcfs) > 1:
        logging.error(
            f"Expected a single joint-genotyped VCF but found {len(vcfs)}: "
            f"{[v.name for v in vcfs]}. Outputs use a fixed name and would be "
            "overwritten; please run one project/VCF at a time."
        )
        sys.exit(1)

    total = len(vcfs)
    success_count, fail_count = 0, 0

    tmpdir = output_dir / "tmp_s3"
    tmpdir.mkdir(parents=True, exist_ok=True)

    for input_vcf in vcfs:
        if input_vcf.name.endswith(".vcf.gz"):
            output_vcf = output_dir / "s3_filter_vcf.vcf.gz"
        elif input_vcf.name.endswith(".vcf"):
            output_vcf = output_dir / "s3_filter_vcf.vcf"
        else:
            logging.warning(f"Skipping unexpected file: {input_vcf}")
            continue

        success, temp_files = filter_and_mask(
            input_vcf, output_vcf, conda_env, args.dp_min, args.gq_min, tmpdir,
            drop_no_alt=not args.keep_no_alt_sites,
        )
        if success:
            logging.info(f"Processed: {input_vcf.name} -> {output_vcf.name}")
            success_count += 1
        else:
            logging.error(f"Failed to process: {input_vcf.name}")
            fail_count += 1

    cleanup_temp_files([tmpdir])

    # ----------------------------
    # Finishing
    # ----------------------------
    duration = time.time() - start_time
    logging.info(f"# Summary: {success_count} succeeded, {fail_count} failed, {total} total")
    logging.info(f"# Runtime: {format_runtime(duration)}")
    logging.info("# --- End of run ---")

    print(f"Filtering (PASS + genotype mask) completed. Log written to {log_file}")


if __name__ == "__main__":
    main()
