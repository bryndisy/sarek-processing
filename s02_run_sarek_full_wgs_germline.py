#!/usr/bin/env python3
"""
File Name    : s02_run_sarek_full_wgs_germline.py
Author       : Bryndis Yngvadottir
Created On   : 22/09/2025
Last Modified: 02/10/2025, 22/10/2025, 04/11/2025

Description: 
Run full nf-core/sarek pipeline for germline data (from FASTQs to annotated VCFs) using a JSON configuration file (02_vep_settings_plugins_paths.json) for all paths, VEP plugins, and dbNSFP settings.

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
# User will need to modify the configuration file (s02_vep_settings_plugins_paths.json) to set paths to reference databases, VEP plugins and columns needed for dbNSFP
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

#!/usr/bin/env python3
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

# Resources for VEP and singularity (adjust here if needed)
VEP_TIME = "48h"
VEP_CPUS = 8
VEP_MEMORY = "64 GB"
PULL_TIMEOUT = "60m"

# ----------------------------
# Helpers: Build commands and arguments
# ----------------------------

def build_vep_custom_args(plugins: dict) -> str:
    """
    Build --vep_custom_args from a dict like:
      {
        "dbNSFP": "dbNSFP5.1a_grch38.gz,field1,field2,...",
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
    input_file: Path,
    outdir: Path,
    config: dict,
    nextflow_cfg_path: str,
    extra_config_path: str | None = None,
) -> list[str]:
    """Build the Nextflow command for nf-core/sarek."""
    vep_args = build_vep_custom_args(config["vep_plugins"])

    prefix = ["conda", "run", "-n", env_name] if env_name else []

    cmd = prefix + [
        "nextflow", "run", "nf-core/sarek", "-r", "3.5.1", "-resume",
        "-profile", "singularity", "-work-dir", "/data/sarek_work_temp", 
    ]

    # Include your optional config (eg disable vcftools) if present
    if extra_config_path:
        cmd += ["-c", extra_config_path]

    # Include our generated temp config (env vars, pullTimeout, process resources)
    cmd += ["-c", nextflow_cfg_path]

    # Pipeline args
    cmd += [
        "--input", str(input_file),
        "--outdir", str(outdir / "sarek_results"),
        "--genome", "GATK.GRCh38",

        # Full pipeline WGS germline (as per your earlier full runs)
        "--step", "mapping",
        "--aligner", "bwa-mem",
        "--joint_germline", "true",

        # WES toggle: keep/remove depending on your dataset
        "--wes", "true",

        # Tools
        "--tools", "haplotypecaller,vep",

        # VEP resources/settings
        "--vep_cache", config["vep_cache"],
        "--vep_include_fasta", "true",
        "--fasta", config["fasta"],
        "--fasta_fai", config["fasta_fai"],
        "--dict", config["dict"],
        "--vep_custom_args", vep_args,

        # dbNSFP settings
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
    parser.add_argument("-i", "--input", required=True, help="Sarek input CSV")
    parser.add_argument("-o", "--base-dir", required=True, help="Base output directory")
    parser.add_argument("--config", required=True, help="JSON config with paths, VEP plugins, dbNSFP settings")
    parser.add_argument("-e", "--env", default=None, help="Conda environment name containing Nextflow (optional)")
    args = parser.parse_args()

    # Normalise paths and names
    script_name = Path(sys.argv[0]).stem
    project = args.project
    input_file = Path(args.input).resolve()
    base_dir = Path(args.base_dir).resolve()
    output_dir = (base_dir / project / "output").resolve()

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

    # ----------------------------
    # Force Singularity temp dirs away from /tmp (which is full)
    # (Use ONE consistent location everywhere)
    # ----------------------------
    #sing_tmp = Path("/home/by215/singularity_tmp")
    #sing_cache = Path("/home/by215/singularity_cache")
    #sing_tmp.mkdir(parents=True, exist_ok=True)
    #sing_cache.mkdir(parents=True, exist_ok=True)

    #os.environ["SINGULARITY_TMPDIR"] = str(sing_tmp)
    #os.environ["SINGULARITY_CACHEDIR"] = str(sing_cache)
    #os.environ["TMPDIR"] = str(sing_tmp)
    #os.environ["TMP"] = str(sing_tmp)
    #os.environ["TEMP"] = str(sing_tmp)

    sing_cache = Path("/home/by215/singularity_cache")
    sing_cache.mkdir(parents=True, exist_ok=True)
    os.environ["SINGULARITY_CACHEDIR"] = str(sing_cache)

    # ----------------------------
    # Temporary Nextflow config (pull timeout + VEP resources)
    # ----------------------------
    tmp_cfg_text = f"""
env {{
  SINGULARITY_CACHEDIR = '{sing_cache}'
}}

executor {{
  name = 'local'
  cpus = 24
}}

process {{
  // Reduce BWA thread usage
  withName: 'NFCORE_SAREK:SAREK:FASTQ_ALIGN_BWAMEM_MEM2_DRAGMAP_SENTIEON:BWAMEM1_MEM' {{
    cpus = 12
  }}

  // MarkDuplicates
  withName: 'NFCORE_SAREK:SAREK:BAM_MARKDUPLICATES:GATK4_MARKDUPLICATES' {{
    cpus   = 4
    memory = '16 GB'
    time   = '24h'
  }}

  // HaplotypeCaller
  withName: 'NFCORE_SAREK:SAREK:BAM_VARIANT_CALLING_GERMLINE_ALL:BAM_VARIANT_CALLING_HAPLOTYPECALLER:GATK4_HAPLOTYPECALLER' {{
    cpus   = 8
    memory = '24 GB'
    time   = '24h'
  }}
	
  // VEP
  withName: 'NFCORE_SAREK:SAREK:VCF_ANNOTATE_ALL:VCF_ANNOTATE_ENSEMBLVEP:ENSEMBLVEP_VEP' {{
    time   = '{VEP_TIME}'
    cpus   = {VEP_CPUS}
    memory = '{VEP_MEMORY}'
  }}
}}

singularity {{
  pullTimeout = '{PULL_TIMEOUT}'
}}
""".strip()



    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
        tmp.write(tmp_cfg_text + "\n")
        tmp.flush()
        nextflow_config_file = tmp.name

    # Optional extra config (eg disable vcftools); include only if it exists
    extra_config = "disable_vcftools.config"
    extra_config_path = str(Path(extra_config).resolve()) if Path(extra_config).is_file() else None
    if extra_config_path is None:
        logging.warning(f"Optional config not found (will continue without it): {extra_config}")

    logging.info("# --- Run full sarek pipeline for germline data ---")
    logging.info(f"Project           : {project}")
    logging.info(f"Timestamp         : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Sarek input file  : {input_file}")
    logging.info(f"Output dir        : {output_dir}")
    logging.info(f"Using conda env   : {conda_env if conda_env else '(none)'}")
    logging.info(f"NXF_OPTS          : {os.environ.get('NXF_OPTS')}")
    logging.info(f"SINGULARITY_TMPDIR: {os.environ.get('SINGULARITY_TMPDIR')}")
    logging.info(f"TMPDIR            : {os.environ.get('TMPDIR')}")
    logging.info(f"Temp NF config    : {nextflow_config_file}")

    start = time.time()

    cmd = build_nextflow_command(
        env_name=conda_env,
        input_file=input_file,
        outdir=output_dir,
        config=config,
        nextflow_cfg_path=nextflow_config_file,
        extra_config_path=extra_config_path,
    )

    logging.info("Nextflow command:\n" + " ".join(cmd))

    ok = run_command(cmd)  # expects True/False
    if not ok:
        logging.error("Nextflow command failed.")
        # keep the tmp config for debugging
        logging.error(f"Left temporary Nextflow config at: {nextflow_config_file}")
        sys.exit(1)

    # Clean up temp config on success
    try:
        os.unlink(nextflow_config_file)
    except Exception:
        pass

    logging.info(f"# Runtime: {format_runtime(time.time() - start)}")
    logging.info("# --- End of run ---")
    print(f"Running sarek pipeline completed. Log written to {log_file}")


if __name__ == "__main__":
    main()
