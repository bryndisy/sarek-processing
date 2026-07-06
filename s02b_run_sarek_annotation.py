#!/usr/bin/env python3
"""
File Name    : s02_run_sarek_annotation.py
Author       : Bryndis Yngvadottir
Created On   : 04/11/2025
Last Modified: 

Description:
Normalize and then annotate a joint-called VCF with nf-core/sarek's annotation
step, using a JSON configuration file (s02_vep_settings_plugins_paths.json) for all
paths, VEP plugins, and dbNSFP settings.

Before annotation, every VCF listed in the input samplesheet is normalized with
  bcftools norm -m -any -f <fasta>
which splits multiallelic records to biallelic AND left-aligns/trims indels. This
is REQUIRED for correct annotation: VEP/dbNSFP/gnomAD match on exact
CHROM:POS:REF:ALT and gnomAD stores left-aligned, biallelic records, so annotating
an un-normalized joint VCF makes multiallelic sites and non-left-aligned indels
look "novel" (absent from gnomAD) when they are not. Sarek does NOT normalize on
its own, so this step does it explicitly on every run. The normalized copies are
written under <base_dir>/<project>/output/normalized_vcfs/ and a rewritten
samplesheet pointing at them is passed to Sarek.

This is the annotation half of the germline workflow:  s02a (call) -> s02b
(normalize + annotate). s02a runs calling only and produces the joint VCF that
feeds this step.

Usage:
python s02b_run_sarek_annotation.py \
  -p <project> \
  -i <input_file> \
  -o <base_dir> \
  -e <conda_env> \
  --config <config_file>

<project>: project name, this will become the directory for the output and is used in file names and logs
<input_file>: path to the sarek annotation samplesheet (VCFs to annotate)
<base_dir>: path to base directory (subdirectories will be created based on project name etc)
<config_file>: .json configuration file with reference databases, vep plugins etc
<conda_env>: name of conda environment with nextflow AND bcftools installed (optional)

Dependencies:
conda, nextflow, bcftools

Notes to user:
# Configuration file: 
# User will need to modify the configuration file (s02_vep_settings_plugins_paths.json) to set paths to reference databases, VEP plugins and columns needed for dbNSFP
# If the dbBSFP resource is used, please check whether a commercial license is required, if this is not used pleased modify the configuration file to exclude.

"""

import sys
import os
import csv
import time
from datetime import datetime
from pathlib import Path
import argparse
import json
import logging

# Import shared utils
from utils import format_runtime, check_conda_env, run_command

# ----------------------------
# Helpers: Normalization
# ----------------------------

def normalize_vcf(env_name, fasta: str, src: Path, dst: Path) -> bool:
    """Normalize a single VCF (split multiallelics + left-align/trim) and index it.

    bcftools norm -m -any -f <fasta> is required before annotation so that records
    match gnomAD's left-aligned, biallelic representation.
    """
    prefix = ["conda", "run", "-n", env_name] if env_name else []
    norm = prefix + [
        "bcftools", "norm", "-m", "-any", "-f", str(fasta),
        "--output-type", "z", "--output", str(dst), str(src),
    ]
    if not run_command(norm):
        return False
    return run_command(prefix + ["bcftools", "index", "-t", str(dst)])


def _is_vcf_path(val: str) -> bool:
    return isinstance(val, str) and (val.endswith(".vcf.gz") or val.endswith(".vcf"))


def normalize_samplesheet(env_name, fasta: str, input_file: Path,
                          norm_dir: Path, out_samplesheet: Path) -> bool:
    """Normalize every VCF referenced in the annotate samplesheet and write a new
    samplesheet pointing at the normalized copies. Returns True on success.

    Any column whose value looks like a VCF path (.vcf/.vcf.gz) is treated as a
    VCF and rewritten, so this is robust to sarek samplesheet column naming.
    """
    with open(input_file, newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        rows = list(reader)

    if not fieldnames:
        logging.error(f"Samplesheet has no header row: {input_file}")
        return False

    norm_dir.mkdir(parents=True, exist_ok=True)
    cache: dict[str, str] = {}  # resolved source VCF -> normalized VCF

    for row in rows:
        for col in fieldnames:
            val = (row.get(col) or "").strip()
            if not _is_vcf_path(val):
                continue
            src = Path(val)
            if not src.is_absolute():
                src = (input_file.parent / src).resolve()
            if not src.is_file():
                logging.error(f"VCF referenced in samplesheet not found: {src}")
                return False
            if str(src) not in cache:
                base = src.name
                for ext in (".vcf.gz", ".vcf"):
                    if base.endswith(ext):
                        base = base[: -len(ext)]
                        break
                dst = (norm_dir / f"{base}.norm.vcf.gz").resolve()
                logging.info(f"Normalizing (bcftools norm -m -any) {src} -> {dst}")
                if not normalize_vcf(env_name, fasta, src, dst):
                    logging.error(f"Normalization failed for {src}")
                    return False
                cache[str(src)] = str(dst)
            row[col] = cache[str(src)]

    if not cache:
        logging.error(f"No VCF paths found in samplesheet columns: {input_file}")
        return False

    with open(out_samplesheet, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logging.info(
        f"Wrote normalized samplesheet -> {out_samplesheet} "
        f"({len(cache)} VCF(s) normalized)"
    )
    return True


# ----------------------------
# Helpers: Build commands and arguments
# ----------------------------

def build_vep_custom_args(plugins: dict, dir_plugins=None) -> str:
    """Build VEP's --vep_custom_args from the vep_plugins config.

    Each entry maps a plugin name to its argument(s):
      - string                -> --plugin NAME,arg            (e.g. REVEL, Mastermind)
      - list/tuple of strings -> --plugin NAME,arg1,arg2,...  (e.g. CADD snv+indel,
                                   or key:value / key=value args for LoF, SpliceAI)
      - null / "" / []        -> --plugin NAME                (no-argument plugins,
                                   e.g. NMD)

    dir_plugins (optional, from the config key "vep_dir_plugins") sets VEP's
    --dir_plugins, i.e. the directories VEP searches for plugin *modules* (.pm
    files) such as LOFTEE's LoF.pm, which is not in the container's bundled
    plugins. Accepts a string or a list of directories (joined with commas).
    IMPORTANT: --dir_plugins REPLACES the default, so if you set it you must also
    include the container's bundled Plugins directory (the one holding CADD.pm/
    REVEL.pm), otherwise those plugins stop being found. Left empty -> the flag is
    omitted and VEP uses its default plugin directory.
    """
    parts = ["--everything", "--total_length", "--offline", "--cache"]
    if dir_plugins:
        if isinstance(dir_plugins, (list, tuple)):
            dir_plugins = ",".join(str(d) for d in dir_plugins if str(d).strip())
        if str(dir_plugins).strip():
            parts += ["--dir_plugins", str(dir_plugins)]
    for name, path in plugins.items():
        if path is None or path == "" or (isinstance(path, (list, tuple)) and len(path) == 0):
            parts.append(f"--plugin {name}")
        elif isinstance(path, (list, tuple)):
            parts.append(f"--plugin {name},{','.join(map(str, path))}")
        else:
            parts.append(f"--plugin {name},{path}")
    return " ".join(parts)

def build_nextflow_command(env_name, input_file: Path, outdir: Path, config: dict, nextflow_config_file: str) -> list[str]:
    vep_args = build_vep_custom_args(config["vep_plugins"], config.get("vep_dir_plugins"))
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

    # Pin a Nextflow version compatible with nf-core/sarek 3.5.1. Newer Nextflow
    # (25.10+/26.x) uses a strict config parser that fails to parse sarek 3.5.1's
    # nextflow.config ("Variable declarations cannot be mixed with config
    # statements"). The launcher honours NXF_VER and fetches this engine version.
    # setdefault so an explicit `export NXF_VER=...` still overrides it.
    os.environ.setdefault("NXF_VER", "24.10.5")

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

    logging.info("# --- Normalize + annotate joint VCF (Sarek annotation step) ---")
    logging.info(f"Project         : {project}")
    logging.info(f"Sarek input file: {input_file}")
    logging.info(f"Output dir      : {output_dir}")
    logging.info(f"Using conda env : {conda_env if conda_env else '(none)'}")
    logging.info(f"VEP config      : {config}")
    logging.info(f"Nextflow config : {nextflow_config_file}")
    logging.info(f"NXF_OPTS        : {os.environ.get('NXF_OPTS')}")

    start = time.time()

    # Normalize every VCF in the samplesheet BEFORE annotation (bcftools norm -m
    # -any -f fasta): required so records match gnomAD's left-aligned, biallelic
    # representation. Produces a rewritten samplesheet pointing at normalized VCFs.
    logging.info("# Normalizing input VCF(s) before annotation")
    norm_dir = output_dir / "normalized_vcfs"
    norm_samplesheet = norm_dir / "samplesheet_normalized.csv"
    if not normalize_samplesheet(conda_env, config["fasta"], input_file, norm_dir, norm_samplesheet):
        logging.error("Normalization step failed; not proceeding to annotation.")
        sys.exit(1)

    cmd = build_nextflow_command(conda_env, norm_samplesheet, output_dir, config, str(nextflow_config_file))

    ok = run_command(cmd)  # expects True/False
    if not ok:
        logging.error("Nextflow command failed.")
        sys.exit(1)

    logging.info(f"# Runtime: {format_runtime(time.time() - start)}")
    logging.info("# --- End of run ---")
    print(f"Running Sarek annotation completed. Log written to {log_file}")

if __name__ == "__main__":
    main()