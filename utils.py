#!/usr/bin/env python3

"""
File Name    : utils.py
Author       : Bryndis Yngvadottir
Created On   : 01/10/2025
Last Modified: 

Description: 
Script contains several helper functions used across scripts in sarek run and process pipeline. Keep in same directory as main script 

#Functions included: 
run_command
format_runtime
list_conda_envs
check_conda_env
get_bcftools_version
load_config
cleanup_temp_files

"""

import os
import subprocess
import logging
import json
import shutil
from pathlib import Path
from datetime import datetime
import sys

# ----------------------------
# Run shell command
# ----------------------------
def run_command(cmd) -> bool:
    """
    Run a shell command, log stdout+stderr, return True if successful.
    """
    logging.info(f"[{datetime.now()}] Running command: {' '.join(map(str, cmd))}")
    try:
        result = subprocess.run(
            cmd,
            text=True,
            capture_output=True,
            check=True
        )
        if result.stdout:
            logging.info(result.stdout.strip())
        if result.stderr:
            logging.warning(result.stderr.strip())
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with code {e.returncode}: {e}")
        if e.stdout:
            logging.error(e.stdout.strip())
        if e.stderr:
            logging.error(e.stderr.strip())
        return False

# ----------------------------
# Runtime formatter
# ----------------------------
def format_runtime(seconds: float) -> str:
    """
    Convert elapsed time in seconds into Hh Mm Ss format.
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}h {minutes}m {secs}s"


# ----------------------------
# Conda environment helpers
# ----------------------------
def list_conda_envs() -> list[str]:
    """
    Return a list of available conda environment names (basename only).
    """
    try:
        result = subprocess.run(
            ["conda", "env", "list", "--json"],
            capture_output=True,
            text=True,
            check=True
        )
        envs = json.loads(result.stdout).get("envs", [])
        return [Path(path).name for path in envs]
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        logging.warning(f"Could not list conda environments ({e})")
        return []


def check_conda_env(env_name: str) -> bool:
    """
    Return True if a conda environment with the given name exists.
    """
    return env_name in list_conda_envs()


# ----------------------------
# bcftools version checker
# ----------------------------
# To do: make two versions, one that requires minimum version and the other just checking the bcftools version?

def get_bcftools_version(conda_env: str, min_version: str = "1.10") -> str:
    """
    Get bcftools version from the given conda environment.
    Return version string if available, otherwise "unknown".
    Logs error and returns "invalid" if version is below min_version.
    """
    try:
        result = subprocess.run(
            ["conda", "run", "-n", conda_env, "bcftools", "--version"],
            capture_output=True,
            text=True,
            check=True
        )
        import re
        match = re.search(r"bcftools (\d+(\.\d+)+)", result.stdout)
        version = match.group(1) if match else "unknown"
    except subprocess.CalledProcessError:
        return "unknown"

    if version != "unknown":
        def ver_tuple(v): return tuple(map(int, v.split(".")))
        if ver_tuple(version) < ver_tuple(min_version):
            logging.error(
                f"bcftools version {version} is too old. "
                f"Need >= {min_version} for +split-vep plugin."
            )
            return "invalid"
    return version

# ----------------------------
# Config loader (JSON)
# ----------------------------
def load_config(config_file):
    """
    Load a JSON config file and return its 'columns' entry.
    """
    path = Path(config_file)
    if not path.is_file():
        sys.exit(f"Error: Config file {config_file} does not exist.")

    try:
        with open(path) as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        sys.exit(f"Error: Failed to parse JSON config file {config_file}: {e}")

    if "columns" not in cfg:
        sys.exit("Error: No 'columns' entry found in config file.")

    return cfg["columns"]


# ----------------------------
# Cleanup
# ----------------------------        
def cleanup_temp_files(paths, keep_temp=False):
    """
    Remove temporary files or directories and their index files (.csi, .tbi).
    Accepts a list of Path or str.
    - If keep_temp=True, nothing is removed.
    - If given a file, removes it and any .csi/.tbi index files.
    - If given a directory, removes the directory and all its contents.
    """
    if keep_temp:
        logging.info("Keeping temporary files (debug mode).")
        return

    for p in paths:
        p = Path(p)

        try:
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
                logging.debug(f"Removed temp directory: {p}")

            elif p.is_file():
                os.remove(p)
                logging.debug(f"Removed temp file: {p}")

                for ext in [".csi", ".tbi"]:
                    idx_file = Path(str(p) + ext)
                    if idx_file.exists():
                        os.remove(idx_file)
                        logging.debug(f"Removed index file: {idx_file}")

        except Exception as e:
            logging.warning(f"Could not remove {p}: {e}")
