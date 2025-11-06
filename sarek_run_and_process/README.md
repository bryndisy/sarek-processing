# 🧬 Run nf-core/sarek and process output 

This repository contains scripts and configuration files for running the [`nf-core/sarek`](https://nf-co.re/sarek/) pipeline with custom settings for whole-exome sequencing (WES) analysis. It also contains further scripts for downstream processing. 

---

## Steps

- Step 1: [`s01_generate_sarek_fastq_input.py`](https://github.com/bryndisy/scripts/blob/main/sarek_run_and_process/s01_generate_sarek_fastq_input.py)
This script searches directory containing FASTQ files and generates the input .csv file required to run the full nf-core/sarek germline pipeline from the FASTQ stage. Handles different possible naming conventions of FASTQ files, compressed and uncompressed (.fastq, .fastg.gz, .fq, .fq.gz). 
Checks for missing R2, only accepts pairs of R1 and R2 for the input and keeps tabs on any unmatched.

### Usage:
```shell
python s01_generate_sarek_fastq_input.py -p <project> -f <fastq_dir> -o <base_dir>
```
    - project: project name, used to create directories, and add to file names and logs 
    - fastq_dir: path to directory with FASTQ files
    - base_dir: path to base directory (subdirectories will be created based on project name etc)

- Step 2: [`s01_generate_sarek_fastq_input.py`](https://github.com/bryndisy/scripts/blob/main/sarek_run_and_process/s01_generate_sarek_fastq_input.py)
This scripts runs the full nf-core/sarek pipeline for germline data (from FASTQs to annotated VCFs) using a JSON configuration file for all paths, VEP plugins, and dbNSFP settings.

```shell
python s02_run_sarek_full_germline.py \
  -p <project> \
  -i <input_file> \
  -o <base_dir> \
  -e <conda_env> \
  --config <config_file>
```
    - project: project name, used to create directories, and add to file names and logs
    - input_file: path to fastq input file required for sarek (generated in Step 1)
    - base_dir: path to base directory (subdirectories will be created based on project name etc)
    - config_file: .json configuration file with reference databases, vep plugins etc
    - conda_env: name of conda environment with nextflow installed (optional)


- Step 3: 

- Step 4: 

- Step 5: 

- Step 6: 