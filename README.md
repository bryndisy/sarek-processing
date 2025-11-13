# 🧬 Run nf-core/sarek and process output 
## Pipeline description
This repository contains scripts and configuration files for running the [`nf-core/sarek`](https://nf-co.re/sarek/) pipeline with custom settings for germline whole-exome sequencing (WES) analysis. It also contains further scripts for downstream processing and filtering. 

*Note: At the moment the steps are expected to be run in order and won't work independenlty due to the directory structure. I plan to make them more independent in the future.*

---

## Steps

- Step 1: [`s01_generate_sarek_fastq_input.py`](https://github.com/bryndisy/sarek-processing/blob/main/s01_generate_sarek_fastq_input.py)
Searches a directory containing FASTQ files and generates the input .csv file required to run the full nf-core/sarek germline pipeline from the FASTQ stage. 

- Step 2a: [`s02_run_sarek_full_germline.py`](https://github.com/bryndisy/sarek-processing/blob/main/s02_run_sarek_full_germline.py)
Runs the full nf-core/sarek pipeline for germline data (from FASTQs to annotated VCFs) using a JSON configuration file for all paths, VEP plugins, and dbNSFP settings.

- Step 2b: [`s02_run_sarek_annotation.py`](https://github.com/bryndisy/sarek-processing/blob/main/s02_run_sarek_annotation.py)
Runs the nf-core/sarek pipeline from the annotation step using a JSON configuration file for all paths, VEP plugins, and dbNSFP settings.

- Step 3: [`s03_filter_vcf_pass.py`](https://github.com/bryndisy/sarek-processing/blob/main/s03_filter_vcf_pass.py)
Filters VCFs in a directory to keep only variants with FILTER == PASS

- Step 4: [`s04_split_vep.py`](https://github.com/bryndisy/sarek-processing/blob/main/s04_split_vep.py)
Splits up VEP annotations using bcftools +split-vep, it removes the extra CSQ column and filters on canonical transcripts. 

- Step 5: [`s05_filter_impact.py`](https://github.com/bryndisy/sarek-processing/blob/main/s05_filter_impact.py)
Splits filters VCF for HIGH and MODERATE impact variants.

- Step 6: [`s06_select_vep_cols.py`](https://github.com/bryndisy/sarek-processing/blob/main/s06_select_vep_cols.py)
Outputs .tsv with each sample per line with their variant and genotype and selected VEP columns of interest from the annotated VCF. 

- Extra helper script (if needed): [`s00_bcftools_include_samples.py`](https://github.com/bryndisy/sarek-processing/blob/main/s00_bcftools_include_samples.py)
Filters out specific samples, only keeps samples in sample_list.txt and creates new VCF with these. 
