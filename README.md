# 🧬 Run nf-core/sarek and process output 
## Pipeline description
This repository contains scripts and configuration files for running the [`nf-core/sarek`](https://nf-co.re/sarek/) pipeline with custom settings for germline whole-exome sequencing (WES) and whole-genome sequencing (WGS) analysis. It also contains further scripts for downstream processing and filtering. 

*Note: At the moment the steps are expected to be run in order and won't work independently due to the directory structure. I plan to make them more independent in the future.*

---

## Steps

- Step 1: [`s01_generate_sarek_fastq_input.py`](https://github.com/bryndisy/sarek-processing/blob/main/s01_generate_sarek_fastq_input.py)
Searches a directory containing FASTQ files and generates the input .csv file required to run the full nf-core/sarek germline pipeline from the FASTQ stage. 

- Step 2a: [`s02a_run_sarek_germline.py`](https://github.com/bryndisy/sarek-processing/blob/main/s02a_run_sarek_germline.py)
Runs the full nf-core/sarek pipeline for germline data (from FASTQs to annotated VCFs) for either **WES** or **WGS** data, selected with `--mode {wes,wgs}`, using a JSON configuration file for all paths, VEP plugins, and dbNSFP settings. Mode-specific Nextflow resource settings live in [`wes.config`](https://github.com/bryndisy/sarek-processing/blob/main/wes.config) / [`wgs.config`](https://github.com/bryndisy/sarek-processing/blob/main/wgs.config). For WES, pass the capture-kit target BED via `--intervals` to restrict calling/QC to the exome targets. 

- Step 2b: [`s02b_run_sarek_annotation.py`](https://github.com/bryndisy/sarek-processing/blob/main/s02b_run_sarek_annotation.py)
Runs the nf-core/sarek pipeline from the annotation step using a JSON configuration file for all paths, VEP plugins, and dbNSFP settings.

- Step 3: [`s03_filter_vcf.py`](https://github.com/bryndisy/sarek-processing/blob/main/s03_filter_vcf.py)
VCF-level filtering in two sub-steps: (1) keep only variants with FILTER == PASS, then (2) mask low-confidence sample genotypes to missing (`./.`) where per-genotype depth or quality falls below threshold (default DP ≥ 10, GQ ≥ 20), using `bcftools +setGT`. Sites are retained; only the failing genotypes are masked.

- Step 4: [`s04_split_vep.py`](https://github.com/bryndisy/sarek-processing/blob/main/s04_split_vep.py)
Splits up VEP annotations using bcftools +split-vep, it removes the extra CSQ column and filters on canonical transcripts. 

- Step 5: [`s05_filter_impact.py`](https://github.com/bryndisy/sarek-processing/blob/main/s05_filter_impact.py)
Splits filters VCF for HIGH and MODERATE impact variants.

- Step 6: [`s06_select_vep_cols.py`](https://github.com/bryndisy/sarek-processing/blob/main/s06_select_vep_cols.py)
Outputs .tsv with each sample per line with their variant and genotype and selected VEP columns of interest from the annotated VCF. 

- Step 7: [`s07_priority_score.py`](https://github.com/bryndisy/sarek-processing/blob/main/s07_priority_score.py)
Computes a 0–100 variant priority score and a discrete tier (1 = highest) from the Step 6 TSV, combining VEP IMPACT, ClinVar significance, in-silico predictors (REVEL/CADD/AlphaMissense) and gnomAD allele frequency. Per-component contributions are written out for transparency, and all weights/thresholds are tunable via [`s07_priority_score_config.json`](https://github.com/bryndisy/sarek-processing/blob/main/s07_priority_score_config.json). Pure Python/pandas — no bcftools/conda required.

- Extra helper script (if needed): [`s00_bcftools_include_samples.py`](https://github.com/bryndisy/sarek-processing/blob/main/s00_bcftools_include_samples.py)
Filters out specific samples, only keeps samples in sample_list.txt and creates new VCF with these. 

---

## How to run each step

The steps share a common directory layout: outputs go to `<base_dir>/<project>/output/`, with logs in `<base_dir>/<project>/output/logs/`. From Step 3 onward this `output/` directory is both the input and output location, and each step finds its input by globbing for the previous step's output, so the steps are expected to be run **in order**.

Two conda environments are assumed in the examples below: `env_nf` (with Nextflow) for Step 2, and `env_bcftools` (with bcftools ≥ 1.10) for the bcftools steps. Use whatever names exist on your system.

> **Flags are consistent across all steps:** the base directory is always `--base-dir` (short `-b`), and the Step 2 input samplesheet is `--samplesheet` (short `-s`). The older short flags still work for backward compatibility — `-o` (Steps 1–3) and `-i` (Steps 4–7) are retained as aliases for `--base-dir`, and `-i/--input` is retained for `--samplesheet` — but the commands below use the canonical flags and are recommended.

### Step 1 — generate Sarek FASTQ input
- **Inputs:** a directory of FASTQ files (`-f`).
- **Config:** none.
- **Output:** the Sarek input CSV used by Step 2.
```bash
python s01_generate_sarek_fastq_input.py \
  -p <project> \
  -f /path/to/fastq_dir \
  --base-dir <base_dir>
```

### Step 2a — run full Sarek germline pipeline (WES or WGS)
- **Inputs:** the FASTQ input CSV from Step 1 (`--samplesheet`).
- **Configs:** `s02_vep_settings_plugins_paths.json` (reference/VEP/dbNSFP paths, via `--config`); resource settings come automatically from `wes.config` or `wgs.config` depending on `--mode`.
- **WES:** add `--intervals <targets.bed>` (see *Exome target intervals* below). **WGS:** omit `--mode`-specific extras.
```bash
# WES
python s02a_run_sarek_germline.py \
  -p <project> \
  --samplesheet <base_dir>/<project>/output/sarek_fastq_input_<project>.csv \
  --base-dir <base_dir> \
  --config s02_vep_settings_plugins_paths.json \
  --mode wes \
  --intervals /path/to/exome_targets.bed \
  -e env_nf

# WGS
python s02a_run_sarek_germline.py \
  -p <project> \
  --samplesheet <base_dir>/<project>/output/sarek_fastq_input_<project>.csv \
  --base-dir <base_dir> \
  --config s02_vep_settings_plugins_paths.json \
  --mode wgs \
  -e env_nf
```

### Step 2b — run Sarek from the annotation step only
Use this when you already have called VCFs and only need annotation.
- **Inputs:** a Sarek annotation samplesheet listing the VCFs to annotate (`--samplesheet`).
- **Configs:** `s02_vep_settings_plugins_paths.json` (`--config`); resources from `annotation.config` (loaded automatically).
```bash
python s02b_run_sarek_annotation.py \
  -p <project> \
  --samplesheet /path/to/annotation_samplesheet.csv \
  --base-dir <base_dir> \
  --config s02_vep_settings_plugins_paths.json \
  -e env_nf
```

### Step 3 — filter VCF (PASS + genotype DP/GQ mask)
- **Inputs:** the joint-genotyped VCF under `…/output/sarek_results/annotation/haplotypecaller/joint_variant_calling/` (found automatically).
- **Config:** none (genotype thresholds are CLI options).
- **Options:** `--dp-min` (default 10) and `--gq-min` (default 20). Runs PASS-site filtering, then sets genotypes with DP below `--dp-min` **or** GQ below `--gq-min` to missing (`./.`); sites are kept.
```bash
python s03_filter_vcf.py \
  -p <project> \
  --base-dir <base_dir> \
  -e env_bcftools \
  --dp-min 10 \
  --gq-min 20
```

### Step 4 — split VEP annotations and select transcripts
- **Inputs:** the `*filter_vcf.vcf.gz` from Step 3 (found automatically).
- **Config:** `s04_split_vep_columns.json` (VEP fields to split out, via `--config`).
- **Options:** `--transcript-pick {priority,canonical,all}` (default `priority` = MANE Select > canonical > first transcript); `--keep-temp` to retain intermediates.
```bash
python s04_split_vep.py \
  -p <project> \
  --base-dir <base_dir> \
  -e env_bcftools \
  --config s04_split_vep_columns.json \
  --transcript-pick priority
```

### Step 5 — filter to HIGH and MODERATE impact
- **Inputs:** the `*split_vep.vcf.gz` from Step 4 (found automatically).
- **Config:** none.
```bash
python s05_filter_impact.py \
  -p <project> \
  --base-dir <base_dir> \
  -e env_bcftools
```

### Step 6 — select VEP columns to a TSV
- **Inputs:** the `*filter_impact.vcf.gz` from Step 5 (found automatically).
- **Config:** `s06_select_vep_columns.json` (columns to export, via `--config`). Columns must already have been split out in Step 4.
```bash
python s06_select_vep_cols.py \
  -p <project> \
  --base-dir <base_dir> \
  -e env_bcftools \
  --config s06_select_vep_columns.json
```

### Step 7 — compute priority score and tier
- **Inputs:** `s6_select_vep_cols.tsv` from Step 6 (found automatically).
- **Config:** `s07_priority_score_config.json` (weights/thresholds, via `--config`).
- No conda env required (pure Python/pandas; needs `pandas` installed).
```bash
python s07_priority_score.py \
  -p <project> \
  --base-dir <base_dir> \
  --config s07_priority_score_config.json
```

### Optional helper — keep/exclude specific samples (`s00`)
Positional arguments (not flags). Samples prefixed with `^` in the list are excluded.
```bash
python s00_bcftools_include_samples.py \
  <project> \
  /path/to/input.vcf.gz \
  /path/to/sample_list.txt \
  <base_dir>/<project>/output \
  env_bcftools
```

---

## Exome target intervals (`--intervals`)

For WES runs (`s02a_run_sarek_germline.py --mode wes`) it is strongly recommended to pass a target BED via `--intervals`, so variant calling and QC are restricted to the captured regions rather than the whole genome (which adds off-target calls, runtime and misleading coverage QC).

**If you don't have the original capture-kit BED** (e.g. old exome data of unknown kit), use a **broad, generic exome BED**. A superset can only *add* regions the old kit didn't capture (those simply return no coverage / no calls — harmless), whereas a too-narrow BED risks *excluding* regions your kit actually captured. Recommended options for GRCh38:

| Option | Notes |
|--------|-------|
| **Twist Comprehensive Exome (hg38)** | Off-the-shelf, broad modern superset. Freely downloadable from Twist Bioscience. Good default when the kit is unknown. |
| **IDT xGen Exome Research Panel v2 (hg38)** | Similar broad off-the-shelf superset, free from IDT. |
| **GENCODE / Ensembl coding exons** | Most kit-agnostic: union of all protein-coding exons derived from the GENCODE annotation, padded ±~100 bp. Fully reproducible (see recipe below). |

A reproducible GENCODE-derived exome BED (requires `bedtools`):

```bash
# 1. Download GENCODE annotation for GRCh38
wget https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_46/gencode.v46.basic.annotation.gtf.gz

# 2. Get an hg38 chrom-sizes file (needed by bedtools slop to clamp at chromosome ends).
#    You do NOT need the alignment reference: the recipe keeps only the main
#    chromosomes (chr1-22, X, Y), whose names/lengths are identical between
#    GATK.GRCh38 and UCSC hg38.
wget https://hgdownload.soe.ucsc.edu/goldenPath/hg38/bigZips/hg38.chrom.sizes
#    (Alternatively, if you have the reference .fai locally:
#       cut -f1,2 /path/to/hg38.GATK.fa.fai > hg38.chrom.sizes )

# 3. Extract protein-coding exons on the main chromosomes -> BED, pad +/-100 bp, merge.
#    The main-chromosome filter avoids GENCODE scaffold/alt names mismatching the
#    reference; the re-sort after slop keeps the input ordered for bedtools merge.
zcat gencode.v46.basic.annotation.gtf.gz \
  | awk -F'\t' '$3=="exon" && /protein_coding/ && $1 ~ /^chr([1-9]|1[0-9]|2[0-2]|X|Y)$/ {print $1"\t"$4-1"\t"$5}' \
  | sort -k1,1 -k2,2n \
  | bedtools slop -b 100 -g hg38.chrom.sizes \
  | sort -k1,1 -k2,2n \
  | bedtools merge \
  > exome_targets_gencode_padded.bed

# 4. Sanity-check: chr-prefixed, only main chromosomes, ~60-90 Mb total
head -1 exome_targets_gencode_padded.bed
cut -f1 exome_targets_gencode_padded.bed | sort -u
awk '{s+=$3-$2} END{print s/1e6" Mb"}' exome_targets_gencode_padded.bed
```

**Important caveats for `GATK.GRCh38`:**
- **Contig naming must match the reference.** `GATK.GRCh38` uses UCSC-style `chr`-prefixed contigs (`chr1`, `chr2`, …), so the BED must be `chr`-prefixed too. Twist/IDT hg38 BEDs and GENCODE are already `chr`-prefixed; if you have a `1/2/3…` (Ensembl-style) BED, add the prefix first.
- **Pad the targets** (~50–100 bp into flanking introns) so splice-region variants are retained.
- These generic targets approximate, not reproduce, the original capture. They are fine for restricting/standardising calling, but on-target coverage metrics will be approximate.
