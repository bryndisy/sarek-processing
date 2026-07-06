# 🧬 Run nf-core/sarek and process output 
## Pipeline description
This repository contains scripts and configuration files for running the [`nf-core/sarek`](https://nf-co.re/sarek/) pipeline with custom settings for germline whole-exome sequencing (WES) and whole-genome sequencing (WGS) analysis. It also contains further scripts for downstream processing and filtering. 

*Note: At the moment the steps are expected to be run in order and won't work independently due to the directory structure. I plan to make them more independent in the future.*

---

## Steps

- Step 1: [`s01_generate_sarek_fastq_input.py`](https://github.com/bryndisy/sarek-processing/blob/main/s01_generate_sarek_fastq_input.py)
Searches a directory containing FASTQ files and generates the input .csv file required to run the nf-core/sarek germline calling pipeline (Step 2a) from the FASTQ stage. 

- Step 2a: [`s02a_run_sarek_germline.py`](https://github.com/bryndisy/sarek-processing/blob/main/s02a_run_sarek_germline.py)
Runs the nf-core/sarek germline **calling** pipeline (from FASTQs to the joint-called VCF; `--tools haplotypecaller`) for either **WES** or **WGS** data, selected with `--mode {wes,wgs}`, using a JSON configuration file for reference paths. Mode-specific Nextflow resource settings live in [`wes.config`](https://github.com/bryndisy/sarek-processing/blob/main/wes.config) / [`wgs.config`](https://github.com/bryndisy/sarek-processing/blob/main/wgs.config). For WES, pass the capture-kit target BED via `--intervals` to restrict calling/QC to the exome targets. This step **does not annotate** — annotation is done by Step 2b, which first normalizes the joint VCF (see the note below on why the order matters).

- Step 2b: [`s02b_run_sarek_annotation.py`](https://github.com/bryndisy/sarek-processing/blob/main/s02b_run_sarek_annotation.py)
**Normalizes and then annotates** the joint-called VCF. Every VCF in the input samplesheet is first normalized with `bcftools norm -m -any -f <fasta>` (split multiallelics + left-align/trim indels), then nf-core/sarek's annotation step runs VEP/dbNSFP on the normalized VCF. Normalization is mandatory for correct gnomAD/dbNSFP matching and runs on **every** annotation run — see [Why normalization is required](#why-normalization-is-required-before-annotation). The germline workflow is therefore sequential: **Step 2a (call) → Step 2b (normalize + annotate)**.

- Step 3: [`s03_filter_vcf.py`](https://github.com/bryndisy/sarek-processing/blob/main/s03_filter_vcf.py)
VCF-level filtering in sub-steps: (1) keep only variants with FILTER == PASS; (2) mask low-confidence sample genotypes to missing (`./.`) where per-genotype depth or quality falls below threshold (default DP ≥ 10, GQ ≥ 20), using `bcftools +setGT`; (3) drop sites where, after masking, no sample carries an alt allele (all `0/0` or `./.`) — disable with `--keep-no-alt-sites`. Otherwise sites are retained and only failing genotypes are masked.

- Step 4: [`s04_split_vep.py`](https://github.com/bryndisy/sarek-processing/blob/main/s04_split_vep.py)
Splits up VEP annotations using bcftools +split-vep, it removes the extra CSQ column and filters on canonical transcripts. 

- Step 5: [`s05_filter_impact.py`](https://github.com/bryndisy/sarek-processing/blob/main/s05_filter_impact.py)
Splits filters VCF for HIGH and MODERATE impact variants.

- Step 6: [`s06_select_vep_cols.py`](https://github.com/bryndisy/sarek-processing/blob/main/s06_select_vep_cols.py)
Outputs .tsv with each sample per line with their variant and genotype and selected VEP columns of interest from the annotated VCF. 

- Step 7: [`s07_priority_score.py`](https://github.com/bryndisy/sarek-processing/blob/main/s07_priority_score.py)
Computes a 0–100 variant priority score and a discrete tier (1 = highest) from the Step 6 TSV, combining VEP IMPACT, ClinVar significance, in-silico predictors (REVEL/CADD/AlphaMissense) and gnomAD allele frequency. Per-component contributions are written out for transparency, and all weights/thresholds are tunable via [`s07_priority_score_config.json`](https://github.com/bryndisy/sarek-processing/blob/main/s07_priority_score_config.json). Pure Python/pandas (no bcftools) — run it from an environment with `pandas` (e.g. `env_sarek`).

- Step 8: [`s08_extract_genes.py`](https://github.com/bryndisy/sarek-processing/blob/main/s08_extract_genes.py)
Extracts the variants in a user-supplied set of genes from the Step 7 prioritised table. Genes are given via `--genes` (comma-separated) and/or `--gene-file` (one per line); matching is case-insensitive on the `vep_SYMBOL` column. Writes the subset as `.tsv` and `.xlsx`, preserving the Step 7 priority ordering. Pure Python/pandas.

- Extra helper script (if needed): [`s00_bcftools_include_samples.py`](https://github.com/bryndisy/sarek-processing/blob/main/s00_bcftools_include_samples.py)
Filters out specific samples, only keeps samples in sample_list.txt and creates new VCF with these. 

---

## What nf-core/sarek does (Step 2)

[nf-core/sarek](https://nf-co.re/sarek/) is a Nextflow workflow that takes raw sequencing reads through to annotated variant calls. This pipeline drives it with germline settings (`--genome GATK.GRCh38`, `--joint_germline true`) in **two separate runs**: Step 2a does calling (`--tools haplotypecaller`), then Step 2b normalizes the joint VCF and does annotation (`--tools vep`). All the heavy tools run inside Singularity containers — nothing besides Nextflow (and bcftools, for the normalization in Step 2b) needs to be installed locally.

With these settings, Sarek runs the following stages:

1. **Preprocessing (`--step mapping`)**
   - **Alignment** of reads to the reference with **BWA-MEM** (`--aligner bwa-mem`).
   - **Mark duplicates** (GATK MarkDuplicates) to flag PCR/optical duplicate reads.
   - **Base Quality Score Recalibration** (GATK BQSR) to correct systematic basecalling error.
   - Produces analysis-ready, recalibrated **CRAM/BAM** files.
2. **Germline variant calling**
   - **GATK HaplotypeCaller** per sample, emitting a **GVCF**.
   - **Joint genotyping** across all samples (GenomicsDBImport → GenotypeGVCFs) into a single **multi-sample VCF**.
   - **Filtering**: variant recalibration (VQSR) / hard filtering, which stamps the `FILTER` column (`PASS` vs. filtered). *This is the `FILTER` field that Step 3 keys on.*
3. **Normalization** (Step 2b, before annotation — done by this pipeline, not Sarek)
   - `bcftools norm -m -any -f <fasta>` splits multiallelic records into biallelic ones and left-aligns/trims indels, so that variants match the representation used by gnomAD and dbNSFP. **Sarek does not do this on its own**, so Step 2b runs it explicitly on every joint VCF before handing it to VEP. See [Why normalization is required](#why-normalization-is-required-before-annotation).
4. **Annotation**
   - **Ensembl VEP** annotates every variant with consequence, IMPACT, gene/transcript, MANE/canonical flags, ClinVar, gnomAD frequencies, and the in-silico predictors configured via the plugins/dbNSFP fields in `s02_vep_settings_plugins_paths.json` (REVEL, CADD, AlphaMissense, etc.). Annotations are packed into the `CSQ` INFO field — this is what Step 4 splits out.
5. **Quality control**
   - Per-sample and per-run QC (FastQC, samtools/mosdepth coverage, variant-calling metrics) aggregated into a **MultiQC** report.

For **WES**, `--intervals <targets.bed>` restricts calling and coverage QC to the capture-kit regions (see *Exome target intervals* below). Stages 1–2 and QC run in **Step 2a**; stages 3–4 (normalize + annotate) run in **Step 2b**.

### Sarek output

Everything is written under `<base_dir>/<project>/output/sarek_results/`:

| Path | Contents | Produced by |
|------|----------|-------------|
| `preprocessing/` | Recalibrated, duplicate-marked CRAM/BAM files and their indexes. | 2a |
| `variant_calling/haplotypecaller/` | Per-sample GVCFs and the joint-genotyped, recalibrated VCF (`joint_germline_recalibrated.vcf.gz`) — **the input to Step 2b**. | 2a |
| `../normalized_vcfs/` | The joint VCF after `bcftools norm` (+ index) and the rewritten samplesheet that points Sarek's annotation run at it. *(sits under `output/`, not `sarek_results/`.)* | 2b |
| `annotation/haplotypecaller/joint_variant_calling/` | **The normalized, annotated, joint-called multi-sample VCF** (`*.vcf.gz` + `.tbi`). **This is the file Step 3 picks up** to begin downstream processing. | 2b |
| `reports/` | Per-tool QC outputs (FastQC, mosdepth/coverage, samtools stats, bcftools stats, etc.). | 2a |
| `multiqc/` | Aggregated `multiqc_report.html` summarising QC across all samples. | 2a |
| `pipeline_info/` | Nextflow execution reports, timeline, software versions, and the run trace. | 2a/2b |

> The **normalized** annotated VCF in `annotation/.../joint_variant_calling/` is the single hand-off point between Sarek and the downstream steps — Step 3 globs that directory for `*.vcf.gz`.

---

## Requirements and environment

The whole pipeline runs from a **single conda environment**. It needs only:

- **Nextflow** — launches nf-core/sarek in Step 2 (the actual aligners/callers/VEP run inside Sarek's Singularity containers, so they are *not* installed here).
- **bcftools** (≥ 1.11, for `norm`, `+split-vep` and `+setGT`) — used to normalize the joint VCF in Step 2b and for filtering/column selection in Steps 3–6.
- **pandas** and **openpyxl** (with Python) — used by the Step 7 priority-scoring step (openpyxl writes the `.xlsx` output).

Create it once with:

```bash
conda create -n env_sarek -c conda-forge -c bioconda nextflow "bcftools>=1.11" pandas openpyxl python
```

Then `conda activate env_sarek` (or pass `-e env_sarek` to the steps that take it). The examples below use `env_sarek`.

> **Why one environment?** Nextflow, bcftools and pandas have no conflicting dependencies, and Sarek's own tools live in containers, so a single env keeps things simple — one name to activate and to pass to every `-e` flag. You can split it into separate envs (e.g. one for Nextflow, one for bcftools/pandas) if you prefer strict isolation, but it is not required.
>
> **Note:** Nextflow/Sarek also needs a container engine (**Singularity/Apptainer**) and Java available on the system. Singularity is usually provided as a system module rather than via conda; Java is pulled in by the Nextflow conda package.

## How to run each step

The steps share a common directory layout: outputs go to `<base_dir>/<project>/output/`, with logs in `<base_dir>/<project>/output/logs/`. From Step 3 onward this `output/` directory is both the input and output location, and each step finds its input by globbing for the previous step's output, so the steps are expected to be run **in order**.

The examples below assume the single `env_sarek` environment described above (pass its name to `-e`). If you keep separate environments instead, substitute the appropriate name in each command.

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

### Step 2a — run Sarek germline calling (WES or WGS)
Calling only; produces the joint VCF that Step 2b normalizes and annotates.
- **Inputs:** the FASTQ input CSV from Step 1 (`--samplesheet`).
- **Configs:** `s02_vep_settings_plugins_paths.json` (only the reference paths `fasta`/`fasta_fai`/`dict` are read here — VEP/dbNSFP keys are used by Step 2b, so the same config is shared); resource settings come automatically from `wes.config` or `wgs.config` depending on `--mode`.
- **WES:** add `--intervals <targets.bed>` (see *Exome target intervals* below). **WGS:** omit `--mode`-specific extras.
- **Output:** `…/output/sarek_results/variant_calling/haplotypecaller/joint_variant_calling/joint_germline_recalibrated.vcf.gz` — the VCF to list in the Step 2b samplesheet.
```bash
# WES
python s02a_run_sarek_germline.py \
  -p <project> \
  --samplesheet <base_dir>/<project>/output/sarek_fastq_input_<project>.csv \
  --base-dir <base_dir> \
  --config s02_vep_settings_plugins_paths.json \
  --mode wes \
  --intervals /path/to/exome_targets.bed \
  -e env_sarek

# WGS
python s02a_run_sarek_germline.py \
  -p <project> \
  --samplesheet <base_dir>/<project>/output/sarek_fastq_input_<project>.csv \
  --base-dir <base_dir> \
  --config s02_vep_settings_plugins_paths.json \
  --mode wgs \
  -e env_sarek
```

### Step 2b — normalize + annotate the joint VCF
Run this after Step 2a (or whenever you have a called VCF to annotate). Every VCF in the samplesheet is first normalized with `bcftools norm -m -any -f <fasta>`, then annotated. **Do not skip this step and annotate directly in Sarek** — un-normalized annotation mis-matches gnomAD/dbNSFP (see below).
- **Inputs:** a Sarek annotation samplesheet listing the VCF(s) to annotate (`--samplesheet`) — typically the `joint_germline_recalibrated.vcf.gz` from Step 2a.
- **Configs:** `s02_vep_settings_plugins_paths.json` (`--config`; `fasta` is used for normalization, plus the VEP/dbNSFP settings for annotation); resources from `annotation.config` (loaded automatically).
- **Outputs:** normalized VCF(s) + rewritten samplesheet under `…/output/normalized_vcfs/`; the annotated joint VCF under `…/output/sarek_results/annotation/haplotypecaller/joint_variant_calling/`.
- **Requires** `bcftools` on the environment (it is in `env_sarek`).
```bash
python s02b_run_sarek_annotation.py \
  -p <project> \
  --samplesheet /path/to/annotation_samplesheet.csv \
  --base-dir <base_dir> \
  --config s02_vep_settings_plugins_paths.json \
  -e env_sarek
```

#### Why normalization is required before annotation
VEP, dbNSFP and the gnomAD custom annotation all match variants by **exact `CHROM:POS:REF:ALT`**, and gnomAD/dbNSFP store records in **normalized** form — one ALT allele per line (biallelic), with indels **left-aligned and trimmed**. GATK's joint VCF does not guarantee that representation: it can emit **multiallelic** sites (several ALTs on one line) and indels that are not left-aligned. If such a record is annotated as-is, its representation never matches the database key, so the variant comes back with **no gnomAD frequency** and looks *novel* even when it is common (this is what made a 27%-frequency variant appear absent from gnomAD). `bcftools norm -m -any -f <fasta>` fixes both problems — `-m -any` splits multiallelics into biallelic records and `-f <fasta>` left-aligns/trims against the reference. Sarek does **not** normalize before VEP, so Step 2b does it explicitly on every run.

### Step 3 — filter VCF (PASS + genotype DP/GQ mask)
- **Inputs:** the joint-genotyped VCF under `…/output/sarek_results/annotation/haplotypecaller/joint_variant_calling/` (found automatically).
- **Config:** none (genotype thresholds are CLI options).
- **Options:** `--dp-min` (default 10) and `--gq-min` (default 20) set the genotype masking thresholds; `--keep-no-alt-sites` keeps sites that have no alt-carrying genotype left after masking (by default they are dropped). Runs PASS-site filtering, then sets genotypes with DP below `--dp-min` **or** GQ below `--gq-min` to missing (`./.`), then removes sites where masking left no sample carrying the variant.
```bash
python s03_filter_vcf.py \
  -p <project> \
  --base-dir <base_dir> \
  -e env_sarek \
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
  -e env_sarek \
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
  -e env_sarek
```

### Step 6 — select VEP columns to a TSV
- **Inputs:** the `*filter_impact.vcf.gz` from Step 5 (found automatically).
- **Config:** `s06_select_vep_columns.json` (columns to export, via `--config`). Columns must already have been split out in Step 4.
```bash
python s06_select_vep_cols.py \
  -p <project> \
  --base-dir <base_dir> \
  -e env_sarek \
  --config s06_select_vep_columns.json
```

### Step 7 — compute priority score and tier
- **Inputs:** `s6_select_vep_cols.tsv` from Step 6 (found automatically).
- **Config:** `s07_priority_score_config.json` (weights/thresholds, via `--config`).
- **Output:** `s7_priority_score.tsv` **and** `s7_priority_score.xlsx` (same content; the Excel file has the header row frozen). The `.xlsx` needs `openpyxl` — if it is missing the step still writes the TSV and warns.
- **Environment:** does not take `-e`/`conda run`; it `import`s pandas directly, so run it from an environment that has `pandas` and `openpyxl` (e.g. `env_sarek`). Either `conda activate env_sarek` first, or invoke via `conda run -n env_sarek python …` as shown.
```bash
conda run -n env_sarek python s07_priority_score.py \
  -p <project> \
  --base-dir <base_dir> \
  --config s07_priority_score_config.json
```

### Step 8 — extract specific genes
- **Inputs:** `s7_priority_score.tsv` from Step 7 (found automatically).
- **Genes:** supply `--genes` (comma-separated) and/or `--gene-file` (one symbol per line; `#` comments and blank lines ignored). Both sources are combined; matching is case-insensitive on `vep_SYMBOL` (override the column with `--gene-column`).
- **Output:** `s8_gene_extract.tsv` and `s8_gene_extract.xlsx` — same columns and priority ordering as Step 7, restricted to the requested genes. Genes with no matching variants are reported in the log.
- **Gene names:** matching is against `vep_SYMBOL`, the VEP gene symbol after Step 4's transcript pick (MANE Select > canonical > first), i.e. current HGNC symbols. A gene list using older aliases or Ensembl gene IDs will not match — update the list to current symbols, or point `--gene-column` at an ID column that is present in the table. Genes that match nothing are listed in the log, which is the quickest way to spot a naming mismatch.
- **Environment:** same as Step 7 — does not take `-e`; run from an environment with `pandas`/`openpyxl` (e.g. `env_sarek`).
```bash
conda run -n env_sarek python s08_extract_genes.py \
  -p <project> \
  --base-dir <base_dir> \
  --genes BRCA1,BRCA2,TP53
# or from a file:
#   --gene-file /path/to/genes.txt
```

### Optional helper — keep/exclude specific samples (`s00`)
Positional arguments (not flags). Samples prefixed with `^` in the list are excluded.
```bash
python s00_bcftools_include_samples.py \
  <project> \
  /path/to/input.vcf.gz \
  /path/to/sample_list.txt \
  <base_dir>/<project>/output \
  env_sarek
```

---

## Priority score and tiers (Step 7)

Step 7 ranks variants two complementary ways: a continuous **priority score** (0–100, fine ranking) and a discrete **priority tier** (1–4, coarse bucket). Everything below is configurable in [`s07_priority_score_config.json`](https://github.com/bryndisy/sarek-processing/blob/main/s07_priority_score_config.json) — the numbers quoted are the shipped defaults.

### Priority score (0–100)

The score is the sum of four weighted components. Each component is normalised to 0–1, then multiplied by its weight; the weights sum to 100, so a variant maxing out every component scores 100. The four per-component contributions are written to the output TSV (`priority_impact`, `priority_clinvar`, `priority_insilico`, `priority_rarity`) alongside the total (`priority_score`) so every score is auditable.

| Component | Weight | What it measures | Normalised value (0–1) |
|-----------|-------:|------------------|------------------------|
| **IMPACT** | 30 | VEP consequence severity | HIGH = 1.0, MODERATE = 0.6, LOW = 0.15, MODIFIER = 0.0 |
| **ClinVar** | 30 | Clinical significance (CLNSIG) | Pathogenic = 1.0, Likely_pathogenic = 0.8, Conflicting = 0.4, Uncertain = 0.3, Benign/Likely_benign = 0.0 |
| **In-silico** | 25 | Pathogenicity predictors | Mean of available missense predictors: REVEL, AlphaMissense, MetaRNN, ClinPred (all 0–1) and CADD_PHRED (capped at 40 → 1.0), then **lifted by the strongest SpliceAI delta score** via `max()` |
| **Rarity** | 15 | Population allele frequency | novel = 1.0, ≤0.0001 = 0.9, ≤0.001 = 0.6, ≤0.01 = 0.3, common = 0.0 |

Notes:
- **In-silico** averages only the predictors that are present, so a variant isn't penalised for missing scores; if none are present the component is 0. The averaged predictors are configured in `insilico.components`.
- **SpliceAI** is folded in as `max(missense_mean, best_SpliceAI_DS)` (the largest of the four `SpliceAI_pred_DS_*` delta scores), so a splice-affecting variant that the missense predictors miss still scores, without a weak splice signal diluting a strong missense one.
- **Rarity** uses gnomAD POPMAX AF (`gnomAD4.1_joint_POPMAX_AF`), falling back to VEP `MAX_AF`. A **missing AF is treated as novel** (highest rarity), on the assumption that absence from gnomAD means rare rather than unmeasured.
- The score **ranks** variants; it is not a probability of pathogenicity.

### Priority tier (1 = highest, 4 = lowest)

The tier is a coarse, rule-based bucket evaluated top-down (first matching rule wins). It combines ClinVar, IMPACT and allele frequency, and is meant for quick filtering; the numeric score ranks finely *within* a tier. Output column: `priority_tier`.

| Tier | Assigned when (defaults) |
|------|--------------------------|
| **1** | ClinVar Pathogenic/Likely_pathogenic, **or** HIGH impact and rare (AF ≤ 0.001), **or** strong splice (SpliceAI DS ≥ 0.8) and rare. |
| **2** | Predicted damaging (REVEL ≥ 0.7, **or** CADD ≥ 20, **or** AlphaMissense = likely_pathogenic, **or** BayesDel = D, **or** SpliceAI DS ≥ 0.5, **or** HIGH impact) and not common (AF ≤ 0.01). |
| **3** | HIGH or MODERATE impact and not common (AF ≤ 0.05). |
| **4** | Everything else. |

A **ClinVar Benign/Likely_benign call overrides everything and demotes the variant to tier 4**, even if it is HIGH impact or rare (`benign_demote: true`).

**LOFTEE confidence** (`loftee_lc_demote: true`): a HIGH-impact loss-of-function call that LOFTEE flags **low-confidence (`LoF = LC`)** does not earn the HIGH-impact promotion into Tiers 1–2 — it still reaches Tier 3 (which uses the raw impact) for review, rather than being dropped. If LOFTEE did not run, `vep_LoF` is empty and nothing is demoted.

All of the SpliceAI/BayesDel/LOFTEE signals **degrade gracefully**: an absent field simply has no effect, so the tiering is unchanged for variants (or whole runs) where those annotations are missing.

The output (written as both `s7_priority_score.tsv` and `s7_priority_score.xlsx`) is sorted by tier ascending, then score descending, so the highest-priority variants are at the top.

### Tuning

To change the scheme, edit `s07_priority_score_config.json` and re-run **Step 7 only** (it reads the existing `s6_select_vep_cols.tsv`, so no bcftools re-run is needed). Common adjustments: change the `weights` to re-balance components, edit `rarity_bins` thresholds, raise/lower the `tiers` cut-offs (e.g. `tier2_revel_min`), or set `benign_demote` to `false` to stop ClinVar-benign overriding everything.

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
