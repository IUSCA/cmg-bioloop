# QC Reports (FastQC / MultiQC)

**Feature Scope:** Generation, storage, and serving of quality control reports for conversion data products

**Status:** Implemented (legacy system)

**Last Updated:** 2026-03-01

---

## Overview

After a bcl2fastq (or similar pipeline) conversion completes, the conversion worker runs FastQC on each output FASTQ file and then aggregates the results with MultiQC. Reports are written to persistent project storage and served to the browser via a symlink under the API's static file directory.

---

## Production Paths

### QC output directory

```
/N/project/CMG-SCA/production/qc/
```

### Report directory structure

```
/N/project/CMG-SCA/production/qc/<conversion._id>/<dataproduct.name>/
    ├── *_fastqc.html          # FastQC per-sample HTML report (one per .fastq.gz)
    ├── *_fastqc.zip           # FastQC per-sample ZIP archive (one per .fastq.gz)
    ├── multiqc_report.html    # MultiQC aggregate report
    └── multiqc_data/          # MultiQC supporting data directory
```

### Concrete example (legacy prod)

```
conversion._id   = 64617f38993d2263d2545395
dataproduct.name = ILMN_1614_Apostolova_WGS382_Feb2023

FastQC:  /N/project/CMG-SCA/production/qc/64617f38993d2263d2545395/ILMN_1614_Apostolova_WGS382_Feb2023/0042972522_S7_L002_R1_001_fastqc.html
MultiQC: /N/project/CMG-SCA/production/qc/64617f38993d2263d2545395/ILMN_1614_Apostolova_WGS382_Feb2023/multiqc_report.html
```

---

## Lookup Logic

### Given a conversion, find its QC reports

```
1. Start with: conversion._id

2. Query data products belonging to that conversion:
     db.dataproducts.find({ conversion: ObjectId("<conversion._id>") })

3. For each data product, its `name` field IS the subdirectory name.

4. Reports reside at:
     /N/project/CMG-SCA/production/qc/<conversion._id>/<dataproduct.name>/
```

### Pseudo-code

```python
QC_BASE = "/N/project/CMG-SCA/production/qc"

def get_qc_reports(conversion_id, dataproduct_name):
    qc_dir = f"{QC_BASE}/{conversion_id}/{dataproduct_name}"

    if not os.path.isdir(qc_dir):
        return None  # no QC reports exist

    fastqc_files = glob(f"{qc_dir}/*_fastqc.html")
    multiqc_file = f"{qc_dir}/multiqc_report.html"
    has_multiqc = os.path.isfile(multiqc_file)

    return {
        "fastqc_reports": fastqc_files,
        "multiqc_report": multiqc_file if has_multiqc else None,
    }
```

---

## How Reports Are Generated

### Worker code path

1. `conversions.py` `_runConvert()` runs the bcl2fastq pipeline.
2. On success, calls `self.run_qc(outdir)` where `outdir` = `<converted_dir>/<conversion_id>/<dataset_name>`.
3. `run_qc()` iterates subdirectories of `outdir`, skipping `Reports` and `Stats`.
4. For each project subdirectory, calls `qc.run_fastqc(source=<subdir>, target=qc_dir)`.
5. `run_fastqc()` extracts `conv_id` (path component [-3]) and `project` (path component [-1]) from the source path, creating output at `<qc_dir>/<conv_id>/<project>/`.
6. `qc.run_multiqc(outdir)` runs MultiQC on the same directory, writing `multiqc_report.html` in place.

### Key source files

- `workers/cmg/worker/conversions.py` — `run_qc()`, `_runConvert()`
- `workers/cmg/qc.py` — `run_fastqc()`, `run_multiqc()`

---

## How Reports Are Served to the Browser

### Symlink mechanism

When a conversion completes, the API route (`api/routes/conversions.js`) creates a symlink:

```
/srv/cmg-docker/api/public/qc/<conversion_id>  →  /N/project/CMG-SCA/production/qc/<conversion_id>
```

The API serves everything under `api/public/` as static files via `express.static()`.

### Browser URL

```
https://cmg.sca.iu.edu/qc/<conversion_id>/<dataproduct_name>/multiqc_report.html
```

### UI code

`ui/src/views/ConversionDetails.vue` — `checkQC()` method:

```javascript
let qc_url = `${this.$config.api}/qc/${dp.conversion}/${dp.name}/multiqc_report.html`;
```

---

## Data Model Relationships

```
conversion (conversions collection)
  ._id                → used as directory name under qc/

dataproduct (dataproducts collection)
  .conversion         → ObjectId ref to conversion
  .name               → used as subdirectory name under qc/<conversion._id>/
```

One conversion can produce **multiple** data products (one per `Sample_Project` in the samplesheet). Each gets its own QC subdirectory.

---

## Edge Cases

### No QC reports exist

The `qc/<conversion._id>/` directory may not exist if:
- QC failed or was never run
- The conversion errored before reaching the QC step

### No subdirectory for a specific data product

The `qc/<conversion._id>/<dataproduct.name>/` directory may be missing even if the parent exists (e.g., QC errored on one product but succeeded on another).

### MultiQC ran but failed

The directory may contain `*_fastqc.*` files but no `multiqc_report.html`.

### Unindexed runs (e.g., iSeq DI)

bcl2fastq places `Undetermined_S0_*` files at the output root, **not** in a project subdirectory. The `run_qc()` loop only enters subdirectories, so **no QC reports are generated** for these conversions. There will be no directory under `qc/<conversion._id>/`.

### One conversion → multiple data products

A single conversion (e.g., a NovaSeq run with many projects in the samplesheet) can produce many data products. Each one gets its own QC subdirectory under the same `qc/<conversion._id>/` parent.

---

## Changelog

### 2026-03-01

- Decision: Documented exact production paths and lookup logic for QC reports
- Clarification: `dataproduct.name` is the subdirectory key under `qc/<conversion._id>/`
- Clarification: Unindexed runs produce no QC reports because `run_qc()` only processes subdirectories
- Clarification: One conversion can map to many data products, each with its own QC subdirectory

---

**Last Updated:** 2026-03-01

