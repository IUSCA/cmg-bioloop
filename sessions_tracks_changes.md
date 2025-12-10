# Bioloop DataProduct & File Type Design  
### CMG → Bioloop Migration & Browser Integration Specification  
**Audience:** Cursor / AI agents implementing the Bioloop rewrite  
**Status:** Authoritative specification  
**Scope:** How to represent and use Dataset Analysis Types + File-Level File Types across Runs, Conversions, DataProducts, Files, Sessions, and Tracks.

---

## 1. Goals

A correct design must:

1. **Separate conceptual “Analysis Type” from technical file format.**
   - *Analysis Type* = what a DataProduct *represents*  
     (alignments, variants, raw reads, QC results, or CMG’s custom labels).
   - *File Type* = the technical format of a specific file  
     (BAM, VCF_GZ, FASTQ_GZ, BIGWIG, BAI, TBI, HTML, TXT, etc.).

2. **Support migration of historic CMG DataProducts, Files, Sessions, and Tracks.**
   - CMG’s `file_type` field (Mongo DataProduct) stores *analysis labels*, not file formats.
   - Technical format is inferred at runtime in CMG (`endsWith('bam'|...)`).

3. **Enable robust genome browser integrations (WashU & IGV Web).**
   - Track discovery must be based on *file-level format*, not Dataset Analysis Type.
   - Correct handling of BAM/BAI, VCF/TBI pairs, bigWig coverage, etc.

4. **Work across Bioloop forks** (genomic + non-genomic).

---

## 2. Key Definitions

### Dataset
Logical entity representing:
- **RAW_DATA** (sequencing run / instrument output), or  
- **DATA_PRODUCT** (output of a Conversion pipeline).

### Dataset File (`dataset_file`)
Physical file belonging to a dataset.  
Each file **must** have a technical format (`filetype`).

### Analysis Type (Dataset-level)
A **label describing the meaning of the DataProduct**.  
Examples:

- Generic: `RAW_READS`, `ALIGNMENT`, `VARIANT_CALLS`, `QC_REPORT`
- CMG-style legacy labels:  
  - `026_ANALYSIS-RESULTS`  
  - `10_RHS24_MERGED_ANALYSIS`  
  - `11_RHS25_ANALYSIS`

This is *not* tied to file formats.

### File Type (File-level)
Normalized **technical format** for each physical file:

- `FASTQ_GZ`
- `BAM`
- `BAI`
- `VCF_GZ`
- `TBI`
- `BIGWIG`
- `HTML_REPORT`
- `QC_TXT`
- `UNKNOWN` (fallback for ambiguous cases)

This **must** be populated by Bioloop or migration tools.

### Genome Attributes
Stored in `dataset_genomic_attributes`:

- `genome_type` (e.g., `"organism"`, `"virus"`, `"panel"`)
- `genome_value` (e.g., `"hg38"`, `"mm10"`, `"GRCh37"`)

---

## 3. CMG Legacy Behavior (To Be Preserved)

### 3.1 CMG’s DataProduct.file_type contains *analysis labels*, not formats
Production examples:

```
026_ANALYSIS-RESULTS
029_ANALYSIS-RESULTS
10_RHS24_MERGED_ANALYSIS
11_RHS25_ANALYSIS
14_RHS31_MERGED_ANALYSIS
...
```

### 3.2 CMG’s `/tracks` endpoint actually discovers trackable files by extension
CMG code:

1. It *tries*:

```js
DataProducts.find({ file_type: { $in: ['bam', 'bigwig', 'bw', 'vcf'] } })
```

→ In production this returns **all DataProducts** because none have `bam`/`vcf` in `file_type`.

2. It then filters files via:

```js
df.path.endsWith('bam') ||
df.path.endsWith('bw') ||
df.path.endsWith('bigwig') ||
df.path.endsWith('vcf')
```

→ **Actual track logic is purely file-extension based**, ignoring DataProduct.file_type.

We must preserve the *behavior*, not the bug.  
Therefore, **Bioloop must use file-level `filetype`** for all format decisions.

---

## 4. Target Bioloop Design

### 4.1 Dataset-level: `analysis_type`
Use `dataset.file_type` as CMG intended:

- **Meaningful human label**
- Used for grouping, categorization, filtering
- Can be CMG-style or standardized like `ALIGNMENT`, `VARIANT_CALLS`, etc.

**Never** use this field to infer file formats.

### 4.2 File-level: `filetype`
Each `dataset_file` receives a **normalized file type**:

Examples:
- `FASTQ_GZ`
- `BAM`
- `BAI`
- `VCF_GZ`
- `TBI`
- `BIGWIG`
- `HTML_REPORT`
- `TEXT` / `QC_TXT`

Bioloop should use this field for:

- IGV and WashU track type selection
- BAM ↔ BAI pairing
- VCF ↔ TBI pairing
- Query performance  
  (`WHERE filetype IN ('BAM','VCF_GZ','BIGWIG')`)

### 4.3 Genome attributes
Stored separately in `dataset_genomic_attributes`.

---

## 5. End-to-End Example (Concrete, Low-Bio Jargon)

### Step 0: RAW_DATA (Sequencing Run)

```
dataset id=1
  type = RAW_DATA
  analysis_type = null
  genomic_details = null
  files:
    path=.../*.bcl → filetype=INSTRUMENT_RAW
```

---

### Step 1: Conversion → FASTQ

```
dataset id=2
  type = DATA_PRODUCT
  analysis_type = FASTQ_CLEANED
  genomic_details.organism = human
  files:
    R1.fastq.gz → FASTQ_GZ
    R2.fastq.gz → FASTQ_GZ
```

---

### Step 2: Conversion → Alignment

```
dataset id=3
  type = DATA_PRODUCT
  analysis_type = ALIGNMENT
  genome = hg38
  files:
    sample.hg38.bam     → BAM
    sample.hg38.bam.bai → BAI
    flagstat.txt        → QC_TXT
```

---

### Step 3: Conversion → Variant calls

```
dataset id=4
  type = DATA_PRODUCT
  analysis_type = GERMLINE_VARIANTS
  genome = hg38
  files:
    sample.hg38.vcf.gz     → VCF_GZ
    sample.hg38.vcf.gz.tbi → TBI
```

---

### Step 4: Browser Session (WashU/IGV)

For `session.genome = hg38`:

Bioloop discovers available tracks by:

```
dataset_file.filetype ∈ (BAM, VCF_GZ, BIGWIG)
AND dataset.genome_value = 'hg38'
```

→ Groups by Dataset.analysis_type:

- ALIGNMENT  
  - sampleX.hg38.bam  

- GERMLINE_VARIANTS  
  - sampleX.hg38.vcf.gz  

**Note:**  
`analysis_type` determines grouping & labels.  
`filetype` determines track *format and behavior*.

---

## 6. Migration Rules (CMG → Bioloop)

### 6.1 Migrate DataProducts → dataset

| CMG field | Bioloop field | Notes |
|----------|----------------|-------|
| DataProduct.file_type | dataset.analysis_type | preserve CMG meaning |
| DataProduct.genomeType | dataset_genomic_attributes.genome_type | |
| DataProduct.genomeValue | dataset_genomic_attributes.genome_value | |
| DataProduct.name | dataset.name | |

### 6.2 Migrate CMG files → dataset_file

For each file:

1. Set `dataset_file.path`
2. Derive `dataset_file.filetype` using a mapping like:

```
*.fastq.gz → FASTQ_GZ
*.bam      → BAM
*.bam.bai  → BAI
*.vcf.gz   → VCF_GZ
*.vcf.gz.tbi → TBI
*.bw or *.bigwig → BIGWIG
*.html → HTML_REPORT
*.txt → TEXT / QC_TXT
```

Use `"UNKNOWN"` when nothing matches.

### 6.3 Migrate Sessions & Tracks

For each CMG Session:

- Create `genome_browser_session`:
  - `genome`, `genome_type` from CMG session
- For each CMG track:
  - Locate matching dataset (by CMG DataProduct ID)
  - Locate matching dataset_file (by filename/path)
  - Create `track`
  - Create `session_track`

This preserves **session behavior** exactly.

---

## 7. Why Both Fields Are Needed

### Storing *only* Analysis Type? (Bad)
- Cannot reliably determine file format.
- Cannot build IGV/WashU tracks.
- Recreates CMG’s `/tracks` bug.
- Multiple file formats inside one DataProduct break assumptions.

### Storing *only* File-level formats? (Also bad)
- Lose CMG’s existing categorization (e.g. `"026_ANALYSIS-RESULTS"`).
- No way to group DataProducts meaningfully in UI.
- No place to preserve `"10_RHS24_MERGED_ANALYSIS"` etc.

### Storing both (Recommended)
- Clean separation of concerns:
  - Dataset.analysis_type → human semantic category  
  - Dataset_file.filetype → machine-readable file format
- Compatible with historic CMG data.
- Supports IGV/WashU robustly.
- Respects Bioloop’s multi-fork architecture.

---

## 8. Implementation Checklist (for Cursor Agents)

### 8.1 Dataset ingestion & conversion

- Set `dataset.analysis_type` using either:
  - migrated CMG values, or
  - standardized values for new pipelines (e.g., ALIGNMENT, VARIANT_CALLS).

### 8.2 dataset_file creation

- Always populate `dataset_file.filetype` via parsing utility.
- Index `(dataset_id, filetype)`.

### 8.3 Browser track discovery logic (Bioloop replacement for CMG `/tracks`)

1. Query Candidate Files:

```
SELECT dataset_file
JOIN dataset
JOIN dataset_genomic_attributes
WHERE filetype IN ('BAM','VCF_GZ','BIGWIG',...)
AND genome_value = session.genome
AND user has access to dataset’s project
```

2. Group by dataset.analysis_type.

3. Return track metadata (path, size, label, color, analysis_type).

### 8.4 Session building

- Insert into `track` → links to dataset_file.id  
- Insert into `session_track` → links track to session

### 8.5 WashU / IGV config generation

For each track:

```
switch(dataset_file.filetype):
  BAM → type: "bam"
  VCF_GZ → type: "vcf"
  BIGWIG → type: "bigwig"
...
```

Use dataset.analysis_type for naming and grouping.

---

## 9. Quick FAQ

**Q: Can a DataProduct have multiple file formats?**  
Yes. BAM + BAI + TXT + VCF is common.  
That’s why file-level `filetype` is essential.

**Q: What if a user manually labels a Dataset’s Analysis Type as “BAM”?**  
Fine — treat it purely as a label.  
Do **not** infer file format from it.

**Q: Should Analysis Type move into `dataset_genomic_attributes`?**  
No. It’s not genomics-specific.  
Genomics-only fields already live there.

**Q: What if a file’s extension is unusual?**  
Set `filetype = "UNKNOWN"` and fall back to filename heuristics only for that file.

---

## 10. Summary Statement

**Bioloop must treat:**

- `dataset.analysis_type`  
  → *Human meaning of the DataProduct (CMG-style labels or standardized categories).*

- `dataset_file.filetype`  
  → *Machine meaning: the actual file format used for IGV, WashU, indexing, pairing, and all technical logic.*

This design:
- Preserves CMG semantics,
- Fixes legacy problems,
- Enables robust browser integration,
- And works across all Bioloop forks.
