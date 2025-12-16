# Bioloop – Index Detection Using `dataset_file.metadata.format` and `metadata.role`

**Note**: the code examples below use typescript. However, we will use javascript in our implemetation.

**Introduction**:

- The aim of this document is to instruct the AI agent (yes, i am talking about you, you silly little hobbit) on how to assign a dataset_file object a format and a role

## 1. Fields to use

For `dataset_file`:

- `metadata.format` – string, normalized technical format, e.g.
  - `"BAM"`, `"BAI"`, `"VCF_GZ"`, `"TBI"`, `"FRAGMENTS_TSV_GZ"`, `"BIGWIG"`, etc.
- `metadata.role` – string, one of:
  - `"PRIMARY"` – track file (can be shown in browser)
  - `"INDEX"` – index file for a track (never shown as its own track)

> Do **not** rely on the `filetype` column for any of this logic.
>  If it exists, treat it as legacy / unused.

------

## 2. Normalizing `metadata.format`

Implement a function that takes the file path and returns a **normalized format** string.

```
function normalizeFormatFromPath(path: string): string | null {
  const lower = path.toLowerCase();

  if (lower.endsWith('.bam')) return 'BAM';
  if (lower.endsWith('.bam.bai')) return 'BAI';
  if (lower.endsWith('.bam.crai')) return 'CRAI';

  if (lower.endsWith('.cram')) return 'CRAM';
  if (lower.endsWith('.cram.crai')) return 'CRAI';

  if (lower.endsWith('.vcf.gz')) return 'VCF_GZ';
  if (lower.endsWith('.vcf.gz.tbi')) return 'TBI';
  if (lower.endsWith('.vcf.gz.csi')) return 'CSI';

  if (lower.endsWith('.bed.gz')) return 'BED_GZ';
  if (lower.endsWith('.bed.gz.tbi')) return 'TBI';
  if (lower.endsWith('.bed.gz.csi')) return 'CSI';

  if (lower.endsWith('fragments.tsv.gz')) return 'FRAGMENTS_TSV_GZ';
  if (lower.endsWith('fragments.tsv.gz.tbi')) return 'TBI';

  if (lower.endsWith('.bw') || lower.endsWith('.bigwig')) return 'BIGWIG';
  if (lower.endsWith('.bb') || lower.endsWith('.bigbed')) return 'BIGBED';

  if (lower.endsWith('.fastq') || lower.endsWith('.fq')) return 'FASTQ';
  if (lower.endsWith('.fastq.gz') || lower.endsWith('.fq.gz')) return 'FASTQ_GZ';

  if (lower.endsWith('.tsv')) return 'TSV';
  if (lower.endsWith('.tsv.gz')) return 'TSV_GZ';
  if (lower.endsWith('.csv')) return 'CSV';
  if (lower.endsWith('.json')) return 'JSON';
  if (lower.endsWith('.log')) return 'LOG';
  if (lower.endsWith('.html')) return 'HTML';
  if (lower.endsWith('.pdf')) return 'PDF';
  if (lower.endsWith('.md5')) return 'CHECKSUM';

  return null;
}
```

When a `dataset_file` is created (or when doing a migration pass):

- Read existing `metadata` (if any),
- Set `metadata.format = normalizeFormatFromPath(path)` if not already set,
- Save back to DB.

------

## 3. Which formats have indexes?

- Define a format->role mapping
- Place it in one of the appropriate constant.js/constants.js file

```
const INDEX_TYPES_BY_MAIN_FORMAT: Record<string, string[]> = {
  // Alignment
  BAM: ['BAI', 'CRAI'],
  CRAM: ['CRAI'],

  // Variants
  VCF_GZ: ['TBI', 'CSI'],

  // Tabix-indexed tabular
  BED_GZ: ['TBI', 'CSI'],
  GFF_GZ: ['TBI', 'CSI'],
  GTF_GZ: ['TBI', 'CSI'],
  TSV_GZ: ['TBI', 'CSI'],

  // 10x ATAC
  FRAGMENTS_TSV_GZ: ['TBI'],

  // Big binary formats (self-indexed)
  BIGWIG: [],
  BIGBED: [],

  // Others: no sidecar index
  FASTQ: [],
  FASTQ_GZ: [],
  CSV: [],
  TSV: [],
  JSON: [],
  HTML: [],
  PDF: [],
  LOG: [],
  CHECKSUM: [],
};
```

------

## 4. Initial `metadata.role` from format

When creating `dataset_file`:

```
function initialRoleFromFormat(fmt: string | null): string | null {
  if (!fmt) return null;

  // Index formats by nature
  if (['BAI', 'CRAI', 'TBI', 'CSI'].includes(fmt)) return 'INDEX';

  // Known track-capable main formats
  if (['BAM', 'CRAM', 'VCF_GZ', 'BED_GZ', 'BIGWIG', 'BIGBED', 'FRAGMENTS_TSV_GZ'].includes(fmt)) {
    return 'PRIMARY';
  }

  // Others: no role or future AUX/SECONDARY if you want
  return null;
}
```

Store this as:

```
metadata = {
  ...,
  format: normalizeFormatFromPath(path),
  role: initialRoleFromFormat(fmt),
};
```



------

## 5. Detecting index files per dataset

The following code can be used to --- given a PRIMARY dataset_file object --- find it's corresponding INDEX file in the dataset_file table:

```
function baseNameForIndexMatching(path: string, format: string | null): string {
  const filename = path.split(/[\\/]/).pop() || path;
  if (!format) return filename;

  // For index formats, strip their suffix to get main filename.
  if (['BAI', 'CRAI'].includes(format)) {
    // sample.bam.bai → sample.bam
    return filename.replace(/\.bam\.(bai|crai)$/i, '.bam');
  }

  if (['TBI', 'CSI'].includes(format)) {
    // sample.vcf.gz.tbi → sample.vcf.gz
    // sample.bed.gz.tbi → sample.bed.gz
    // fragments.tsv.gz.tbi → fragments.tsv.gz
    return filename
      .replace(/\.vcf\.gz\.(tbi|csi)$/i, '.vcf.gz')
      .replace(/\.bed\.gz\.(tbi|csi)$/i, '.bed.gz')
      .replace(/\.gff\.gz\.(tbi|csi)$/i, '.gff.gz')
      .replace(/\.gtf\.gz\.(tbi|csi)$/i, '.gtf.gz')
      .replace(/\.tsv\.gz\.(tbi|csi)$/i, '.tsv.gz');
  }

  // For primary files, filename *is* the base.
  return filename;
}
```



------

## 6. How the session/track code should use `metadata.role` and `metadata.format`

This section explains how the roles and formats are intended to be used downstream (e.g., when building Session track lists or IGV/WashU datahubs).

- as a reminder, when a Session is opened in a Gnome Browser like WashU, the JSON list of files to be made available to the Genome Browser is generated by the existing /datahub endpoint, based on the Session in question. If oyu cant find the endpoint  lemme know.
- The following is the logic that can be used for creating this JSON array:

### 6.1 Selecting candidate track files

- Find the 'track' objects that are associated with this genome_browser_session object
- For the selected tracks, find the dataset_files associated with that track.

- **Include only dataset_files where:**
  - `metadata.role === "PRIMARY"`, and
  - `metadata.format` is one of the known browser-compatible formats (see 'Implementatiom Notes' section below --- this list should go in a constants file), for example:
    - `"BAM"`
    - `"CRAM"`
    - `"VCF_GZ"`
    - `"BIGWIG"`
    - `"BIGBED"`
    - `"FRAGMENTS_TSV_GZ"`
    - `"BED_GZ"` (if you plan to support it directly)
- **Exclude:**
  - Files with `metadata.role === "INDEX"`,
  - Files with `metadata.format` not in the supported set (e.g., `FASTQ`, `TSV_GZ` if not used as tracks, `CHECKSUM`, etc.).

In SQL/Prisma-like terms, a “selectable tracks” query might conceptually be:

```
// pseudo-query idea, not exact code:
SELECT *
FROM dataset_file
WHERE dataset_id = :datasetId
  AND metadata->>'role' = 'PRIMARY'
  AND metadata->>'format' IN ('BAM','CRAM','VCF_GZ','BIGWIG','BIGBED','FRAGMENTS_TSV_GZ')
```

(Exact JSON access syntax will depend on how you query Prisma.)

### 6.2 Attaching index files to track config

When building the actual track configuration (i.e. the JSON array returned by /datahub endpoint) to be ready by the genome browser (WashU/IGV):

1. for each file in the above list of filtered **PRIMARY** datasert_files.
   1. Look up its corres[pondoing] INDEX file:
      1. Search within the same dataset and directory.
      2. Look for a `dataset_file` whose:
         1. `metadata.format` is in `INDEX_TYPES_BY_MAIN_FORMAT[primaryFormat]`.
         2. `baseNameForIndexMatching` matches the primary file’s base.
2. If found, generate URLs for both:
   - `url` → secure_download URL for the main (PRIMARY) file.
   - `indexURL` → secure_download URL for the INDEX file.

Example IGV/WashU-style track entry:

```
{
  "type": "bam",
  "name": "NA12878 Alignments (GRCh38)",
  "url": "https://<secure_download>/staged_data/.../NA12878.bam?token=...",
  "indexURL": "https://<secure_download>/staged_data/.../NA12878.bam.bai?token=..."
}
```

Or for VCF:

```
{
  "type": "vcf",
  "name": "NA12878 Genotypes (GRCh38)",
  "url": "https://<secure_download>/staged_data/.../NA12878.GRCh38.genotype.vcf.gz?token=...",
  "indexURL": "https://<secure_download>/staged_data/.../NA12878.GRCh38.genotype.vcf.gz.tbi?token=..."
}
```

If no index is found:

- For formats that **strictly require** index (BAM, CRAM, VCF_GZ, FRAGMENTS_TSV_GZ), you may:
  - don't select thpses files in the array returned by /datahub.
- For formats that don’t need an index (BIGWIG/BIGBED etc.), just omit `indexURL`.

The important part: **INDEX files are never presented as separate tracks**; they’re always inferred and attached.

------

## 7. When to run detection and role assignment

The AI agent / system should integrate this behavior in **one place**:

1. **At conversion / ingest time** – When new `dataset_file` records are created:
   - Set `metadata.format` using `normalizeFormatFromPath(path)`.
   - Set initial `metadata.role` using `initialRoleFromFormat(format)`.

This way:

- New conversions automatically get correct index inference and tagging.

------

## 8. Summary of Responsibilities for the AI Agent

1. **Never** rely on `dataset_file.filetype` for track/index logic in this design.
    Always use `metadata.format` and `metadata.role`.
2. Implement and use:
   - `normalizeFormatFromPath(path: string): string | null`
   - `initialRoleFromFormat(fmt: string | null): string | null`
   - `baseNameForIndexMatching(path: string, format: string | null): string`
   - `INDEX_TYPES_BY_MAIN_FORMAT: Record<string, string[]>`
3. Ensure that:
   - All new `dataset_file` rows get a `metadata.format`.
   - `metadata.role` is assigned as `"PRIMARY"` or `"INDEX"` where appropriate.
   - When a new Session is created in the UI, the UI only offers tracks (to be selected in the Session beong created) whose correspondiong dataset_files have role `"PRIMARY"` , with browser-supported formats. This may already being done in either the UI or the API, but the current filtering logic probably does not filter the suitable files via PRIMARY role. If so, we should update it to use the role.
   - Track configs for WashU/IGV include `indexURL` for those formats that require it.

This mechanism keeps the logic:

- **Format-aware** (no guessing that `.bam.md5` is an index),
- **Browser-compatible**, and

****

**Implementation Notes**:

1. dataset_file objects are created via the `POST /datasets/:id/files` endpoint. that's where we will assign dataset_file.metadata.format and dataset_file.metadata.role
2. These assignments will only be done if the genome_browser/genomeBrowser feature is enabled. Whther this feature is enabled can be checked in the config file in UI/API/workers as needed.
   - There should most likely be existing service files - at least on the API/UI side - which can be used to check if a feature is enabled
3. The tracks corresponding to dataset_files object are also created in the `POST /datasets/:id/files` API. It currently uses the method **getFileFormatFromExtension** to derive the format of the given file. This method can possibly be replaced by one of the methods included in this doc. If so, please update this method and its usages with the one that will replace it.
   1. in this APi, tracks are currently being created for each dataset_file regardless of whether the dataset_file is a PRIMARY file or an INDEX file (because the currrent code does not have the concept of PRIMARY/INDEX files). We need to change this so that tracks are created only for dataset_files which have role PRIMARY.   
4. In thr requested implementation, whenever a constant value is to be used in the code (for example, INDEX_TYPES_BY_MAIN_FORMAT mentioned above), it should go in the approproiate constants file, which already exists in the codebase
5. While the /datahub array will be part of the core cmg-bioloop API, the endpoint via which the genome browser will actually read the files will be in the secure_download API. An /expose endpoint may already be implemented in secure_download for this (which may need to be revised based on these requested changes).
6. DON'T create a DB enum for PRIMARY/INDEX.



**What do do first**:

- tell me your understanding of what needs to be done before beginning implementation



**Final thoughts**:

- Capisce?
- 