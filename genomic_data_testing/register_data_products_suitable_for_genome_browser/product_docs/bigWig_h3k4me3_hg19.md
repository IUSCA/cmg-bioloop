# bigWig_h3k4me3_hg19 - H3K4me3 ChIP-seq Signal Track

## Dataset Overview

**File:** `GSM429321.bigWig`  
**Source:** https://vizhub.wustl.edu/hubSample/hg19/GSM429321.bigWig  
**Size:** ~several MB  
**Format:** BigWig  
**Genome:** hg19  
**Assay:** ChIP-seq (H3K4me3)

### What This Dataset Is

✅ **Real H3K4me3 ChIP-seq signal data**

✅ **Histone modification associated with active transcription**

✅ **Suitable for testing continuous signal visualization**

### Purpose

This dataset is used to test:
- BigWig file format support
- Continuous signal track visualization
- ChIP-seq data display
- Signal intensity rendering

---

## H3K4me3 Background

**H3K4me3** (Histone H3 Lysine 4 Trimethylation):
- **Function:** Marks active gene promoters
- **Location:** Typically found at transcription start sites (TSS)
- **Interpretation:** High signal = active transcription region
- **Application:** Used to identify active genes and regulatory elements

---

## Testing Ranges (hg19)

Use these genomic coordinates to verify signal tracks:

### Test Region 1
```
chr12:6643000-6648500
```
- **Chromosome:** 12
- **Region Size:** ~5.5 KB
- **Expected:** H3K4me3 signal peaks at promoter regions

### Test Region 2
```
chr7:5566000-5571000
```
- **Chromosome:** 7
- **Region Size:** ~5 KB
- **Expected:** Signal intensity patterns visible

### Test Region 3
```
chr8:128748000-128756000
```
- **Chromosome:** 8
- **Region Size:** ~8 KB
- **Expected:** Continuous signal display

---

## Using This Dataset in Bioloop

### 1. Registration

The dataset is automatically registered as DATA_PRODUCT when placed in:
```
/opt/sca/data/origin/data_products/bigWig_h3k4me3_hg19/
```

### 2. Track Auto-Creation

Because the file has the `.bigWig` extension:
- **Format Detection:** `normalizeFormatFromPath('GSM429321.bigWig')` → `'BIGWIG'`
- **Role Assignment:** `getRoleFromFormat('BIGWIG')` → `'PRIMARY'`
- **Track Creation:** Automatic track created

### 3. Creating a Genome Browser Session

Once the track is created:
1. Navigate to `/sessions/new`
2. Select the `GSM429321.bigWig` track
3. Set genome to `hg19`
4. Create the session
5. View signal patterns in IGV or WashU

---

## Genome Browser Compatibility

### IGV Browser

The track will be serialized as:

```json
{
  "type": "wig",
  "format": "bigwig",
  "name": "GSM429321.bigWig",
  "url": "/api/sessions/{id}/files/expose/...",
  "color": "#2669a3",
  "height": 100
}
```

### WashU Epigenome Browser

The track will be serialized as:

```json
{
  "type": "bigwig",
  "name": "GSM429321.bigWig",
  "url": "/api/sessions/{id}/files/expose/...",
  "showOnHubLoad": true,
  "options": {
    "color": "#2669a3",
    "height": 100
  }
}
```

---

## Verification Steps

### 1. Check Dataset Registration

```bash
curl http://localhost:3030/api/datasets | jq '.datasets[] | select(.name=="bigWig_h3k4me3_hg19")'
```

### 2. Check Track Creation

```bash
curl http://localhost:3030/api/tracks | jq '.tracks[] | select(.name | contains("GSM429321"))'
```

### 3. Test in Genome Browser

1. Create a session with the track
2. Navigate to `chr12:6643000-6648500`
3. Verify signal intensity is visible
4. Look for peaks at promoter regions
5. Test zoom functionality

---

## Interpreting the Data

When viewing this track:
- **High signal** → Active promoter/transcription start site
- **Low signal** → Inactive or repressed region
- **Sharp peaks** → Well-defined TSS
- **Broad signals** → Extended regulatory regions

Compare with gene annotations to verify H3K4me3 enrichment at promoters.

---

## Data Source

**WashU Epigenome Browser Hub Samples**  
https://vizhub.wustl.edu/

Publicly available ChIP-seq data for testing epigenomic visualization.

