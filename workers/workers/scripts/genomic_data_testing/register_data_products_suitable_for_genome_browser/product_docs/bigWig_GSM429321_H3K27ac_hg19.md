# bigWig_GSM429321_H3K27ac_hg19 - H3K27ac ChIP-seq Signal Track

## Dataset Overview

**File:** `GSM429321_H3K27ac.bigWig`  
**Source:** https://egg.wustl.edu/d/hg19/GSM429321_H3K27ac.bigWig  
**Size:** Variable (MB range)  
**Format:** BigWig  
**Genome:** hg19  
**Assay:** ChIP-seq (H3K27ac)

### What This Dataset Is

✅ **Real H3K27ac ChIP-seq signal data**

✅ **Histone modification marking active enhancers and promoters**

✅ **Suitable for testing continuous signal visualization**

✅ **Complementary to H3K4me3 for regulatory element mapping**

### Purpose

This dataset is used to test:
- BigWig file format support
- Continuous signal track visualization
- ChIP-seq data display
- Enhancer and promoter identification
- Multi-track epigenomic analysis (when combined with H3K4me3)

---

## H3K27ac Background

**H3K27ac** (Histone H3 Lysine 27 Acetylation):

### Function & Significance
- **Primary Role:** Marks active enhancers and promoters
- **Cellular Process:** Associated with transcriptional activation
- **Genomic Distribution:** Found at both proximal and distal regulatory elements
- **Biological Importance:** Key marker for identifying active regulatory regions

### Interpretation Guidelines
- **High signal at promoters:** Active gene transcription
- **High signal distal to genes:** Active enhancers
- **Broad peaks:** Extended regulatory domains
- **Sharp peaks:** Well-defined regulatory elements

### Comparison with H3K4me3
| Feature | H3K27ac | H3K4me3 |
|---------|---------|---------|
| Primary Location | Enhancers + Promoters | Promoters only |
| Peak Width | Broader | Narrower/Sharp |
| Regulatory Role | Activation mark | TSS marker |
| Genomic Coverage | Wider distribution | TSS-focused |

**Combined Analysis:** Using both H3K27ac and H3K4me3 together allows distinction between:
- **Active promoters:** High H3K27ac + High H3K4me3
- **Active enhancers:** High H3K27ac + Low/No H3K4me3
- **Poised promoters:** Low H3K27ac + High H3K4me3

---

## Testing Recommendations

### Suggested Genomic Regions

While this file may have data across the genome, good testing regions typically include:

#### Known Enhancer-Rich Regions
```
chr8:128700000-128900000
```
- **Region Type:** Super-enhancer region
- **Expected Pattern:** Broad H3K27ac domains
- **Cell Type Specific:** May vary by sample

#### Active Gene Promoters
```
chr12:6640000-6650000
```
- **Region Type:** Gene promoter regions
- **Expected Pattern:** Sharp H3K27ac peaks at TSS
- **Compare with:** H3K4me3 signal (should overlap)

#### Random Sampling for Validation
```
chr7:5500000-5600000
```
- **Region Type:** General genomic region
- **Expected Pattern:** Mix of peaks and background
- **Use For:** Overall data quality assessment

---

## Data Source

**WashU Epigenome Browser Data**  
https://egg.wustl.edu/

Publicly available ChIP-seq data for testing epigenomic visualization and regulatory element mapping.
