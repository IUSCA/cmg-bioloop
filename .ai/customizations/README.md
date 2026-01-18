# CMG-Bioloop Customizations

**Purpose:** Documentation for CMG-specific features and customizations built on top of the Bioloop platform.

This directory contains customizations that are **specific to the CMG-Bioloop fork** and not present in the base Bioloop platform.

---

## CMG-Specific Features

### 1. Genome Browser Sessions & Tracks
**File:** `features/sessions-tracks.md`
- Session management for IGV and WashU browsers
- Track configuration from dataset files
- File serving with cookie authentication
- Browser-specific integration patterns

### 2. Genomic Conversions
**File:** `features/conversions.md`
- Bioinformatics pipeline framework
- Conversion definitions and execution
- Command-line program management
- FASTQ → BAM → VCF pipelines

### 3. CMG Database Migration
**File:** `features/cmg-database-migration.md`
- MongoDB → PostgreSQL migration
- Two-phase sync strategy (Big-Bang + Pollers)
- Cursor-based incremental sync
- Data mapping and transformation

---

## CMG-Specific Conventions

### api_conventions.md
CMG-specific API additions:
- File exposure routing for genome browsers
- Cookie-based file authentication
- Range request support
- Compression handling for binary files

### ui_conventions.md
CMG-specific UI patterns:
- React-in-Vue integration (WashU browser)
- Genome-specific components
- Browser selection patterns

### genome_browser_notes.md
Genome browser implementation details:
- IGV integration
- WashU integration
- File serving patterns
- Generic vs browser-specific naming

### pitfalls.md
CMG-specific mistakes to avoid:
- Genome browser file serving errors
- React unmounting issues
- Compression problems with binary files

---

## Relationship to Platform Core

These customizations **build on** the platform core in `.ai/bioloop/`:

1. **Inheritance:** CMG features use all platform conventions
2. **Extension:** CMG adds new features (sessions, tracks, conversions)
3. **Override:** CMG can override platform defaults where needed

**Priority Order:**
1. `.ai/customizations/` (highest - CMG-specific)
2. `.ai/bioloop/` (platform defaults)
3. Repository code
4. Chat history (lowest)

---

**Last Updated:** 2026-01-16

