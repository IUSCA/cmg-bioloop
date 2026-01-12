# Conversion Testing Documentation

This directory contains auto-generated documentation for sequencing run conversions.

## How Documentation Is Created

When you run a registration script from the `runs/` directory, it automatically creates a markdown file here with the naming pattern:

```
[run_name]---[pipeline].md
```

For example:
- `iseq-DI---bcl2fastq.md` - Created by `runs/register_iseq-DI.sh`

## What's In Each Document

Each conversion testing document includes:

### 1. Dataset Overview
- Source URL and file information
- What the run is (and isn't)
- Purpose for pipeline testing

### 2. Recommended Conversion Configuration
- Complete conversion command (e.g., bcl2fastq)
- Flag-by-flag justification
- What NOT to use and why

### 3. Required Configuration Files
- Complete SampleSheet or equivalent
- Explanation of each section
- Why the configuration is correct

### 4. Expected Outputs
- Directory structure after conversion
- File characteristics
- What makes output valid vs. invalid

### 5. Validation & Troubleshooting
- Checklist for verifying successful conversion
- Common issues and solutions
- Downstream processing steps

## Naming Convention

The naming pattern `[run_name]---[pipeline].md` allows for:
- Multiple pipelines per run (e.g., `iseq-DI---bcl2fastq.md`, `iseq-DI---bcl-convert.md`)
- Clear association between runs and conversion tools
- Easy sorting and organization

## Using This Documentation

These documents serve as:
1. **Reference guides** for running conversions correctly
2. **Validation checklists** to verify successful processing
3. **Troubleshooting resources** when conversions fail
4. **Onboarding materials** for new team members

## Example Usage

```bash
# 1. Register a sequencing run
cd ../runs
./register_iseq-DI.sh

# 2. Check the generated documentation
cat ../conversion_testing/iseq-DI---bcl2fastq.md

# 3. Use the documented command to run conversion
# (Copy bcl2fastq command from the markdown)

# 4. Validate outputs using the checklist
# (Follow validation section in the markdown)
```

## Notes

- Documents are automatically created/updated when registration scripts run
- Don't manually edit these files - regenerate by rerunning the registration script
- Each document is self-contained and comprehensive
- Documents focus on goal-aligned, practical conversion approaches


