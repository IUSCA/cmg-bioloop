# Conversions Feature

**Feature Scope:** Genomic data conversion pipelines that transform datasets from one format to another (e.g., FASTQ → BAM → VCF).

**Status:** Implemented

**Related Documentation:**
- `/genome-conversion-pipelines-2026-01-11.md`
- `/genome-browser-sessions-tracks-conversions-2026-01-03.md`
- `/workers/README_conversion_tools.md`

---

## 2026-01-16

### Initial State Documentation

**Context:** This feature provides a flexible, extensible framework for executing bioinformatics pipelines that transform genomic datasets.

**Key Architecture Decisions:**
- Decision: Conversions are stateless pipeline executors
- Decision: Conversions do NOT create genome browser sessions directly
- Decision: Sessions are created only after data products exist
- Constraint: Input datasets must match conversion definition's `dataset_types` whitelist
- Constraint: Only `enabled` conversion definitions can be initiated

**Database Schema:**
- `conversion_definition`: Registry of available conversion pipelines
  - Includes: name, description, enabled, dataset_types, tags, program_id
  - References: cmd_line_program, user (author)
  
- `cmd_line_program`: Executable programs used by conversions
  - Includes: name, executable_path, executable_directory, allow_additional_args
  - Has many: arguments
  
- `argument`: Command-line arguments for programs
  - Includes: flag, datatype, required, default_value, description
  
- `conversion`: Instance of a conversion execution
  - Includes: status, input_dataset_id, output_dataset_id
  - References: conversion_definition, user, datasets
  - Has many: conversion_outputs (for multiple output datasets)

**Execution Flow:**
1. User initiates conversion via API
2. API validates input dataset type against conversion definition
3. API creates conversion record with status `pending`
4. Celery task submitted to worker queue
5. Worker executes command-line program with arguments
6. Worker creates output dataset(s) and links to conversion
7. Conversion status updated to `completed` or `failed`

**Key Code Locations:**
- API Routes: `/api/src/routes/conversions.js`
- API Service: `/api/src/services/conversion.js`
- Worker Tasks: `/workers/workers/tasks/conversion_task.py`
- UI Components: `/ui/src/components/conversions/`
- Prisma Schema: `/api/prisma/schema.prisma` (models: conversion, conversion_definition, cmd_line_program, argument)

**CMG Migration Notes:**
- CMG's conversion pipelines are being migrated to Bioloop's conversion_definition table
- Mapping: CMG `pipeline` field → Bioloop `conversion_definition.name`
- Seed data: `/api/prisma/seed_data/conversion/` (mock data for testing)
- Future: Populate conversion_definition based on unique CMG pipeline values

**Current Status:**
- Core conversion framework: Implemented
- API endpoints: Implemented
- Worker integration: Implemented
- UI components: Implemented
- CMG migration: Planned (not yet executed)

---

## Future Entries

Add entries here as decisions are made, changes are implemented, or issues are resolved.

Format:
```
## YYYY-MM-DD

- Decision: [What was decided]
- Constraint: [New constraint or limitation]
- Clarification: [Behavior clarification]
- Change: [What changed and why]
```

---

**Last Updated:** 2026-01-16

