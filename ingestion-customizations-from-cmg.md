Developer notes:

- We need to change the dataset registration-process (which occurs through watch.py > INtegratd workflow > inspect.py step) so that it checks for completion markers, depending on host. Only the inspect step will need to be changed for this, not the watch.py script.

- for either nanopore or standard, if the instructions below mentioned hardcoded paths, they will go in the config. For exmaple, nanopore registrations' source paths.

- the directory-filtering criteria, like "is directory", "rejects list" etc is alfeady taken care of by watch.py.

- inspect step will always read from dataset's origin path (or retrieved extraciton path) - that part is not going to change. But inspect step - for new datasets - will be changed so that it checks for completion markers, when apporpriate.

- the code for "wait for x seconds until dir is stable" is also already taken care of by await_stability step, no need to account for it. HOWEVER, the recency_threshold_seconds value of common.py will be selected at runtime based on nanopore/standard registration. Create a second peoperty called recency_threshold_seconds_nanopore.

- for checking if its a nanopore registration in any wf step, the origin_path of the `dataset` retrieved from the API can be checked if it begins with any among a set of certain paths (which will come from config > paths.registration object, like:
```
    'registration': {
        'RAW_DATA': {
            'source_dir_nanopore_p2solo': '/path/to/source/raw_data',
            'source_dir_nanopore_p24': '/path/to/source/raw_data_2',
```
- these are the same paths where watch,py picks new dataset dirs up from.).

Below are notes from AI agent:

---

# Dataset Registration Readiness Criteria for Bioloop

**Purpose**: This document defines the complete set of criteria that CMG uses to determine when a sequencing dataset is ready for registration. A Bioloop AI agent should apply the same criteria when evaluating dataset readiness.

---

## Overview

CMG uses two different registration workers depending on the sequencing platform:
1. **Standard Registration** (`registrations.py`) - For Illumina and most sequencing platforms
2. **Nanopore Registration** (`registrations_nanopore.py`) - For Oxford Nanopore datasets

---

## 1. Standard Illumina Dataset Registration

### Source Locations
Datasets are monitored in directories configured as `source_dirs` in production settings.

**Production Path**: `/path/to/sequencer` (configured per sequencer)

### Registration Criteria (ALL must be satisfied)

#### 1.1 Directory Structure
- ✅ **Must be a directory** (not a file)
- ✅ **Must not be in reject list**: `.snapshots`, `Log Files`, `OldFolders`

#### 1.2 Completion Marker Files (REQUIRED)
The dataset directory must contain **at least one** of the following completion indicator files:

- `*CopyComplete*` (any file matching this pattern)
- `RTAComplete.txt` (exact match)

**Implementation Details:**
```python
# Pattern matching using regex
re.search('CopyComplete', filename)  # Match anywhere in filename
re.search('RTAComplete.txt', filename)  # Exact match
```

**Sequencer Notes:**
- **Standard Illumina (NovaSeq, NextSeq, HiSeq)**: Generates `RTAComplete.txt` when run completes
- **MiSeq**: May not generate the same output files; relies primarily on `CopyComplete` indicators
- **Note**: Some `copy_complete` files are created early in the sequencing process and would yield false positives - only `CopyComplete` and `RTAComplete.txt` are reliable

#### 1.3 Directory Stability Check (REQUIRED)
The dataset must have **no file modifications** for a minimum stability period.

**Stability Period**: `30 seconds` (configured as `Settings.etc['max_age']`)

**Implementation**:
```python
# Find the most recently modified file in the directory
latest_change = max(all_files_in_dir, key=os.path.getmtime)
last_modified_timestamp = os.path.getmtime(latest_change)
time_since_last_change = current_time - last_modified_timestamp

# Dataset is stable if:
if time_since_last_change > 30:  # seconds
    # Ready for registration
```

**Why This Matters**: Ensures all file transfers and writes have completed before starting inspection.

---

## 2. Nanopore Dataset Registration

### Source Locations
Nanopore datasets are monitored in separate source directories configured as `source_dirs_p2`

**Production Paths**:
- `/data/p2solo/` - PromethION P2 Solo sequencer
- `/zpool/p24/` - PromethION P24 sequencer

### Registration Criteria (ALL must be satisfied)

#### 2.1 Path-Based Detection (REQUIRED)
The dataset path **must start with** one of these prefixes:
- `/data/p2solo/`
- `/zpool/p24/`

#### 2.2 NO Completion Marker Files Required
**Important**: Unlike Illumina datasets, Nanopore datasets do **NOT** require completion marker files.

**Rationale** (from code comments): "for nanopore data at 'p2solo' ... no complete file check"

#### 2.3 Directory Stability Check (REQUIRED)
**Stability Period**: `21,600 seconds` (6 hours)

**Implementation**:
```python
# Find the most recently modified file in the directory
latest_change = max(all_files_in_dir, key=os.path.getmtime)
last_modified_timestamp = os.path.getmtime(latest_change)
time_since_last_change = current_time - last_modified_timestamp

# Dataset is stable if:
if time_since_last_change > 21600:  # 6 hours
    # Ready for registration
```

**Why Longer?**: Nanopore sequencing runs can be very long (24-72 hours), with periodic file updates during the run. The 6-hour window ensures the run has fully completed and basecalling is finished.

#### 2.4 Directory Structure
- ✅ **Must be a directory** (not a file)
- ✅ **Must not be in reject list**: `.snapshots`, `Log Files`, `OldFolders`
- ✅ **Must not be already registered/completed**


---

## 3. Common Exclusion Criteria (All Datasets)

These directories are **always rejected** regardless of platform:

| Directory Name | Reason |
|----------------|--------|
| `.snapshots` | NetApp/ZFS snapshot directories |
| `Log Files` | System log directories |
| `OldFolders` | Archived/moved datasets |

---

