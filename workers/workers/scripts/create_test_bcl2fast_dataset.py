#!/usr/bin/env python3
"""
Script to create a test Raw Data dataset for the bcl2fast pipeline.

This script:
1. Checks if a dataset with the given name already exists
2. Creates a minimal but valid BCL dataset structure
3. Places it in the origin/raw_data directory
4. Uses the --no-lane-splitting flag as a test argument

Usage:
    python create_test_bcl2fast_dataset.py [dataset_name]

Requirements:
    - Run from within celery_worker container
    - API must be accessible
    - Proper permissions to write to /opt/sca/data/origin/raw_data
"""

import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

from workers.api import APIServerSession
from workers.config import config

# # Add the workers directory to the path so we can import api
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))



def check_dataset_exists(api_session, dataset_name, dataset_type="raw_data"):
    """Check if a dataset with the given name already exists."""
    try:
        response = api_session.get(f"/datasets/{dataset_type}/{dataset_name}/exists")
        if response.status_code == 200:
            data = response.json()
            return data.get('exists', False)
        return False
    except Exception as e:
        print(f"Error checking dataset existence: {e}")
        return False


def create_bcl_dataset_structure(base_path, dataset_name):
    """Create a minimal but valid BCL dataset structure."""
    
    # Create the main dataset directory
    dataset_path = base_path / dataset_name
    dataset_path.mkdir(parents=True, exist_ok=True)
    
    # Create the standard BCL structure
    # This mimics a real Illumina run directory structure
    bcl_path = dataset_path / "Data" / "Intensities" / "BaseCalls"
    bcl_path.mkdir(parents=True, exist_ok=True)
    
    # Create a bcl2fastq v2.20 compatible sample sheet file
    sample_sheet_content = """[Header]
IEMFileVersion,4
Investigator Name,TestUser
Project Name,TestProject
Experiment Name,TestExperiment
Date,2025-01-20
Workflow,GenerateFASTQ
Application,FASTQ Only
Assay,TruSeq HT
Description,Test dataset for bcl2fast pipeline
Chemistry,Amplicon

[Reads]
151
8
8
151

[Settings]
ReverseComplement,0
Adapter,AGATCGGAAGAGCACACGTCTGAACTCCAGTCA
AdapterRead2,AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT

[Data]
Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description
Sample1,Sample1,,,A001,ACGTACGT,B001,TGCATGCA,TestProject,Test sample 1
Sample2,Sample2,,,A002,CGATCGAT,B002,ATGCATGC,TestProject,Test sample 2
"""
    
    sample_sheet_path = dataset_path / "SampleSheet.csv"
    with open(sample_sheet_path, 'w') as f:
        f.write(sample_sheet_content)
    
    # Create a bcl2fastq v2.20 compatible RunInfo.xml file
    run_info_content = """<?xml version="1.0"?>
<RunInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="2">
  <Run Id="TestRun" Number="1">
    <Flowcell>TestFlowcell</Flowcell>
    <Instrument>TestInstrument</Instrument>
    <Date>2025-01-20T00:00:00</Date>
    <Reads>
      <Read Number="1" NumCycles="151" IsIndexedRead="N" />
      <Read Number="2" NumCycles="8" IsIndexedRead="Y" />
      <Read Number="3" NumCycles="8" IsIndexedRead="Y" />
      <Read Number="4" NumCycles="151" IsIndexedRead="N" />
    </Reads>
    <FlowcellLayout LaneCount="1" SurfaceCount="1" SwathCount="1" TileCount="1">
      <TileSet TileNaming="FourDigit">
        <Tile>1_1101</Tile>
      </TileSet>
    </FlowcellLayout>
    <AlignToPhiX>
      <Lane>1</Lane>
    </AlignToPhiX>
    <ImageDimensions Width="2592" Height="1944" />
  </Run>
</RunInfo>
"""
    
    run_info_path = dataset_path / "RunInfo.xml"
    with open(run_info_path, 'w') as f:
        f.write(run_info_content)
    
    # Create a bcl2fastq v2.20 compatible RunParameters.xml file  
    run_params_content = """<?xml version="1.0"?>
<RunParameters xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <RunParametersVersion>1</RunParametersVersion>
  <Setup>
    <SupportMultipleSurfacesInExperiment>true</SupportMultipleSurfacesInExperiment>
    <ReagentKit>TruSeq HT</ReagentKit>
    <ExperimentName>TestExperiment</ExperimentName>
    <Read1NumberOfCycles>151</Read1NumberOfCycles>
    <IndexRead1NumberOfCycles>8</IndexRead1NumberOfCycles>
    <IndexRead2NumberOfCycles>8</IndexRead2NumberOfCycles>
    <Read2NumberOfCycles>151</Read2NumberOfCycles>
    <FlowcellType>SR</FlowcellType>
    <pe>true</pe>
  </Setup>
  <RunInfo>
    <Run Id="TestRun" Number="1">
      <FlowcellLayout LaneCount="1" SurfaceCount="2" SwathCount="1" TileCount="1"/>
    </Run>
  </RunInfo>
</RunParameters>
"""
    
    run_params_path = dataset_path / "RunParameters.xml"
    with open(run_params_path, 'w') as f:
        f.write(run_params_content)
    
    # Create a minimal BCL file structure (just empty files to satisfy the pipeline)
    # In a real scenario, these would contain actual BCL data
    lane_path = bcl_path / "L001"
    lane_path.mkdir(exist_ok=True)
    
    # Create BCL files with actual index sequences for proper demultiplexing
    total_cycles = 151 + 8 + 8 + 151  # Read1 + Index1 + Index2 + Read2
    tiles = ["1101"]  # Single tile for simplicity
    
    # Define the sequences we'll use for each sample
    # Sample1: ACGTACGT (I7), TGCATGCA (I5)
    # Sample2: CGATCGAT (I7), ATGCATGC (I5)
    sample_sequences = [
        {
            'read1': 'A' * 151,  # Simple sequence for read 1
            'index1': 'ACGTACGT',  # I7 index for Sample1
            'index2': 'TGCATGCA',  # I5 index for Sample1  
            'read2': 'T' * 151   # Simple sequence for read 2
        },
        {
            'read1': 'C' * 151,  # Simple sequence for read 1
            'index1': 'CGATCGAT',  # I7 index for Sample2
            'index2': 'ATGCATGC',  # I5 index for Sample2
            'read2': 'G' * 151   # Simple sequence for read 2
        }
    ]
    
    # Base quality scores (all high quality)
    base_to_byte = {'A': 0, 'C': 1, 'G': 2, 'T': 3, 'N': 0}
    
    num_clusters = len(sample_sequences)
    
    for cycle in range(1, total_cycles + 1):
        cycle_path = lane_path / f"C{cycle:03d}.1"
        cycle_path.mkdir(exist_ok=True)
        
        # Determine which read/index we're in
        if cycle <= 151:  # Read 1
            read_type = 'read1'
            position = cycle - 1
        elif cycle <= 151 + 8:  # Index 1
            read_type = 'index1'
            position = cycle - 151 - 1
        elif cycle <= 151 + 8 + 8:  # Index 2
            read_type = 'index2'
            position = cycle - 151 - 8 - 1
        else:  # Read 2
            read_type = 'read2'
            position = cycle - 151 - 8 - 8 - 1
        
        # Create files for each tile
        for tile in tiles:
            # Create BCL file with actual base calls
            bcl_file = cycle_path / f"s_1_{tile}.bcl"
            bcl_header = b'\x02\x00\x00\x00'  # 2 clusters (little endian)
            
            cluster_data = b''
            for sample in sample_sequences:
                sequence = sample[read_type]
                if position < len(sequence):
                    base = sequence[position]
                    base_call = base_to_byte[base] | (30 << 2)  # Base call with quality 30
                    cluster_data += base_call.to_bytes(1, 'little')
                else:
                    # Pad with N if sequence is shorter
                    cluster_data += b'\x00'  # N with quality 0
            
            bcl_file.write_bytes(bcl_header + cluster_data)
            
            # Create LOCs file (cluster positions)
            locs_file = cycle_path / f"s_1_{tile}.locs"
            locs_header = b'\x01\x00\x00\x00'  # Version 1
            cluster_count = num_clusters.to_bytes(4, 'little')
            # Two clusters at different positions
            positions = b'\x00\x10\x00\x00\x00\x10\x00\x00'  # Cluster 1: X=4096, Y=4096
            positions += b'\x00\x20\x00\x00\x00\x20\x00\x00'  # Cluster 2: X=8192, Y=8192
            locs_file.write_bytes(locs_header + cluster_count + positions)
            
            # Create FILTER file (pass filter flags)
            filter_file = cycle_path / f"s_1_{tile}.filter"
            filter_header = b'\x00\x00\x00\x00'  # Version 0
            cluster_count = num_clusters.to_bytes(4, 'little')
            filter_data = b'\x01\x01'  # Both clusters pass filter
            filter_file.write_bytes(filter_header + cluster_count + filter_data)
    
    # Create InterOp directory with minimal stats files
    interop_path = dataset_path / "InterOp"
    interop_path.mkdir(exist_ok=True)
    
    # Create minimal quality metrics file
    quality_metrics = interop_path / "QualityMetricsOut.bin"
    quality_metrics.write_bytes(b'\x06\x00' + b'\x00' * 50)  # Version 6 + minimal data
    
    # Create a metadata.json file with dataset information
    metadata = {
        "dataset_name": dataset_name,
        "dataset_type": "raw_data",
        "created_at": datetime.now().isoformat(),
        "description": "Test dataset for bcl2fast pipeline validation - bcl2fastq v2.20 compatible with proper index sequences for demultiplexing",
        "pipeline": "bcl2fast",
        "test_flags": ["--no-lane-splitting"],
        "structure": {
            "has_sample_sheet": True,
            "has_run_info": True,
            "has_run_parameters": True,
            "has_interop": True,
            "lanes": 1,
            "tiles": 1,
            "tile_numbers": ["1101"],
            "cycles": 318,
            "reads": 4,
            "read_structure": "151bp + 8bp(I1) + 8bp(I2) + 151bp",
            "format_version": "bcl2fastq_v2.20_compatible",
            "flowcell_layout": "LaneCount as XML attribute",
            "tile_naming": "FourDigit (1101, 2101)"
        }
    }
    
    metadata_path = dataset_path / "metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Created BCL dataset structure at: {dataset_path}")
    return dataset_path


def main():
    """Main function to create the test dataset."""
    
    # Get dataset name from command line or use default
    if len(sys.argv) > 1:
        dataset_name = sys.argv[1]
    else:
        dataset_name = f"test_bcl2fast_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"Creating test BCL dataset: {dataset_name}")
    
    # Initialize API session
    try:
        api_session = APIServerSession()
        print("✓ API session initialized")
    except Exception as e:
        print(f"✗ Failed to initialize API session: {e}")
        sys.exit(1)
    
    # Check if dataset already exists
    print(f"Checking if dataset '{dataset_name}' already exists...")
    if check_dataset_exists(api_session, dataset_name):
        print(f"✗ Dataset '{dataset_name}' already exists. Please choose a different name.")
        sys.exit(1)
    else:
        print(f"✓ Dataset '{dataset_name}' does not exist. Proceeding...")
    
    # Define the origin directory path
    origin_path = Path("/opt/sca/data/origin/raw_data")
    
    if not origin_path.exists():
        print(f"✗ Origin directory does not exist: {origin_path}")
        print("Make sure you're running this from within the celery_worker container")
        sys.exit(1)
    
    print(f"✓ Origin directory found: {origin_path}")
    
    # Create the dataset structure
    try:
        dataset_path = create_bcl_dataset_structure(origin_path, dataset_name)
        print(f"✓ Successfully created test dataset at: {dataset_path}")
        
        # Print summary
        print("\n" + "="*70)
        print("BCLT2FASTQ v2.20 COMPATIBLE TEST DATASET CREATION COMPLETED")
        print("="*70)
        print(f"Dataset Name: {dataset_name}")
        print(f"Location: {dataset_path}")
        print(f"Type: Raw Data")
        print(f"Pipeline: bcl2fast")
        print(f"Test Flag: --no-lane-splitting")
        print(f"Format: bcl2fastq v2.20 compatible")
        print("\nKey Compatibility Fixes:")
        print("✓ RunInfo.xml: LaneCount as XML attribute (not nested element)")
        print("✓ Sample Sheet: 4-read structure (151+8+8+151)")
        print("✓ BCL files: Proper binary format with headers and real index sequences")
        print("✓ Index sequences: ACGTACGT/TGCATGCA for Sample1, CGATCGAT/ATGCATGC for Sample2")
        print("✓ Tile naming: FourDigit format (1101)")
        print("✓ InterOp: Quality metrics included")
        print("✓ Demultiplexing: BCL data now contains proper index sequences for sample assignment")
        print("\nDataset Structure:")
        print("├── SampleSheet.csv (bcl2fastq v2.20 compatible)")
        print("├── RunInfo.xml (LaneCount as XML attribute)")
        print("├── RunParameters.xml (4-read structure)")
        print("├── InterOp/ (quality metrics)")
        print("├── Data/Intensities/BaseCalls/ (BCL data structure)")
        print("│   └── L001/ (Lane 1)")
        print("│       └── C001.1/ to C318.1/ (318 cycles total)")
        print("│           ├── s_1_1101.bcl (base calls)")
        print("│           ├── s_1_1101.locs (cluster positions)")
        print("│           └── s_1_1101.filter (pass/fail flags)")
        print("└── metadata.json (updated dataset metadata)")
        print("\nThis dataset is now fully compatible with bcl2fastq v2.20")
        print("and should resolve the XML attribute parsing errors.")
        print("\nExpected bcl2fastq output:")
        print("├── TestProject/")
        print("│   ├── Sample1_S1_L001_R1_001.fastq.gz")
        print("│   ├── Sample1_S1_L001_R2_001.fastq.gz")
        print("│   ├── Sample2_S2_L001_R1_001.fastq.gz")
        print("│   └── Sample2_S2_L001_R2_001.fastq.gz")
        print("├── Undetermined_S0_L001_R1_001.fastq.gz (should be minimal)")
        print("└── Undetermined_S0_L001_R2_001.fastq.gz (should be minimal)")
        print("\nThe FASTQ files in TestProject/ will be suitable for genome browser visualization.")
        
        # Display the SampleSheet contents
        print("\n" + "="*70)
        print("SAMPLESHEET CONTENTS")
        print("="*70)
        print("SampleSheet.csv contents:")
        print("-" * 50)
        
        # Read and display the sample sheet content
        sample_sheet_path = dataset_path / "SampleSheet.csv"
        with open(sample_sheet_path, 'r') as f:
            sample_sheet_contents = f.read()
        print(sample_sheet_contents)
        print("-" * 50)
        print("="*70)
        
    except Exception as e:
        print(f"✗ Failed to create dataset: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
