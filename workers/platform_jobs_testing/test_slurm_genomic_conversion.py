#!/usr/bin/env python3
"""
Platform-Based Genomic Conversion Test Script

PURPOSE:
    Proof-of-concept test for SLURM-based genomic_conversion workflow.
    Tests end-to-end flow: download → register → stage → convert

WHAT IT DOES:
    1. Downloads iseq-DI dataset to registration location
    2. Waits for watch.py to register it as RAW_DATA
    3. Monitors for dataset to reach STAGED state
    4. Kicks off genomic_conversion workflow with SLURM execution
    5. Exits after success or timeout

REQUIREMENTS:
    - workers/.env file configured with API_BASE_URL and APP_API_TOKEN
    - watch.py running (or run manually after download)
    - SLURM cluster configured in workers/config/production.py
    - Poetry environment activated
    - Access to /N/scratch/cmguser/... paths

USAGE:
    # Basic usage (uses defaults)
    cd /opt/sca/cmg-bioloop/workers
    poetry shell
    python platform_jobs_testing/test_slurm_genomic_conversion.py

    # Skip download if already exists
    python platform_jobs_testing/test_slurm_genomic_conversion.py --skip-download

    # Use custom registration directory
    python platform_jobs_testing/test_slurm_genomic_conversion.py --reg-dir /custom/path

    # Set custom timeout
    python platform_jobs_testing/test_slurm_genomic_conversion.py --timeout 60

    # Use specific conversion definition
    python platform_jobs_testing/test_slurm_genomic_conversion.py --definition-id 5

    # Test without SLURM (local execution)
    python platform_jobs_testing/test_slurm_genomic_conversion.py --no-slurm

    # Dry run (show what would happen)
    python platform_jobs_testing/test_slurm_genomic_conversion.py --dry-run

EXAMPLES:
    # Full test with all defaults
    python platform_jobs_testing/test_slurm_genomic_conversion.py

    # Quick test if dataset already downloaded
    python platform_jobs_testing/test_slurm_genomic_conversion.py --skip-download --timeout 10

    # Test local execution instead of SLURM
    python platform_jobs_testing/test_slurm_genomic_conversion.py --no-slurm

EXPECTED OUTPUT:
    Step 1: Downloads iseq-DI.tar.gz (~541 MB)
    Step 2: Waits for dataset to appear in API (usually < 1 minute)
    Step 3: Waits for dataset to be STAGED (varies, usually 5-15 minutes)
    Step 4: Creates conversion with SLURM execution and shows conversion ID
    
    Final output includes conversion ID, workflow ID, and monitoring commands.

TROUBLESHOOTING:
    - If download fails: Check network connection or use --skip-download
    - If registration hangs: Check that watch.py is running (pm2 list)
    - If staging hangs: Check worker logs (pm2 logs) for errors
    - If conversion fails: Check conversion definition ID is correct

NOTES:
    - Default conversion definition ID is 1 (may need adjustment)
    - Default timeout is 30 minutes per step
    - Script automatically retries API calls with exponential backoff
    - SLURM job script is a simple test job (customize as needed)
"""

import argparse
import os
import sys
import time
import subprocess
from pathlib import Path
from pprint import pprint
from urllib.parse import urljoin
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter, Retry


# ============================================================================
# CONFIGURATION (adjust as needed)
# ============================================================================

DATASET_NAME = "iseq-DI"
DOWNLOAD_URL = "https://cf.10xgenomics.com/supp/spatial-exp/demultiplexing/iseq-DI.tar.gz"
DEFAULT_REGISTRATION_DIR = "/N/scratch/cmguser/cmg-bioloop/origin/raw_data"
DEFAULT_POLL_INTERVAL = 5  # seconds
DEFAULT_TIMEOUT = 30 * 60  # 30 minutes
DEFAULT_DEFINITION_ID = 1  # bcl2fastq - adjust based on your system


# ============================================================================
# API CLIENT (mirrors workers/workers/api.py patterns)
# ============================================================================

class APIClient(requests.Session):
    """Simple API client for testing - similar to workers/api.py."""
    
    def __init__(self, base_url, token):
        super().__init__()
        adapter = HTTPAdapter(max_retries=Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 502, 503]
        ))
        self.mount("http://", adapter)
        self.mount("https://", adapter)
        self.base_url = base_url
        self.token = token
        self.timeout = (5, 30)
    
    def request(self, method, url, *args, **kwargs):
        joined_url = urljoin(self.base_url, url)
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout
        headers = kwargs.pop('headers', {})
        headers['Authorization'] = f'Bearer {self.token}'
        kwargs['headers'] = headers
        return super().request(method, joined_url, *args, **kwargs)


def get_api_client():
    """Initialize API client with credentials from .env."""
    workers_dir = Path(__file__).parent.parent
    load_dotenv(workers_dir / '.env')
    
    api_base_url = os.environ.get('API_BASE_URL')
    api_token = os.environ.get('APP_API_TOKEN')
    
    if not api_base_url or not api_token:
        raise RuntimeError(
            "Missing API credentials. Ensure workers/.env has:\n"
            "  API_BASE_URL=https://...\n"
            "  APP_API_TOKEN=..."
        )
    
    return APIClient(api_base_url, api_token)


# ============================================================================
# CORE FUNCTIONS
# ============================================================================

def download_dataset(reg_dir, skip=False):
    """Download iseq-DI dataset to registration location."""
    print(f"\n{'='*70}")
    print("Step 1: Download Dataset")
    print(f"{'='*70}")
    
    dest_dir = Path(reg_dir) / DATASET_NAME
    tar_file = dest_dir / f"{DATASET_NAME}.tar.gz"
    
    if skip:
        print(f"⊘ Skipping download (--skip-download)")
        if tar_file.exists():
            print(f"✓ File exists: {tar_file}")
        return
    
    print(f"Dataset: {DATASET_NAME}")
    print(f"URL: {DOWNLOAD_URL}")
    print(f"Destination: {dest_dir}")
    
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    if tar_file.exists():
        print(f"✓ File already exists: {tar_file}")
        print(f"  Size: {tar_file.stat().st_size / (1024**2):.1f} MB")
        return
    
    print(f"\nDownloading... (this may take a few minutes)")
    cmd = [
        'curl', '-L', '--fail', '--retry', '3',
        '-o', str(tar_file),
        DOWNLOAD_URL
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print(f"✓ Downloaded: {tar_file}")
        print(f"  Size: {tar_file.stat().st_size / (1024**2):.1f} MB")
    except subprocess.CalledProcessError as e:
        print(f"✗ Download failed: {e}")
        sys.exit(1)


def wait_for_registration(client, timeout, poll_interval):
    """Wait for watch.py to register the dataset."""
    print(f"\n{'='*70}")
    print("Step 2: Wait for Dataset Registration")
    print(f"{'='*70}")
    print(f"Waiting for '{DATASET_NAME}' to appear in API...")
    print(f"Polling every {poll_interval}s (timeout: {timeout/60:.0f} min)")
    print("(watch.py should detect and register it automatically)")
    
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            print(f"\n✗ Timeout after {timeout/60:.0f} minutes")
            print("  Check that watch.py is running: pm2 list")
            sys.exit(1)
        
        try:
            r = client.get('datasets', params={
                'type': 'RAW_DATA',
                'name': DATASET_NAME,
                'match_name_exact': True
            })
            r.raise_for_status()
            datasets = r.json()
            
            if datasets:
                dataset = datasets[0]
                print(f"\n✓ Dataset registered!")
                print(f"  ID: {dataset['id']}")
                print(f"  Name: {dataset['name']}")
                print(f"  Type: {dataset['type']}")
                return dataset
        except Exception as e:
            print(f"  [{elapsed:.0f}s] API error: {e}")
        
        print(f"  [{elapsed:.0f}s] Not found yet...")
        time.sleep(poll_interval)


def wait_for_staged(client, dataset_id, timeout, poll_interval):
    """Wait for dataset to reach STAGED state."""
    print(f"\n{'='*70}")
    print("Step 3: Wait for STAGED State")
    print(f"{'='*70}")
    print(f"Waiting for dataset {dataset_id} to be staged...")
    print(f"Polling every {poll_interval}s (timeout: {timeout/60:.0f} min)")
    
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            print(f"\n✗ Timeout after {timeout/60:.0f} minutes")
            print("  Check worker logs: pm2 logs")
            sys.exit(1)
        
        try:
            r = client.get(f'datasets/{dataset_id}')
            r.raise_for_status()
            dataset = r.json()
            
            is_staged = dataset.get('is_staged', False)
            
            if is_staged:
                print(f"\n✓ Dataset is STAGED!")
                print(f"  Staged path: {dataset.get('staged_path')}")
                return dataset
            else:
                print(f"  [{elapsed:.0f}s] Not staged yet (is_staged={is_staged})")
        except Exception as e:
            print(f"  [{elapsed:.0f}s] API error: {e}")
        
        time.sleep(poll_interval)


def create_conversion(client, dataset_id, definition_id, use_slurm, dry_run):
    """Kick off genomic_conversion workflow."""
    print(f"\n{'='*70}")
    print("Step 4: Start genomic_conversion Workflow")
    print(f"{'='*70}")
    
    payload = {
        'definition_id': definition_id,
        'dataset_id': dataset_id,
        'argument_values': [],  # Use defaults from definition
    }
    
    if use_slurm:
        # SLURM test job script
        job_script = """#!/bin/bash
#SBATCH --job-name=bioloop_test_conversion
#SBATCH --output=bioloop_test_%j.out
#SBATCH --error=bioloop_test_%j.err
#SBATCH --time=01:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=4

echo "=========================================="
echo "Bioloop Platform-Based Conversion Test"
echo "=========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Started: $(date)"
echo ""
echo "This is a test job script."
echo "Actual conversion command would go here."
echo ""
echo "Simulating work for 10 seconds..."
sleep 10
echo ""
echo "Finished: $(date)"
echo "=========================================="
exit 0
"""
        
        payload['process_requests'] = [{
            'execution_platform': 'SLURM',
            'execution_config': {
                'partition': 'general',
                'memory': '8G',
                'cpus': 4,
                'time': '01:00:00'
            },
            'artifacts': [{
                'artifact_type': 'JOB_SCRIPT',
                'storage_type': 'INLINE',
                'content_inline': job_script
            }]
        }]
        print("Execution: SLURM")
    else:
        print("Execution: LOCAL (worker will run directly)")
    
    print(f"Dataset ID: {dataset_id}")
    print(f"Definition ID: {definition_id}")
    
    if dry_run:
        print("\n⊘ DRY RUN - Would send payload:")
        pprint(payload, indent=2)
        print("\n✓ Dry run complete (no API call made)")
        return
    
    print("\nCreating conversion...")
    try:
        r = client.post('conversions', json=payload)
        r.raise_for_status()
        conversion = r.json()
        
        print(f"\n✓ Conversion created!")
        print(f"  Conversion ID: {conversion['id']}")
        print(f"  Workflow ID: {conversion.get('workflow_id', 'N/A')}")
        
        print(f"\n{'='*70}")
        print("✓ TEST COMPLETE")
        print(f"{'='*70}")
        print("\nNext steps:")
        print("  1. Monitor worker logs:")
        print("     pm2 logs conversion")
        if use_slurm:
            print("  2. Check SLURM queue:")
            print("     squeue -u cmguser")
            print("  3. Check SLURM job output:")
            print("     cat /tmp/slurm_jobs/bioloop_test_*.out")
        print(f"  4. Check conversion status via API:")
        print(f"     curl -H 'Authorization: Bearer $TOKEN' \\")
        print(f"          $API_BASE_URL/conversions/{conversion['id']}")
        
        return conversion
        
    except Exception as e:
        print(f"\n✗ Failed to create conversion: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        sys.exit(1)


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Test platform-based genomic_conversion workflow',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic test
  python test_slurm_genomic_conversion.py
  
  # Skip download if already exists
  python test_slurm_genomic_conversion.py --skip-download
  
  # Test local execution instead of SLURM
  python test_slurm_genomic_conversion.py --no-slurm
  
  # Dry run (show what would happen)
  python test_slurm_genomic_conversion.py --dry-run
"""
    )
    
    parser.add_argument(
        '--reg-dir',
        default=DEFAULT_REGISTRATION_DIR,
        help=f'Registration directory (default: {DEFAULT_REGISTRATION_DIR})'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f'Timeout in seconds per step (default: {DEFAULT_TIMEOUT}s = {DEFAULT_TIMEOUT//60}min)'
    )
    parser.add_argument(
        '--poll-interval',
        type=int,
        default=DEFAULT_POLL_INTERVAL,
        help=f'Polling interval in seconds (default: {DEFAULT_POLL_INTERVAL}s)'
    )
    parser.add_argument(
        '--definition-id',
        type=int,
        default=DEFAULT_DEFINITION_ID,
        help=f'Conversion definition ID (default: {DEFAULT_DEFINITION_ID})'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip download step (use if dataset already exists)'
    )
    parser.add_argument(
        '--no-slurm',
        action='store_true',
        help='Use local execution instead of SLURM'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would happen without making API calls'
    )
    
    args = parser.parse_args()
    
    print("="*70)
    print("Platform-Based Genomic Conversion Test")
    print("="*70)
    print(f"Dataset: {DATASET_NAME}")
    print(f"Registration dir: {args.reg_dir}")
    print(f"Timeout: {args.timeout//60} minutes per step")
    print(f"Execution: {'LOCAL' if args.no_slurm else 'SLURM'}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    
    try:
        # Initialize API client
        client = get_api_client()
        
        # Step 1: Download dataset
        download_dataset(args.reg_dir, args.skip_download)
        
        # Step 2: Wait for registration
        dataset = wait_for_registration(client, args.timeout, args.poll_interval)
        
        # Step 3: Wait for STAGED state
        dataset = wait_for_staged(client, dataset['id'], args.timeout, args.poll_interval)
        
        # Step 4: Create conversion
        create_conversion(
            client,
            dataset['id'],
            args.definition_id,
            use_slurm=not args.no_slurm,
            dry_run=args.dry_run
        )
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n✗ Interrupted by user")
        return 1
    except Exception as e:
        print(f"\n\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

