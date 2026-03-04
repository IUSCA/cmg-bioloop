import shutil
import tempfile
from pathlib import Path
from pprint import pprint

from celery import Celery

import workers.api as api
import workers.config.celeryconfig as celeryconfig
from workers import cmd
from workers.config import config
from workers.conversion import get_conversion_output_dir
from workers.exceptions import ConversionException
from workers.executors.slurm import SlurmExecutor

app = Celery("tasks")
app.config_from_object(celeryconfig)


def get_sample_sheet_content(arguments: list) -> str:
    """Extract sample sheet content from flattened arguments list."""
    for i, arg in enumerate(arguments):
        if arg == '--sample-sheet' and i + 1 < len(arguments):
            return arguments[i + 1]
    return None


def has_sample_sheet(arguments: list) -> bool:
    """Check if sample sheet argument exists in flattened arguments list."""
    return '--sample-sheet' in arguments


def write_sample_sheet(arguments: list, dataset: dict) -> None:
    dataset_staged_path = Path(dataset['staged_path'])
    sample_sheet_path = dataset_staged_path / f'{dataset["id"]}_samplesheet.csv'
    with open(sample_sheet_path, 'w') as f:
        f.write(get_sample_sheet_content(arguments=arguments))


def get_program_args(arguments: list,
                     dataset: dict,
                     conversion_output_dir: Path) -> list:
    # Flags that are not supported by bcl2fastq v2.20.0.422
    UNSUPPORTED_FLAGS = {'--delete-undetermined'}
    
    processed_args = []
    
    i = 0
    while i < len(arguments):
        arg = arguments[i]
        
        # Skip None values
        if arg is None:
            i += 1
            continue
        
        # Skip unsupported flags
        if arg in UNSUPPORTED_FLAGS:
            i += 1
            continue
        
        # Only process sample sheets for genomic conversions
        if (arg == '--sample-sheet' and i + 1 < len(arguments)):
            processed_args.append(arg)        

            # Replace the sample sheet content with the path to the written sample sheet file
            dataset_staged_path = Path(dataset['staged_path'])
            sample_sheet_path = dataset_staged_path / f'{dataset["id"]}_samplesheet.csv'
            processed_args.append(str(sample_sheet_path))            
            i += 2  # +2 to skip next element, which is the sample sheet content
        # Check if this is a flag with a None value - skip both flag and value
        elif i + 1 < len(arguments) and arguments[i + 1] is None and arg.startswith('--'):
            i += 2  # Skip both the flag and the None value
        else:
            processed_args.append(arg)
            i += 1
    
    # Add required arguments for genomic conversions
    processed_args.extend([
        '--runfolder-dir', str(dataset['staged_path']),
        '--output-dir', str(conversion_output_dir)
    ])

    return processed_args


def run_conversion(celery_task, conversion_id, **kwargs):
    import logging
    logger = logging.getLogger(__name__)
    
    conversion = api.get_conversion(conversion_id=conversion_id,
                                    include_dataset=True,
                                    include_definition=True)

    print(f"conversion:")
    pprint(conversion, indent=4)

    # Validate process_requests count
    process_requests = conversion.get('requested_processes', [])
    if len(process_requests) > 1:
        raise ConversionException(f"Expected 0 or 1 process requests, got {len(process_requests)}")

    process_request = process_requests[0] if process_requests else None
    
    if process_request:
        logger.info(f"[SLURM-CONVERSION] Process request found for conversion {conversion_id}")
        logger.info(f"[SLURM-CONVERSION] Execution platform: {process_request['execution_platform']}")
        logger.info(f"[SLURM-CONVERSION] Process request ID: {process_request['id']}")
    else:
        logger.info(f"[SLURM-CONVERSION] No process request - running locally for conversion {conversion_id}")

    # Common preparation for both local and platform execution
    dataset_id = conversion['dataset_id']
    conversion_id = conversion['id']
    argsList = conversion['argsList']

    definition_details = api.get_conversion_definition(definition_id=conversion['definition_id'])

    program = definition_details['program']
    print(f"program:")
    pprint(program, indent=4)

    # Get full dataset information to access staged_path
    dataset = api.get_dataset(dataset_id=dataset_id)

    if not dataset['is_staged']:
        raise ConversionException(f"Dataset {dataset_id} is not staged")

    conversion_output_dir = get_conversion_output_dir(conversion=conversion)
    conversion_output_dir.mkdir(parents=True, exist_ok=True)

    # If Dataset being converted has a sample sheet, write it to the Dataset's staged directory
    if has_sample_sheet(arguments=argsList):
        write_sample_sheet(arguments=argsList, dataset=dataset)

    cwd_str = program['executable_directory']
    if cwd_str:
        cwd = Path(cwd_str).resolve()
        if not cwd.exists():
            raise ConversionException(f"Executable directory {cwd} does not exist")
        executable_path = cwd / program['executable_path']
    else:
        cwd = None
        executable_path = Path(program['executable_path']).resolve()

    if not executable_path.exists():
        raise ConversionException(f"Executable {executable_path} does not exist")
    if not executable_path.is_file():
        raise ConversionException(f"Executable {executable_path} is not a file")

    args = [program['executable_path']] + get_program_args(
        arguments=argsList,
        dataset=dataset,
        conversion_output_dir=conversion_output_dir
    )

    print(f"conversion_output_dir: {conversion_output_dir}")
    print("--------------------------------")
    print("contents of conversion_output_dir:")
    for item in conversion_output_dir.iterdir():
        print(f"  {item}")
    print("--------------------------------")

    print(f"args: {args}")
    print("args (joined): " + " ".join(str(a) for a in args))

    # Execute locally or via platform
    platform_based_execution_enabled = config.get('enabled_features', {}).get('platform_based_execution', False)
    if process_request is None or not platform_based_execution_enabled:
        # Run locally
        print("No process requests - running locally")
        logger.info(f"[SLURM-CONVERSION] Executing locally (no platform specified)")
        print("DEBUG: Capturing logs: ", definition_details.get('capture_logs'))
        if definition_details.get('capture_logs', False):
            print("DEBUG: Capturing logs")
            logger.info(f"[SLURM-CONVERSION] Running with log tracking enabled")
            cmd.execute_with_log_tracking(cmd=args, celery_task=celery_task, cwd=str(cwd) if cwd else None)
        else:
            print("DEBUG: Not capturing logs")
            logger.info(f"[SLURM-CONVERSION] Running without log tracking")
            cmd.execute(cmd=args, cwd=str(cwd) if cwd else None)
    else:
        # Run via platform
        process_request_id = process_request['id']
        execution_platform = process_request['execution_platform']

        print(f"Found 1 process request - submitting to {execution_platform}")
        logger.info(f"[SLURM-CONVERSION] Found process request - submitting to {execution_platform}")

        if execution_platform == 'SLURM':
            logger.info(f"[SLURM-CONVERSION] Starting SLURM submission for process_request {process_request_id}")
            
            # Get SLURM config from application config
            slurm_config = config.get('execution_platform', {}).get('SLURM', {})
            logger.info(f"[SLURM-CONVERSION] SLURM host: {slurm_config['connection']['host']}")
            logger.info(f"[SLURM-CONVERSION] SLURM user: {slurm_config['connection']['user']}")
            logger.info(f"[SLURM-CONVERSION] Remote work dir: {slurm_config.get('remote_work_dir', '/tmp/slurm_jobs')}")
            
            executor_config = {
                'host': slurm_config['connection']['host'],
                'ssh_user': slurm_config['connection']['user'],
                'remote_work_dir': slurm_config.get('remote_work_dir', '/tmp/slurm_jobs'),
                'ssh_key_path': slurm_config['connection'].get('private_key'),
            }

            # Create SLURM executor
            logger.info(f"[SLURM-CONVERSION] Creating SlurmExecutor instance")
            executor = SlurmExecutor(config=executor_config, process_request_id=process_request_id)

            # Submit job - executor fetches artifacts and submits to SLURM
            logger.info(f"[SLURM-CONVERSION] Submitting job to SLURM...")
            slurm_job_id = executor.submit_job()

            print(f"Submitted SLURM job {slurm_job_id} for process_request {process_request_id}")
            logger.info(f"[SLURM-CONVERSION] ✓ SLURM job submitted successfully!")
            logger.info(f"[SLURM-CONVERSION] SLURM job ID: {slurm_job_id}")
            logger.info(f"[SLURM-CONVERSION] Process request ID: {process_request_id}")
            logger.info(f"[SLURM-CONVERSION] Conversion ID: {conversion_id}")

            # TODO: store slurm_job_id in worker_process table or process_request table
        else:
            logger.error(f"[SLURM-CONVERSION] Unsupported execution platform: {execution_platform}")
            raise ConversionException(f"Execution platform {execution_platform} not implemented")

    print(f"conversion_output_dir: {conversion_output_dir}")
    print("--------------------------------")
    print("contents of conversion_output_dir:")
    for item in conversion_output_dir.iterdir():
        print(f"  {item}")
    print("--------------------------------")

    print(f"task convert returned dataset_id, conversion_id")
    print(f"dataset_id: {dataset_id}")
    print(f"conversion_id: {conversion_id}")
    return {'dataset_id': dataset_id, 'conversion_id': conversion_id},

