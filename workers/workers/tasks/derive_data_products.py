from __future__ import annotations

import os
import pprint
from pathlib import Path

import requests
from celery import Celery
from sca_rhythm import Workflow
from this import d

import workers.api as api
import workers.config.celeryconfig as celeryconfig
import workers.workflow_utils as wf_utils
from workers.celery_app import app as celery_app
from workers.config import config
from workers.conversion import get_conversion_output_dir

app = Celery("tasks")
app.config_from_object(celeryconfig)


def get_fastq_files_directories(conversion_output_dir: Path,
                              conversion_id: int,
                              dataset_name: str) -> list[Path]:
    # Check if there are any *.fastq.gz files at the root of conversion output directory
    root_fastq_files = list(conversion_output_dir.glob("*.fastq.gz"))
    print(f"root_fastq_files: {root_fastq_files}")

    output_directories: list[Path] = []
    if root_fastq_files:
        print(f"Found file {root_fastq_files} at the root of conversion output directory")
        # Create a new directory for the fastq files
        new_dir_name = f"Conversion-{conversion_id}-{dataset_name}"
        data_product_path = conversion_output_dir / new_dir_name
        data_product_path.mkdir(exist_ok=True)
        
        print(f"Found {len(root_fastq_files)} *.fastq.gz files, moving them to {new_dir_name}")
        
        # Move all fastq files to the new directory
        for fastq_file in root_fastq_files:
            destination = data_product_path / fastq_file.name
            print(f"  - Moving {fastq_file.name} to {new_dir_name}/")
            fastq_file.rename(destination)
        
        # Add the new directory to output_directories
        output_directories.append(data_product_path)
        print(f"  - Added new directory: {new_dir_name}")
    else:
        # Fall back to existing logic for directories with '_' in their names
        for item in conversion_output_dir.iterdir():
            item_path: Path = conversion_output_dir / item
            print(f"item_path: {item_path}")
            if item_path.is_dir() and '_' in item_path.name:
                print(f"  - Appending: {item_path.name}")
                output_directories.append(item_path)
    
    return output_directories


def get_product_track_files(product_id: int) -> list[int]:
        """Get files for a single product that match the name pattern."""
        product_details: dict = api.get_dataset(product_id, files=True)
        files: list[dict] = product_details.get('files', [])
        
        # Filter files that match the name pattern and create payload format
        matching_files_ids: list[dict] = [
            {'id': file['id']} 
            for file in files 
            if file['name'].endswith('bam') or file['name'].endswith('bw') or file['name'].endswith('vcf') or file['name'].endswith('bigwig')
        ]
        
        print(f"Found {len(matching_files_ids)} matching files for product {product_id}")
        return matching_files_ids
    

# def create_tracks_for_data_products(data_products: list[dict]) -> None:
#     """
#     Create tracks for all data products by finding files that match a certain name pattern.
#     Avoids nested loops by using list comprehensions and functional programming.
    
#     @param data_products: List of data product dictionaries with 'id' field
#     @param file_name_pattern: Pattern to match in file names (default: '_')
#     """
#     # Process all data products and create tracks
#     for data_product in data_products:
#         product_id = data_product['id']
#         # Todo: avoid nested loop
#         track_file_ids = get_product_track_files(product_id)
        
#         if track_file_ids:
#             api.create_tracks(product_id, track_file_ids)
#             print(f"Associated {len(track_file_ids)} files as tracks for data product {product_id}")
#         else:
#             print(f"No matching files found for data product {product_id}")


def derive_data_products(celery_task, dataset_id: int, conversion_id: int):
    """
    Create data product datasets from conversion output directories.
    
    For all directories inside conversion_output_dir, a new data product dataset
    is created using the POST /datasets/bulk API.
    
    @param celery_task: WorkflowTask
    @param dataset_id: (int): Source dataset ID
    @param conversion_id: (int): Conversion ID
    @return: tuple (dataset_id, conversion_id)
    """
    # Get conversion information
    conversion = api.get_conversion(conversion_id=conversion_id, include_dataset=True)
    dataset = api.get_dataset(dataset_id=dataset_id)
    
    conversion_output_dir = get_conversion_output_dir(conversion)
    print(f"conversion_output_dir: {conversion_output_dir}")

    # Find all directories inside this conversion's output directory
    output_directories: list[Path] = []
    
    output_directories = get_fastq_files_directories(conversion_output_dir, conversion_id, dataset['name'])
    
    if not output_directories:
        print("No output directories found - no data products to create")
        return {'dataset_id': dataset['id'], 'conversion_id': conversion['id']},
    
    print(f"Found {len(output_directories)} output directories to create as data products")
    print("Output directories:")
    for output_dir in output_directories:
        print(f"  - {output_dir.name}")

    # Prepare datasets for bulk creation
    data_products_to_create: list[dict] = []
    for output_dir in output_directories:
        # Todo: later, this will need to be set to the directory (generated by the conversion program)'s name.
        #       This is just a temporary fix to get the data products created, since the bcl2fastq process doesn't
        #       seem to be creating the Data Product directories in the output directory.
        # data_product_name = dataset['name'] + f"__{conversion['id']}" + f"__{output_dir.name}"
                
        product_payload = {
            "name": output_dir.name,
            "type": "DATA_PRODUCT",
            "origin_path": str(output_dir),
        }
        data_products_to_create.append(product_payload)
    
    # Create all data products using bulk API
    print(f"Creating {len(data_products_to_create)} data products via bulk API...")
    result = api.bulk_create_datasets(data_products_to_create)
    
    # print(f"Bulk creation result: {result}")
    
    # Log results
    if result.get('created'):
        print(f"Successfully created {len(result['created'])} data products")
        for created_dataset in result['created']:
            print(f"  - Created: {created_dataset['name']} (ID: {created_dataset['id']})")
    
    if result.get('conflicted'):
        print(f"Found {len(result['conflicted'])} conflicted datasets (already exist)")
        for conflicted in result['conflicted']:
            print(f"  - Conflicted: {conflicted['name']}")
    
    if result.get('errored'):
        print(f"Found {len(result['errored'])} errored datasets")
        for errored in result['errored']:
            print(f"  - Errored: {errored['name']}")
    
    # result['created'] is a list of datasets that were created:
    # [
    #   {
    #     'id': 1,
    #    'name': 'data_product_1',
    #    'type': 'DATA_PRODUCT',
    #    'origin_path': '/path/to/data_product_1'
    #   ...
    # {
    #     'id': 2,
    #    'name': 'data_product_2',
    #    'type': 'DATA_PRODUCT',
    #    'origin_path': '/path/to/data_product_2'
    #   ...
    # ]
    created_data_products: list[dict] = result['created']
    
    # result['conflicted'] is a list of datasets that were conflicted:
    # [
    #   {
    #    'name': 'data_product_1',
    #    'type': 'DATA_PRODUCT'
    #   },
    #   {
    #    'name': 'data_product_2',
    #    'type': 'DATA_PRODUCT'
    #   }
    # ]
    conflicting_data_products: list[dict] = []
    if result.get('conflicted'):
        print(f"Fetching details for {len(result['conflicted'])} conflicted datasets...")
        # Gather details of conflicted datasets
        for conflicted in result['conflicted']:
            conflicting_data_product_matches = api.get_all_datasets(
                name=conflicted['name'],
                dataset_type=conflicted['type'],
                match_name_exact=True,
            )
            if len(conflicting_data_product_matches) > 0:
                data_product = conflicting_data_product_matches[0]
                conflicting_data_products.append(data_product)
                print(f"  - Found conflicted dataset: {data_product['name']} (ID: {data_product['id']})")

    # Create a combined list of all datasets (created + conflicting). Filter out duplicates.
    derived_data_products: list[dict] = []
    seen_ids = set()
    
    # Add created datasets first
    for data_product in created_data_products:
        if data_product['id'] not in seen_ids:
            derived_data_products.append(data_product)
            seen_ids.add(data_product['id'])
    
    # Add conflicting datasets, avoiding duplicates
    for data_product in conflicting_data_products:
        if data_product['id'] not in seen_ids:
            derived_data_products.append(data_product)
            seen_ids.add(data_product['id'])
        
    # Create hierarchy relationships for all data products (created + conflicted)
    if derived_data_products:
        dataset_hierarchy_data: list[dict] = []
        conversion_derived_dataset_associations: list[dict] = []
        
        for data_product in derived_data_products:
            dataset_hierarchy_data.append({
                "source_id": dataset['id'],
                "derived_id": data_product['id']
            })
            conversion_derived_dataset_associations.append({
                "conversion_id": conversion['id'],
                "dataset_id": data_product['id']
            })

        # print("dataset_hierarchy_data: ")
        # pprint.pprint(dataset_hierarchy_data)
        # print("conversion_derived_dataset_associations: ")
        # pprint.pprint(conversion_derived_dataset_associations)
        print(f"Created {len(dataset_hierarchy_data)} dataset hierarchies...")
        try:
            api.create_dataset_hierarchy(dataset_hierarchy_data)
            print("Dataset hierarchies created successfully")
        except requests.exceptions.HTTPError as e:
            print(f"Error creating dataset hierarchies: {e}")
            if e.response.status_code == 409:
                print(f"Conflict creating dataset hierarchies: {e}")
                print(f"Skipping creation of dataset hierarchies")
            else:
                raise

        print(f"Creating {len(conversion_derived_dataset_associations)} conversion's derived-dataset associations...")
        try:
            api.post_conversion_derived_datasets(conversion_derived_dataset_associations)
            print("Conversion's derived-dataset associations created successfully")
        except requests.exceptions.HTTPError as e:
            print(f"Error creating conversion's derived-dataset associations: {e}")
            if e.response.status_code == 409:
                print(f"Conflict creating conversion's derived-dataset associations: {e}")
                print(f"Skipping creation of conversion's derived-dataset associations")
            else:
                raise
        
        # Associate files as tracks for all data products
        # create_tracks_for_data_products(derived_data_products)

    # Kick off 'Integrated' workflow for all data products
    for data_product in derived_data_products:
        wf = Workflow(celery_app=celery_app, **wf_utils.get_wf_body(wf_name='integrated'))
        wf.start(data_product['id'])
        print(f"Started workflow {wf} for data product {data_product['id']}")
        api.add_workflow_to_dataset(dataset_id=data_product['id'], workflow_id=wf.workflow['_id'])


def derive(celery_task, dataset_id_conversion_id, **kwargs):
    dataset_id = dataset_id_conversion_id['dataset_id']
    conversion_id = dataset_id_conversion_id['conversion_id']
    derive_data_products(celery_task, dataset_id, conversion_id)

    return {'dataset_id': dataset_id, 'conversion_id': conversion_id},

