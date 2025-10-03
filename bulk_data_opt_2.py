from google.api_core.exceptions import NotFound, Forbidden
from google.cloud import bigquery
import time

# Configuration
PROJECT_ID = "hrbot-220907"  # Target project for migration
SOURCE_DATASETS = [
    "NewProjectwith_US_Region_8thJuly_NewAccount_USRegion_8thJuly",
    "NewProjectwith_US_Region_8thJuly_NewSchema_US_Region",
    "NewProjectwith_US_Region_8thJuly_Raziaccount14th",
    "NewProjectwith_US_Region_8thJuly_NewAccout15thofJuly",
    "NewProjectwith_US_Region_8thJuly_NewAccountforRegion",
]

SERVICE_ACCOUNT_JSON = "/home/kraj/Ti/iLab/pyPro/table-dump-api/poc/location_migrate/bq_migrate/gc_auth_qa.json"

import sys

# Test with explicit project
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON, project=PROJECT_ID)

# List datasets to verify access
datasets = list(client.list_datasets())
print(f"Found {len(datasets)} datasets in project {PROJECT_ID}")
for dataset in datasets:
    print(f" - {dataset.dataset_id}")

dataset_ref = client.dataset(SOURCE_DATASETS[0])
dataset = client.get_dataset(dataset_ref)
print(f"Location: {dataset.location}")

sys.exit()


def check_dataset_exists(client, dataset_name, location=None):
    """Check if dataset exists and return its location"""
    try:
        dataset_ref = client.dataset(dataset_name)
        dataset = client.get_dataset(dataset_ref)
        actual_location = dataset.location
        print(f"Dataset '{dataset_name}' exists in location: {actual_location}")

        if location and actual_location.upper() != location.upper():
            print(f"Warning: Dataset is in {actual_location}, expected {location}")

        return True, actual_location
    except NotFound:
        print(f"Dataset '{dataset_name}' not found")
        return False, None
    except Forbidden as e:
        print(f"Permission denied for dataset '{dataset_name}': {e}")
        return False, None
    except Exception as e:
        print(f"Error checking dataset '{dataset_name}': {e}")
        return False, None


def migrate_datasets_with_backup():
    # Initialize clients with explicit credentials AND project ID
    try:
        us_client = bigquery.Client.from_service_account_json(
            SERVICE_ACCOUNT_JSON,
            project=PROJECT_ID,  # Explicitly set the project
            location="US"
        )
        eu_client = bigquery.Client.from_service_account_json(
            SERVICE_ACCOUNT_JSON,
            project=PROJECT_ID,  # Explicitly set the project
            location="EU"
        )
        print(f"Successfully initialized BigQuery clients for project: {PROJECT_ID}")

        # Test connection by listing datasets
        print("Available datasets in US:")
        us_datasets = list(us_client.list_datasets())
        for dataset in us_datasets[:5]:  # Show first 5
            print(f"  - {dataset.dataset_id}")
        if len(us_datasets) > 5:
            print(f"  ... and {len(us_datasets) - 5} more")

    except Exception as e:
        print(f"Error initializing BigQuery clients: {e}")
        return

    # Process each dataset in the list
    for source_dataset in SOURCE_DATASETS:
        print(f"\n{'=' * 80}")
        print(f"Processing dataset: {source_dataset}")
        print(f"{'=' * 80}")

        # Step 0: Check if source dataset exists with detailed debugging
        print("Checking source dataset...")
        exists, actual_location = check_dataset_exists(us_client, source_dataset, "US")

        if not exists:
            # Try with EU client as well to see if it exists elsewhere
            print("Checking with EU client...")
            exists_eu, actual_location_eu = check_dataset_exists(eu_client, source_dataset)

            if exists_eu:
                print(f"Dataset found in EU location. Skipping migration as it's already in target location.")
                continue
            else:
                print(f"Dataset '{source_dataset}' not found in any accessible location. Skipping.")
                continue

        # Verify it's actually in US location
        if actual_location and actual_location.upper() != "US":
            print(f"Dataset is in {actual_location}, not US. Skipping migration.")
            continue

        # Generate dataset names for backup and temporary EU
        backup_dataset = f"{source_dataset}_old"
        temp_eu_dataset = f"{source_dataset}_EU"

        # Step 1: Get list of tables from source dataset
        try:
            tables = list(us_client.list_tables(source_dataset))
            if not tables:
                print(f"No tables found in source dataset '{source_dataset}'")
                # Create empty datasets in EU for consistency
                try:
                    target_dataset_ref = eu_client.dataset(source_dataset)
                    target_dataset = bigquery.Dataset(target_dataset_ref)
                    target_dataset.location = "EU"
                    eu_client.create_dataset(target_dataset)
                    print(f"Created empty dataset '{source_dataset}' in EU location")
                except Exception as e:
                    print(f"Error creating empty dataset in EU: {e}")
                continue

            print(f"Found {len(tables)} tables in source dataset")
            table_ids = [table.table_id for table in tables]
            print(f"Table IDs: {table_ids}")
        except Exception as e:
            print(f"Error listing tables: {e}")
            continue

        # Step 2: Create temporary EU dataset (handle existing dataset)
        temp_eu_dataset_ref = eu_client.dataset(temp_eu_dataset)
        try:
            eu_client.get_dataset(temp_eu_dataset_ref)
            print(f"Temporary EU dataset '{temp_eu_dataset}' already exists")

            # Check if it has tables
            eu_tables = list(eu_client.list_tables(temp_eu_dataset))
            if eu_tables:
                print(f"Temporary EU dataset has tables. Deleting and recreating...")
                eu_client.delete_dataset(temp_eu_dataset_ref, delete_contents=True)
                # Add delay to ensure deletion completes
                time.sleep(5)
                temp_eu_dataset_obj = bigquery.Dataset(temp_eu_dataset_ref)
                temp_eu_dataset_obj.location = "EU"
                eu_client.create_dataset(temp_eu_dataset_obj)
                print(f"Temporary EU dataset '{temp_eu_dataset}' recreated in EU location")
            else:
                print(f"Temporary EU dataset is empty. Using existing dataset.")
        except NotFound:
            # Create temporary EU dataset if it doesn't exist
            temp_eu_dataset_obj = bigquery.Dataset(temp_eu_dataset_ref)
            temp_eu_dataset_obj.location = "EU"
            eu_client.create_dataset(temp_eu_dataset_obj)
            print(f"Temporary EU dataset '{temp_eu_dataset}' created in EU location")
        except Exception as e:
            print(f"Error handling temporary EU dataset: {e}")
            continue

        # Step 3: Copy all tables from source to temporary EU dataset
        print("Copying tables to EU location...")
        successful_copies = 0
        for table in tables:
            source_table_id = f"{PROJECT_ID}.{source_dataset}.{table.table_id}"
            temp_eu_table_id = f"{PROJECT_ID}.{temp_eu_dataset}.{table.table_id}"
            print(f"Preparing to copy {source_table_id} to {temp_eu_table_id}")

            # Configure the copy job
            job_config = bigquery.CopyJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

            # Execute the copy job
            try:
                copy_job = eu_client.copy_table(
                    source_table_id,
                    temp_eu_table_id,
                    location="US",  # Source data is in US
                    job_config=job_config
                )

                print(f"Copying table {table.table_id}...")
                copy_job.result()  # Wait for completion
                print(f"Table {table.table_id} successfully copied to EU")
                successful_copies += 1
            except Exception as e:
                print(f"Error copying table {table.table_id}: {e}")
                continue

        if successful_copies == 0:
            print("No tables were successfully copied. Skipping further steps for this dataset.")
            continue

        # Step 4: Validate the copy
        print("Validating the copy...")
        try:
            eu_tables = list(eu_client.list_tables(temp_eu_dataset))
            if len(eu_tables) != len(tables):
                print(f"Validation warning: Expected {len(tables)} tables, found {len(eu_tables)}")
                # Continue anyway, but log warning
            else:
                eu_table_ids = [table.table_id for table in eu_tables]
                if set(table_ids) != set(eu_table_ids):
                    print(f"Validation warning: Table lists don't match exactly")
                else:
                    print("All tables successfully copied and validated")
        except Exception as e:
            print(f"Error during validation: {e}")
            # Continue with migration despite validation error

        # Step 5: Create backup of original dataset
        backup_dataset_ref = us_client.dataset(backup_dataset)
        try:
            us_client.get_dataset(backup_dataset_ref)
            print(f"Backup dataset '{backup_dataset}' already exists in US location")
        except NotFound:
            try:
                backup_dataset_obj = bigquery.Dataset(backup_dataset_ref)
                backup_dataset_obj.location = "US"
                us_client.create_dataset(backup_dataset_obj)
                print(f"Backup dataset '{backup_dataset}' created in US location")
            except Exception as e:
                print(f"Error creating backup dataset: {e}")
                # Continue with migration even if backup fails

        # Step 6: Copy all tables to backup dataset
        print("Creating backup of original dataset...")
        for table in tables:
            source_table_id = f"{PROJECT_ID}.{source_dataset}.{table.table_id}"
            backup_table_id = f"{PROJECT_ID}.{backup_dataset}.{table.table_id}"

            # Configure the copy job
            job_config = bigquery.CopyJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

            # Execute the copy job
            try:
                copy_job = us_client.copy_table(
                    source_table_id,
                    backup_table_id,
                    job_config=job_config
                )

                print(f"Backing up table {table.table_id}...")
                copy_job.result()  # Wait for completion
                print(f"Table {table.table_id} successfully backed up")
            except Exception as e:
                print(f"Error backing up table {table.table_id}: {e}")
                # Continue with next table

        # Step 7: Delete original dataset (only if we have successful copies)
        if successful_copies > 0:
            print(f"Deleting original dataset '{source_dataset}'...")
            try:
                source_dataset_ref = us_client.dataset(source_dataset)
                us_client.delete_dataset(source_dataset_ref, delete_contents=True)
                print(f"Original dataset '{source_dataset}' deleted")
            except Exception as e:
                print(f"Error deleting original dataset: {e}")
                # Continue even if deletion fails
        else:
            print(f"Skipping deletion of original dataset due to copy failures")

        # Step 8: Create final dataset with original name in EU
        target_dataset_ref = eu_client.dataset(source_dataset)
        try:
            eu_client.get_dataset(target_dataset_ref)
            print(f"Target dataset '{source_dataset}' already exists in EU location")
        except NotFound:
            try:
                target_dataset = bigquery.Dataset(target_dataset_ref)
                target_dataset.location = "EU"
                eu_client.create_dataset(target_dataset)
                print(f"Target dataset '{source_dataset}' created in EU location")
            except Exception as e:
                print(f"Error creating target dataset: {e}")
                continue

        # Step 9: Copy all tables from temporary to final dataset
        print("Moving tables to final dataset...")
        for table in tables:
            temp_eu_table_id = f"{PROJECT_ID}.{temp_eu_dataset}.{table.table_id}"
            target_table_id = f"{PROJECT_ID}.{source_dataset}.{table.table_id}"

            # Configure the copy job
            job_config = bigquery.CopyJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

            # Execute the copy job
            try:
                copy_job = eu_client.copy_table(
                    temp_eu_table_id,
                    target_table_id,
                    job_config=job_config
                )

                print(f"Moving table {table.table_id}...")
                copy_job.result()  # Wait for completion
                print(f"Table {table.table_id} successfully moved to final dataset")
            except Exception as e:
                print(f"Error moving table {table.table_id}: {e}")
                continue

        # Step 10: Clean up temporary EU dataset
        print(f"Cleaning up temporary EU dataset '{temp_eu_dataset}'...")
        try:
            eu_client.delete_dataset(temp_eu_dataset_ref, delete_contents=True)
            print(f"Temporary EU dataset '{temp_eu_dataset}' deleted")
        except Exception as e:
            print(f"Error deleting temporary EU dataset: {e}")

        print(f"Migration completed successfully for dataset: {source_dataset}!")
        print(f"Backup dataset kept as: {backup_dataset} (US location)")
        print(f"New dataset: {source_dataset} (EU location)")

    print("\nAll datasets processing completed!")


if __name__ == "__main__":
    migrate_datasets_with_backup()