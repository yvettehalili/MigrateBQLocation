from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# Configuration
PROJECT_ID = "ti-is-datti-prodenv"
SOURCE_DATASETS = [
    "meta_ai_us_copy_CO_Meta",
    "meta_ai_us_copy_PDO_Meta",
    
]

SERVICE_ACCOUNT_JSON = "/root/jsonfiles/ti-dba-prod-01.json"


def migrate_datasets_with_backup():
    # Initialize clients with explicit credentials
    us_client = bigquery.Client.from_service_account_json(
        SERVICE_ACCOUNT_JSON, location="US"
    )
    eu_client = bigquery.Client.from_service_account_json(
        SERVICE_ACCOUNT_JSON, location="EU"
    )

    # Process each dataset in the list
    for source_dataset in SOURCE_DATASETS:
        print(f"\n{'=' * 80}")
        print(f"Processing dataset: {source_dataset}")
        print(f"{'=' * 80}")

        # Generate dataset names for backup and temporary EU
        backup_dataset = f"{source_dataset}_old"
        temp_eu_dataset = f"{source_dataset}_EU"

        # Step 0: Check if source dataset exists
        try:
            source_dataset_ref = us_client.dataset(source_dataset)
            us_client.get_dataset(source_dataset_ref)
            print(f"Source dataset '{source_dataset}' found in US location")
        except NotFound:
            print(f"Source dataset '{source_dataset}' not found in US location")
            continue  # Skip to next dataset

        # Step 1: Get list of tables from source dataset
        try:
            tables = list(us_client.list_tables(source_dataset))
            if not tables:
                print(f"No tables found in source dataset '{source_dataset}'")
                continue  # Skip to next dataset

            print(f"Found {len(tables)} tables in source dataset")
            table_ids = [table.table_id for table in tables]
            print(f"Table IDs: {table_ids}")
        except Exception as e:
            print(f"Error listing tables: {e}")
            continue  # Skip to next dataset

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

        # Step 3: Copy all tables from source to temporary EU dataset
        print("Copying tables to EU location...")
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
            except Exception as e:
                print(f"Error copying table {table.table_id}: {e}")
                continue  # Skip to next table

        # Step 4: Validate the copy
        print("Validating the copy...")
        try:
            eu_tables = list(eu_client.list_tables(temp_eu_dataset))
            if len(eu_tables) != len(tables):
                print(f"Validation failed: Expected {len(tables)} tables, found {len(eu_tables)}")
                continue  # Skip to next dataset

            eu_table_ids = [table.table_id for table in eu_tables]
            if set(table_ids) != set(eu_table_ids):
                print(f"Validation failed: Table lists don't match")
                continue  # Skip to next dataset

            print("All tables successfully copied and validated")
        except Exception as e:
            print(f"Error during validation: {e}")
            continue  # Skip to next dataset

        # Step 5: Create backup of original dataset
        backup_dataset_ref = us_client.dataset(backup_dataset)
        try:
            us_client.get_dataset(backup_dataset_ref)
            print(f"Backup dataset '{backup_dataset}' already exists in US location")
        except NotFound:
            backup_dataset_obj = bigquery.Dataset(backup_dataset_ref)
            backup_dataset_obj.location = "US"
            us_client.create_dataset(backup_dataset_obj)
            print(f"Backup dataset '{backup_dataset}' created in US location")

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
                continue  # Skip to next table

        # Step 7: Delete original dataset
        print(f"Deleting original dataset '{source_dataset}'...")
        try:
            us_client.delete_dataset(source_dataset_ref, delete_contents=True)
            print(f"Original dataset '{source_dataset}' deleted")
        except Exception as e:
            print(f"Error deleting original dataset: {e}")
            continue  # Skip to next dataset

        # Step 8: Create final dataset with original name in EU
        target_dataset_ref = eu_client.dataset(source_dataset)
        try:
            eu_client.get_dataset(target_dataset_ref)
            print(f"Target dataset '{source_dataset}' already exists in EU location")
        except NotFound:
            target_dataset = bigquery.Dataset(target_dataset_ref)
            target_dataset.location = "EU"
            eu_client.create_dataset(target_dataset)
            print(f"Target dataset '{source_dataset}' created in EU location")

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
                continue  # Skip to next table

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
