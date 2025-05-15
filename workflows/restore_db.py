# restore db workflow

import os
import json
from prefect import flow
from src.utils import get_secret, upload_to_s3, file_dl, db_counter, get_time
from typing import Literal

DropDownRunChoice = Literal["restore", "clear_working_dir"]
DropDownEnvChoice = Literal["dev", "qa", "stage", "prod"]

@flow(name="cbio-restore-flow", log_prints=True)
def restore_db(
    run_type: DropDownRunChoice,
    target_env_name: DropDownEnvChoice,
    source_bucket: str,
    sql_dump_path: str,
    output_bucket: str,
    output_path: str,
):
    """Execute database restore and upload to S3.

    Args:
        run_type (str): Type of run for workflow (i.e. restore db + validate restore, or clear working dir)
        target_env_name (str): Environment to restore database to (e.g., qa, stage, prod)
        source_bucket (str): S3 bucket name to download from (e.g. cbio-backup-dev)
        sql_dump_path (str): Path to the SQL dump file in the S3 bucket
        output_bucket (str): S3 bucket name to upload to (e.g. cbio-backup-qa)
        output_path (str): Path in the S3 bucket where the output/validation file(s) will be uploaded
    """

    # set working directory
    working_dir = "/usr/local/data/dumps"

    if run_type == "clear_working_dir":

        # change to working directory
        os.chdir(working_dir)

        # remove any files in the working directory
        for file in os.listdir(working_dir):
            file_path = os.path.join(working_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"✅ Removed file: {file_path}")
            elif os.path.isdir(file_path):
                os.rmdir(file_path)
                print(f"✅ Removed directory: {file_path}")
        print(f"✅ Removed all files in working directory: {working_dir}")
    
    else:

        # grab creds from secrets manager
        creds_string = get_secret(target_env_name)
        creds = json.loads(creds_string)

        # create working directory
        os.makedirs(working_dir, exist_ok=True)

        # change to working directory
        os.chdir(working_dir)
        print(f"✅ Changed working directory to: {working_dir}")

        # download the dump file from S3
        file_dl(source_bucket, sql_dump_path)

        #check if the file exists
        dump_file_name = os.path.basename(sql_dump_path)
        if not os.path.exists(dump_file_name):
            raise FileNotFoundError(f"File {dump_file_name} does not exist.")
        
        print(f"✅ Downloaded dump file from S3: {dump_file_name}")

        # restore the database using the dump file
        ## COMMENTED OUT UNTIL DB IS READY
        """if restore_dump(dump_file_path=file_name, **creds):
            print(f"✅ Restored database from dump file: {file_name}")
        else:
            print(f"❌ Failed to restore database from dump file: {file_name}")
            raise Exception(f"Failed to restore database from dump file: {file_name}")"""

        # perform row and col counts on the dump database file
        dump_counts = db_counter("dump", dump_file=dump_file_name)

        print(f"✅ Dump db counts:")
        print(dump_counts)

        # perform row and col counts on the restored database
        # TESTING: performing against dev database until we have a qa database
        restore_counts = db_counter(target_env_name)

        print(f"✅ Restore db counts:")
        print(restore_counts)

        # combined the two counts into a single dataframe and check if row and col counts match
        combined_counts = dump_counts.merge(restore_counts, on="table_name", suffixes=("_dump", f"_{target_env_name}"))
        combined_counts["columm_count_match"] = combined_counts["column_count_dump"] == combined_counts[f"column_count_{target_env_name}"]
        combined_counts["row_count_match"] = combined_counts["row_count_dump"] == combined_counts[f"row_count_{target_env_name}"]
        
        print(f"✅ Combined counts:")
        print(combined_counts)

        # save validation file and upload the validation file(s) to S3
        validation_report_fname = f"{working_dir}/cbio_restore_database_{target_env_name}_{get_time()}.tsv"
        combined_counts.to_csv(validation_report_fname, sep="\t", index=False)

        upload_to_s3(validation_report_fname, output_bucket, output_path="restore_validations")
        
        # remove any files in the working directory
        for f in os.listdir(working_dir):
            file_path = os.path.join(working_dir, f)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"✅ Removed file: {file_path}")
            elif os.path.isdir(file_path):
                os.rmdir(file_path)
                print(f"✅ Removed directory: {file_path}")
        print(f"✅ Removed all files in working directory: {working_dir}")