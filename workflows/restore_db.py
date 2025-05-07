# restore db workflow

import os
import json
from prefect import flow
from src.utils import create_dump, get_secret, upload_to_s3, file_dl, restore_dump
from typing import Literal


DropDownChoice = Literal["qa", "stage", "prod"]


@flow(name="cbio-restore-flow", log_prints=True)
def restore_db(
    target_env_name: DropDownChoice,
    source_bucket: str,
    sql_dump_path: str,
    output_bucket: str,
    output_path: str,
):
    """Execute database restore and upload to S3.

    Args:
        target_env_name (str): Environment to restore database to (e.g., qa, stage, prod)
        source_bucket (str): S3 bucket name to download from (e.g. cbio-backup-dev)
        sql_dump_path (str): Path to the SQL dump file in the S3 bucket
        output_bucket (str): S3 bucket name to upload to (e.g. cbio-backup-qa)
        output_path (str): Path in the S3 bucket where the output/validation file(s) will be uploaded
    """

    # retrieve and load creds
    creds_string = get_secret(target_env_name)
    creds = json.loads(creds_string)

    # create working directory
    working_dir = "/usr/local/data/dumps"
    os.makedirs(working_dir, exist_ok=True)

    # change to working directory
    os.chdir(working_dir)
    print(f"✅ Changed working directory to: {working_dir}")

    # download the dump file from S3
    file_dl(source_bucket, sql_dump_path)

    #check if the file exists
    file_name = os.path.basename(sql_dump_path)
    if not os.path.exists(file_name):
        raise FileNotFoundError(f"File {file_name} does not exist.")
    
    print(f"✅ Downloaded dump file from S3: {file_name}")

    # restore the database using the dump file
    if restore_dump(dump_file_path=file_name, **creds):
        print(f"✅ Restored database from dump file: {file_name}")
    else:
        print(f"❌ Failed to restore database from dump file: {file_name}")
        raise Exception(f"Failed to restore database from dump file: {file_name}")

    # TODO: validate the database restore

    # TODO: upload the validation file(s) to S3
    
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