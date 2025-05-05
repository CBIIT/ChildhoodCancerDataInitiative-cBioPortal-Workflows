import json
import requests
import os
import sys
import pandas as pd
import shutil
from prefect_shell import ShellOperation
from prefect.task_runners import ConcurrentTaskRunner
from typing import Literal
from functools import partial

# prefect dependencies
import boto3
from botocore.exceptions import ClientError
from prefect import flow, task, get_run_logger, runtime
from src.utils import get_time, file_dl, folder_ul## workflow for transforming and formatting cnv data


# task to read in manifest file to dataframe and check columns
@task(name="read_manifest", log_prints=True)
def read_manifest(manifest_name: str) -> pd.DataFrame:
    """Read in manifest file to dataframe and check columns

    Args:
        manifest_name (str): Path to the manifest file

    Returns:
        pd.DataFrame: Dataframe of the manifest file
    """
    # read in manifest file
    manifest_df = pd.read_csv(manifest_name, sep="\t")

    # check if required columns are present
    required_columns = ["sample_id", "s3_url", "file_name", "md5sum", "file_size"]
    for col in required_columns:
        if col not in manifest_df.columns:
            raise ValueError(f"Missing required column: {col}")
    # check if there are any missing values in required columns
    for col in required_columns:
        if manifest_df[col].isnull().any():
            raise ValueError(f"Missing values in required column: {col}")
    # check if there are any duplicate sample_ids
    if manifest_df["sample_id"].duplicated().any():
        raise ValueError("Duplicate sample_ids found in manifest file")
    # check if there are any duplicate file_names
    if manifest_df["file_name"].duplicated().any():
        raise ValueError("Duplicate file_names found in manifest file")
    # check if there are any duplicate s3_urls
    if manifest_df["s3_url"].duplicated().any():
        raise ValueError("Duplicate s3_urls found in manifest file")
    # check if there are any duplicate md5sums
    if manifest_df["md5sum"].duplicated().any():
        raise ValueError("Duplicate md5sums found in manifest file")
    
    return manifest_df


# task to download cnv files from S3
@task(
    name="cnv-json-downloader",
    task_run_name="cnv-json-downloader-{filename}", 
    log_prints=True,
    tags=["cnv-json-downloader-tag"],
    retries=3,
    retry_delay_seconds=0.5,
)
def json_dl(dl_parameter: dict, logger, runner_logger):
    """Download cnv files from S3

    Args:
        dl_parameter (dict): Dictionary of parameters for downloading the file
        logger: Logger object 
        runner_logger: Prefect logger object
    """
    # Set the s3 resource object for local or remote execution
    region_name = "us-east-1"
    bucket = dl_parameter['s3_url'].split("/")[2]
    file_path = "/".join(dl_parameter['s3_url'].split("/")[3:])
    s3 = boto3.client("s3", region_name=region_name)
    source = s3.Bucket(bucket)
    file_key = file_path
    row = dl_parameter['row']
    filename = dl_parameter['file_name']
    try:
        source.download_file(file_key, filename)
        
    except ClientError as ex:
        ex_code = ex.response["Error"]["Code"]
        ex_message = ex.response["Error"]["Message"]
        runner_logger.error(
            f"ClientError occurred while downloading file {filename} from bucket {bucket}:\n{ex_code}, {ex_message}"
        )
        logger.error(f"ClientError occurred while downloading file {filename} from bucket {bucket}:\n{ex_code}, {ex_message}")
        raise



# flow to download cnv data from S3 and verify md5 checksum
# use concurrency pool to download multiple files in parallel
@flow(name="download-cnv-flow", task_runner=ConcurrentTaskRunner(), log_prints=True)
def download_cnv(manifest_df: pd.DataFrame, bucket: str) -> None:
    """Download cnv files from S3 and verify md5 checksum

    Args:
        manifest_df (pd.DataFrame): Dataframe of the manifest file
        bucket (str): S3 bucket name
    """
    # download cnv files from S3
    for index, row in manifest_df.iterrows():
        sample_id = row["sample_id"]
        s3_url = row["s3_url"]
        file_name = row["file_name"]
        md5sum = row["md5sum"]
        file_size = row["file_size"]

        # download file
        file_dl(bucket, s3_url)

        # verify md5 checksum
        if not verify_md5(file_name, md5sum):
            raise ValueError(f"MD5 checksum failed for file: {file_name}")

        # check file size
        if os.path.getsize(file_name) != file_size:
            raise ValueError(f"File size mismatch for file: {file_name}")



# task to read in and transform cnv file's data
# parse segments with significant p-value and add to a new dataframe


# task to perform gene mappings to segement location for continuous data
# and create new dataframe with gene names and their corresponding log2 ratios



DropDownChoices = Literal["segment", "cnv_gene", "segment_cnv_gene", "cleanup"]

#main flow to orchestrate the tasks
@flow(name="cbio-cnv-flow")
def cnv_flow(bucket: str, manifest_path: str, output_path: str, flow_type: DropDownChoices):
    """_summary_

    Args:
        bucket (str): S3 bucket name
        manifest_path (str): Path to the manifest file in specified S3 bucket
        output_path (str): Output path at specified S3 bucket for the transformed data
        flow_type (DropDownChoices): Type of flow to run. Options are "segment", "cnv-gene", or "cleanup"
    """

    runner_logger = get_run_logger()
    runner_logger.info(f"Running cnv_flow with bucket: {bucket}, manifest_path: {manifest_path}, output_path: {output_path}, flow_type: {flow_type}")

    # download manifest file from S3
    runner_logger.info(f"Downloading manifest file from S3 bucket")
    file_dl(bucket, manifest_path)

    # read in manifest file
    runner_logger.info(f"Reading in manifest file")
    manifest_df = read_manifest(manifest_path)

    # download cnv files from S3
    runner_logger.info(f"Downloading cnv files from S3 bucket")
    download_cnv(manifest_df, bucket)

if __name__ == "__main__":
    # testing
    cnv_flow()
