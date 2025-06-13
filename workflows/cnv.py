import json
import os
import time
import pandas as pd
import hashlib
import shutil
from prefect_shell import ShellOperation
from prefect.task_runners import ConcurrentTaskRunner
from typing import Literal
import boto3
from botocore.exceptions import ClientError
from prefect import flow, task, get_run_logger, unmapped
from src.utils import get_time, file_dl, get_logger, upload_folder_to_s3, set_s3_resource


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

# task to calculate md5sum of file
def get_md5(file_path):
    """Get md5sum of file

    Args:
        file_path (str): Path to the file

    Returns:
        str: md5sum of the file
    """
    # Create a hash object for MD5
    md5_hash = hashlib.md5()

    # Open the file in binary mode
    with open(file_path, "rb") as file:
        # Read the file in chunks to avoid memory issues with large files
        for byte_block in iter(lambda: file.read(4096), b""):
            md5_hash.update(byte_block)

    # Return the hexadecimal digest of the file's MD5 checksum
    return md5_hash.hexdigest()


# task to download cnv files from S3
@task(
    name="cnv-json-downloader",
    task_run_name="cnv-json-downloader_{dl_parameter[file_name]}", 
    log_prints=True,
    tags=["cnv-json-downloader-tag"],
    retries=3,
    retry_delay_seconds=1,
)
def json_dl(dl_parameter: dict, logger, runner_logger):
    """Download cnv files from S3

    Args:
        dl_parameter (dict): Dictionary of parameters for downloading the file
        logger: Logger object 
        runner_logger: Prefect logger object
    """
    # Set the s3 resource object for local or remote execution
    bucket = dl_parameter['s3_url'].split("/")[2]
    file_key = "/".join(dl_parameter['s3_url'].split("/")[3:]) #file path in bucket
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
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

    # check if file was downloaded successfully
    if os.path.exists(filename):
        runner_logger.info(f"File {filename} downloaded successfully")
        logger.info(f"File {filename} downloaded successfully")
    else:
        runner_logger.error(f"File {filename} not downloaded successfully")
        logger.error(f"File {filename} not downloaded successfully")
        raise ValueError(f"File {filename} not downloaded successfully")
    
    # check if md5sum matches
    # get md5sum of downloaded file
    md5sum = get_md5(filename)
    # check if md5sum matches
    if md5sum != dl_parameter['md5sum']:
        runner_logger.error(f"MD5 checksum does not match for file {filename}")
        logger.error(f"MD5 checksum does not match for file {filename}")
        raise ValueError(f"MD5 checksum does not match for file {filename}")
    else:
        runner_logger.info(f"MD5 checksum matches for file {filename}")
        logger.info(f"MD5 checksum matches for file {filename}")
    
    return "completed"



# flow to download cnv data from S3 and verify md5 checksum
# use concurrency pool to download multiple files in parallel
@flow(name="download-cnv-flow", task_runner=ConcurrentTaskRunner(), log_prints=True)
def download_cnv(manifest_df: pd.DataFrame, logger) -> None:
    """Download cnv files from S3 and verify md5 checksum

    Args:
        manifest_df (pd.DataFrame): Dataframe of the manifest file
        bucket (str): S3 bucket name
    """
    # download cnv files from S3
    runner_logger = get_run_logger()

    # throttle submission of tasks to avoid overwhelming the system
    time.sleep(2)

    #setup with list of dicts to iterate over and then run with map
    submit_list = []

    for _, row in manifest_df.iterrows():
        row["bucket"] = row["s3_url"].split("/", 3)[2]
        row["file_path"] = "/".join(row["s3_url"].split("/", 3)[3:])
        f_name = os.path.basename(row["s3_url"])

        if f_name != row["file_name"]:
            runner_logger.error(
                f"Expected file name {row['file_name']} does not match observed file name in s3 url, {f_name}, not downloading file"
            )
            #logger.error(
            #    f"Expected file name {row['file_name']} does not match observed file name in s3 url, {f_name}, not downloading file"
            #)
        else:
            submit_list.append(row.to_dict())


    file_downloads = json_dl.map(submit_list, unmapped(logger), unmapped(runner_logger))
    
    return file_downloads.result()



# task to read in and transform cnv file's data
# parse segments with significant p-value and add to a new dataframe
@task(
    name="cnv-segment-parser",
    log_prints=True,
    tags=["cnv-json-downloader-tag"],
    retries=3,
    retry_delay_seconds=1,
)
def parse_segments(file_path, logger):
    """Parse out relevant segments from the data."""
    # Load the data
    # check of file exists
    if not os.path.exists(file_path):
        logger.error(f"File at path {file_path} does not exist. Not parsing")

        return [''] * 12

    with open(file_path, 'r') as f:
        data = json.load(f)

    # Extract relevant segments
    segments = []
    
    participant = data['metadata']['sample_name'].split("-")[0]
    sample_id = data['metadata']['sample_name'].split("-")[1]

    for segment in data['segments']:
        chrom = segment['position']['chrom']
        start = segment['position']['start']
        end = segment['position']['end']
        length = segment['position']['length']
        log2ratio = segment['cnv']['log2_copy_ratio']
        num_points = segment['cnv']['cnv_supporting_points']
        num_reads = segment['cnv']['cnv_supporting_reads']
        log2_p_value = segment['cnv']['log2_pval']
        log2_ci_low = segment['cnv']['log2_copy_ratio_90per_ci_low']
        log2_ci_high = segment['cnv']['log2_copy_ratio_90per_ci_high']

        segments.append([
            participant,
            sample_id,
            chrom,
            start,
            end,
            length,
            log2ratio,
            num_points,
            num_reads,
            log2_p_value,
            log2_ci_low,
            log2_ci_high
        ])

    return pd.DataFrame(segments)


# flow to parse segments
@flow(name="parse_segments_flow", task_runner=ConcurrentTaskRunner(), log_prints=True)
def parse_segments_flow(manifest_df: pd.DataFrame, download_path: str, logger) -> None:
    """Parse segments from copy number files

    Args:
        manifest_df (pd.DataFrame): Dataframe of the manifest file
        bucket (str): S3 bucket name
    """
    # download cnv files from S3
    runner_logger = get_run_logger()

    # throttle submission of tasks to avoid overwhelming the system
    time.sleep(2)

    #setup with list of file_names
    submit_list = [os.path.join(download_path, i) for i in manifest_df['file_name'].to_list()]

    parse_segments_map = parse_segments.map(submit_list, unmapped(logger))
    
    segment_data =  parse_segments_map.result()

    seg_df = pd.concat(segment_data)

    seg_df.columns = [
        'participant',
        'sample_id',
        'chrom',
        'start',
        'end',
        'length',
        'log2ratio',
        'num_points',
        'num_reads',
        'log2_p_value',
        'log2_ci_low',
        'log2_ci_high'
    ]

    #replace 'chr' with '' in chrom column
    seg_df['chrom'] = seg_df['chrom'].str.replace('chr', '', regex=False)

    return seg_df


# task to perform gene mappings to segement location for continuous data
# and create new dataframe with gene names and their corresponding log2 ratios
@task(name="download_genocde_file", log_prints=True)
def download_gencode_file(gencode_version: int):

    #download gene annotations GTF file from gencode
    # URL structure https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_48/gencode.v48.annotation.gtf.gz
    url = f"https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_{gencode_version}/gencode.v{gencode_version}.annotation.gtf.gz"
    rename_file = f"gencode_genes_{gencode_version}.gtf.gz"
    download = ShellOperation(commands=[f"curl -L -o {rename_file} {url}"], stream_output=True)

    download.run()
    
    # check if file downloaded
    if not os.path.exists(rename_file):
        raise ValueError(f"File {rename_file} NOT downloaded")
    else:
        print(f"✅ File {rename_file} downloaded!")

        #unzip file
        ShellOperation(
            commands=[f"gunzip -f {rename_file}"],  # -f forces overwrite if needed
            stream_output=True
        ).run()
        return f"{rename_file.replace('.gz', '')}"

def extract_genes(cell):
	gene_names = [i.strip().replace("gene_name", "").replace('"', '').strip() for i in cell.split(";") if 'gene_name' in i]
	if len(gene_names) > 1:
		return ";".join(gene_names)
	else:
		return gene_names[0]

DropDownChoices = Literal["segment_and_cnv_gene", "cleanup"]

#main flow to orchestrate the tasks
@flow(name="cbio-cnv-flow", log_prints=True)
def cnv_flow(bucket: str, manifest_path: str, destination_path: str, gencode_version: int, flow_type: DropDownChoices):
    """Prefect workflow to download, parse and transform cnv data for ingestion into cBioPortal.

    Args:
        bucket (str): S3 bucket name of location of manifest and to direct output files
        manifest_path (str): Path to the manifest file in specified S3 bucket
        destination_path (str): Destination path at specified S3 bucket for the output/transformed data files and log file
        gencode_version (int): Gencode version to use for gene mappings. Default is "48" for hg38.
        flow_type (DropDownChoices): Type of flow to run. Options are "segment_and_cnv-gene", "cleanup"
    """

    runner_logger = get_run_logger()

    dt = get_time()
    
    if flow_type == "cleanup":
        
        # get a list of all dirs in /usr/local/data/cnv
        runner_logger.info(f"Cleaning up cnv_flow output directory")
        output_path = "/usr/local/data/cnv"
        dirs = os.listdir(output_path)
        # delete all dirs in /usr/local/data/cnv
        for dir in dirs:
            dir_path = os.path.join(output_path, dir)
            if os.path.isdir(dir_path):
                runner_logger.info(f"Deleting directory: {dir_path}")
                shutil.rmtree(dir_path)
                runner_logger.info(f"Deleted directory: {dir_path}")
            else:
                runner_logger.info(f"Skipping non-directory file: {dir_path}")
    
    else:
        runner_logger.info(f"Running cnv_flow with bucket: {bucket}, manifest_path: {manifest_path}, destination_path: {destination_path}, flow_type: {flow_type}")
        
        # change working directory to mounted drive
        output_path = os.path.join("/usr/local/data/cnv", "cnv_run_"+dt)
        os.makedirs(output_path, exist_ok=True)

        download_path = os.path.join(output_path, "cnv_downloads_"+dt)
        os.makedirs(download_path, exist_ok=True)

        # change working directory to output path
        runner_logger.info(f"Output path: {output_path}")
        os.chdir(output_path)

        # create logger
        log_filename = f"{output_path}/cbio_cnv_transform.log"
        logger = get_logger(f"{output_path}/cbio_cnv_transform", "info")
        logger.info(f"Output path: {output_path}")

        logger.info(f"Logs beginning at {get_time()}")

        # download manifest file from S3
        runner_logger.info(f"Downloading manifest file from S3 bucket")
        file_dl(bucket, manifest_path)

        # read in manifest file
        runner_logger.info(f"Reading in manifest file")
        manifest_df = read_manifest(os.path.basename(manifest_path))[:10]

        logger.info(f"Expected number of files to download: {len(manifest_df)}")
        runner_logger.info(f"Expected number of files to download: {len(manifest_df)}")

        # download cnv files from S3
        runner_logger.info(f"Downloading cnv files from S3 bucket")

        # change working directory to download path
        os.chdir(download_path)
        download_cnv(manifest_df, logger)

        # count number of files downloaded
        num_files = len(os.listdir(download_path))
        logger.info(f"Actual number of files downloaded: {num_files}")
        runner_logger.info(f"Actual number of files downloaded: {num_files}")

        # change back to output path
        os.chdir(output_path)

        # parse segement data from cnv files
        segment_data = parse_segments_flow(manifest_df, download_path, logger)

        cols_parse = [
            'sample_id',
            'chrom',
            'start',
            'end',
            'num_points',
            'log2ratio',
            ]

        segment_data.to_csv(f"segment_data_raw_{dt}.tsv", sep="\t", index=False)

        segment_data_parse = segment_data[cols_parse]

        segment_data_parse.columns = [
            'ID',
            'chrom',
            'loc.start',
            'loc.end',
            'num.mark',
            'seg.mean'
        ]

        segment_data_parse.to_csv(f"data_cna_hg38_{dt}.seg", sep="\t", index=False)

        genocode_file_name = download_gencode_file(gencode_version)

        genocode_df = pd.read_csv(genocode_file_name, sep="\t", header=None, comment="#")

        #filter only genes that are protein_coding, exclude readthrough genes and mitochondrial DNA annotationsqq
        df_gene_protein = genocode_df[(genocode_df[2] == 'gene') & (genocode_df[8].str.contains('protein_coding')) & ~(genocode_df[8].str.contains('readthrough_gene')) & (genocode_df[0] != 'chrM')][[0, 3, 4, 8]]

        df_gene_protein['gene_names'] = df_gene_protein[8].apply(extract_genes)

        print(df_gene_protein)
        
        if not os.path.exists(log_filename):
            print(f"Log file does not exist: {log_filename}")
        else:
            print(f"Log file exists: {log_filename}")

        os.rename(log_filename, log_filename.replace(".log", "_"+dt+".log"))

        # remove downloaded JSON files by removing download path
        shutil.rmtree(download_path)
        runner_logger.info(f"Removed downloaded JSON files from {download_path}")

        #upload output directory to S3
        upload_folder_to_s3(
            local_folder=output_path,
            bucket=bucket,
            destination=destination_path,
            sub_folder=""
        )



if __name__ == "__main__":
    # testing
    #cnv_flow(bucket="cbioportal-data", manifest_path="cnv/manifest.txt", destination_path="cnv", flow_type="segment")
    pass