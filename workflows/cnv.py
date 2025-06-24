import json
import csv
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
            logger.error(
                f"Expected file name {row['file_name']} does not match observed file name in s3 url, {f_name}, not downloading file"
            )
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
    
    try:
        segment_data =  parse_segments_map.result()
    except Exception as e:
        runner_logger.error(f"Error parsing segments: {e}")
        logger.error(f"Error parsing segments: {e}")
        raise

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
def download_gencode_file(gencode_version: int, logger):

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
        logger.info(f"File {rename_file} downloaded!")

        #unzip file
        try: 
            ShellOperation(
                commands=[f"gunzip -f {rename_file}"],  # -f forces overwrite if needed
                stream_output=True
            ).run()
            
            return f"{rename_file.replace('.gz', '')}"
        except Exception as e:
            logger.error(f"Error unzipping file {rename_file}: {e}")
            raise ValueError(f"Error unzipping file {rename_file}: {e}")



@task(name="gencode_gene_list_format", log_prints=True)
def gene_list_format(file_name: str, logger):
    gencode_df = pd.read_csv(file_name, sep="\t", header=None, comment="#")

    #filter only genes that are protein_coding, exclude readthrough genes and mitochondrial DNA annotationsqq
    df_gene_protein = gencode_df[(gencode_df[2] == 'gene') & (gencode_df[8].str.contains('protein_coding')) & ~(gencode_df[8].str.contains('readthrough_gene')) & (gencode_df[0] != 'chrM')][[0, 3, 4, 8]]

    df_gene_protein.columns = ['chrom', 'start', 'end', 'tags']

    def extract_genes(cell):
        gene_names = [i.strip().replace("gene_name", "").replace('"', '').strip() for i in cell.split(";") if 'gene_name' in i]
        if len(gene_names) > 1:
            return ";".join(gene_names)
        else:
            return gene_names[0]
    
    # extract gene names
    df_gene_protein['gene_names'] = df_gene_protein['tags'].apply(extract_genes)

    # genes that share same location in genome and transcribed from same transcript, pick first annotation
    # since the overlap in genome
    df_gene_protein = df_gene_protein.sort_values(['chrom', 'start', 'end']).drop_duplicates(subset=['chrom', 'start', 'end'], keep='first')

    # for genes that have multiple positions, pick instance with longest length
    df_gene_protein['length'] = df_gene_protein['end'] - df_gene_protein['start']
    df_gene_protein = df_gene_protein.sort_values(['chrom', 'start', 'length'], ascending=[True, True, False]).drop_duplicates(subset=['chrom', 'gene_names'], keep='first')
    # drop length column
    df_gene_protein = df_gene_protein.drop('length', axis=1)

    #update positions to 0-based indexing BED format
    # see for more details https://bedtools.readthedocs.io/en/latest/content/general-usage.html
    df_gene_protein['start'] = df_gene_protein['start'] - 1

    #drop tags and save to BED file
    df_gene_protein.drop('tags', axis=1).to_csv(f"{file_name.replace('gtf', 'bed')}", sep="\t", index=False, quoting=csv.QUOTE_NONE, escapechar='\\')

    # log that gene list formatting is complete
    logger.info(f"Gene list formatting complete. File saved to {file_name.replace('gtf', 'bed')}")
    print(f"✅ Gene list formatting complete. File saved to {file_name.replace('gtf', 'bed')}")

    # return name of mapping file
    return f"{file_name.replace('gtf', 'bed')}"

@task(name="segment_file_format", log_prints=True)
def segment_file_format(file_name: str, logger):

    df = pd.read_csv(file_name, sep="\t")

    # drop num.mark column, reorder columns
    df = df.drop('num.mark', axis=1)[['chrom', 'loc.start', 'loc.end', 'ID', 'seg.mean']]

    df.columns = ['chrom', 'start', 'end', 'sample_id', 'log2_ratio']

    df['chrom'] = 'chr' + df['chrom'].astype(str)

    df.to_csv(f'{file_name.replace(".seg", ".bed")}', sep="\t", index=False)

    print(f"✅ Segment BED file formatting complete. File saved to {file_name.replace('.seg', '.bed')}")
    logger.info(f"Segment BED file formatting complete. File saved to {file_name.replace('.seg', '.bed')}")

    return f'{file_name.replace(".seg", ".bed")}'

@task(name="bedtools_intersect", log_prints=True)
def bedtools_intersect(segment_bed_file: str, mapping_file: str, output_file: str):
    """Perform bedtools intersect on segment and mapping files

    Args:
        segment_bed_file (str): Path to the segment BED file
        mapping_file (str): Path to the mapping BED file
        output_file (str): Path to the output file
    """
    # check if bedtools is installed by running bedtools --version
    bedtools_check = ShellOperation(
        commands=["bedtools --version"],
        stream_output=True
    )
    bedtools_check.run()

    # run bedtools intersect command
    intersect_command = f"bedtools intersect -a {mapping_file} -b {segment_bed_file} -wo -f 0.5 > {output_file}"
    intersect_operation = ShellOperation(
        commands=[intersect_command],
        stream_output=True
    )

    intersect_operation.run()

def gistic_like_calls(val):
    # log2 bins that are gistic like are as follows:
    # > 1.0 AMP (> CN 4.0)
    # > 0.3 Gain (>CN 2.4)
    # >= -0.3 to <= 0.3 Copy Neutral (1.62 to 2.46 CN)
    # < -0.3 Hemizy Loss (< 1.62) 
    # < -1.0 Homozy Loss (< 1)
    if val > 1.0:
        return 2
    elif val > 0.3:
        return 1
    elif val < -1.0:
        return -2
    elif val < -0.3:
        return  -1 
    else:
        return 0

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
        manifest_df = read_manifest(os.path.basename(manifest_path))

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
        logger.info(f"Raw segment data parsed. File saved to segment_data_raw_{dt}.tsv")
        runner_logger.info(f"Segment data parsed. File saved to segment_data_raw_{dt}.tsv")

        segment_data_parse = segment_data[cols_parse]

        segment_data_parse.columns = [
            'ID',
            'chrom',
            'loc.start',
            'loc.end',
            'num.mark',
            'seg.mean'
        ]

        seg_data_file_name = f"data_cna_hg38_{dt}.seg"

        segment_data_parse.to_csv(seg_data_file_name, sep="\t", index=False)
        logger.info(f"Segment data for cBio ingestion. File saved to {seg_data_file_name}")
        runner_logger.info(f"Segment data parsed for cBio ingestion. File saved to {seg_data_file_name}")

        genocode_file_name = download_gencode_file(gencode_version, logger)

        # format GTF file to BED format
        mapping_file = gene_list_format(genocode_file_name, logger)

        #check if mapping file was created
        if not os.path.exists(mapping_file):
            raise ValueError(f"Mapping file {mapping_file} was not created. Please check the gene list formatting task.")
        else:
            runner_logger.info(f"Mapping file {mapping_file} was created successfully")
        
        #remove gencode file to save space
        if os.path.exists(genocode_file_name):
            os.remove(genocode_file_name)
            runner_logger.info(f"Removed gencode file {genocode_file_name} to save space")
        else:
            runner_logger.warning(f"Gencode file {genocode_file_name} does not exist. Not removing file.")

        # format seg file to BED format
        segment_bed_file = segment_file_format(seg_data_file_name, logger)

        #perform bedtools intersect
        runner_logger.info(f"Performing bedtools intersect on {segment_bed_file} and {mapping_file}")

        intersect_output_file = f"cnv_gene_mappings_{dt}.tsv"

        bedtools_intersect(
            segment_bed_file=segment_bed_file,
            mapping_file=mapping_file,
            output_file=intersect_output_file
        )

        # check if output file was created
        if not os.path.exists(intersect_output_file):
            raise ValueError(f"Output file of raw intersections {intersect_output_file} was not created. Please check the bedtools command.")
        else:
            runner_logger.info(f"Output file of raw intersections {intersect_output_file} was created successfully")
            logger.info(f"Output file of raw intersections {intersect_output_file} was created successfully")

        # cut out fields 4, 8, and 9 from the output file using bash command since so large
        cnv_gene_map_file = f"cnv_gene_map_{dt}.tsv"
        command = f"cut -f 4,8,9 {intersect_output_file} | sed 's/\"//g' | sed 's/;//g' | sed 's/ //g' > {cnv_gene_map_file}"
        runner_logger.info(f"Running command: {command}")
        cut_operation = ShellOperation(
            commands=[command],
            stream_output=True
        )
        cut_operation.run()
        # check if output file was created
        
        if not os.path.exists(cnv_gene_map_file):
            raise ValueError(f"Output file of cut command {cnv_gene_map_file} was not created. Please check the cut command.")
        else:
            runner_logger.info(f"Output file of cut command {cnv_gene_map_file} was created successfully")
            logger.info(f"Output file of cut command {cnv_gene_map_file} was created successfully")


        # cut fields 5,6,7,8 for later validation of mapping
        validation_mapping = f"cnv_gene_map_validation_{dt}.tsv"
        command_validation = f"cut -f 5,6,7,8 {intersect_output_file} | sort | uniq > {validation_mapping}"
        runner_logger.info(f"Running command: {command_validation}")
        cut_validation_operation = ShellOperation(
            commands=[command_validation],
            stream_output=True
        )
        cut_validation_operation.run()

        # check if output file was created
        if not os.path.exists(validation_mapping):
            raise ValueError(f"Output file of cut command {validation_mapping} was not created. Please check the cut command.")
        else:
            runner_logger.info(f"Output file of cut command {validation_mapping} was created successfully")
            logger.info(f"Output file of cut command {validation_mapping} was created successfully")

        # gzip the original intersect output file to save space
        command_gzip = f"gzip -f {intersect_output_file}"
        runner_logger.info(f"Running command: {command_gzip}")
        gzip_operation = ShellOperation(
            commands=[command_gzip],
            stream_output=True
        )
        gzip_operation.run()
        # check if output file was gzipped
        if not os.path.exists(intersect_output_file + ".gz"):
            raise ValueError(f"Output file of gzip command {intersect_output_file}.gz was not created. Please check the gzip command.")
        else:
            runner_logger.info(f"Output file of gzip command {intersect_output_file}.gz was created successfully")
            logger.info(f"Output file of gzip command {intersect_output_file}.gz was created successfully")

        #format for cbio input file
        cnv_gene_map_cbio = pd.read_csv(cnv_gene_map_file, sep="\t", header=None)

        cnv_gene_map_cbio.columns = ['sample_id', 'Hugo_Symbol', 'log2']

        try:
            cnv_gene_map_cbio_pivot = cnv_gene_map_cbio.pivot(
                index='Hugo_Symbol',
                columns='sample_id',
                values='log2'
            ).reset_index().fillna("NA")

            cnv_gene_map_cbio_pivot.to_csv(f"data_log2_cna_{dt}.txt", sep="\t", index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
            logger.info(f"Continuous gene level mappings complete. File saved to data_log2_cna_{dt}.txt")
            runner_logger.info(f"Continuous gene level mappings complete. File saved to data_log2_cna_{dt}.txt")
        except ValueError as e:
            runner_logger.error(f"Error pivoting cnv_gene_map_cbio: {e}")
            logger.error(f"Error pivoting cnv_gene_map_cbio: {e}")
        
        # format for gistic like calls
        try:
            cnv_gene_map_cbio['gistic_like'] = cnv_gene_map_cbio['log2'].apply(gistic_like_calls)
            cnv_gene_map_cbio_gistic = cnv_gene_map_cbio.pivot(
                index='Hugo_Symbol',
                columns='sample_id',
                values='gistic_like'
            ).reset_index().fillna(0)
            cnv_gene_map_cbio_gistic.to_csv(f"data_cna_{dt}.txt", sep="\t", index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
            logger.info(f"GISTIC-like discrete gene level mappings complete. File saved to data_cna_{dt}.txt")
            runner_logger.info(f"GISTIC-like discrete gene level mappings complete. File saved to data_cna_{dt}.txt")
        except ValueError as e:
            runner_logger.error(f"Error pivoting cnv_gene_map_cbio for gistic like calls: {e}")
            logger.error(f"Error pivoting cnv_gene_map_cbio for gistic like calls: {e}")

        # validate that all samples and segments have had gene level mappings performed
        # original file so big, might need to run in terminal/command line
        print(f"Validating that all samples and segments have had gene level mappings performed")
        segment_data_validate = segment_data.groupby(['sample_id', 'chrom', 'start', 'end']).size().reset_index(name='exp_counts')
        segment_data_validate['chrom'] = 'chr' + segment_data_validate['chrom'].astype(str)

        gene_data_validate = pd.read_csv(validation_mapping, sep="\t", header=None).drop_duplicates().rename(columns={0: 'chrom', 1: 'start', 2: 'end', 3: 'sample_id'}).groupby(['sample_id', 'chrom', 'start', 'end']).size().reset_index(name='obs_counts')

        # merge the two dataframes on sample_id, chrom, start, end
        validate_df = pd.merge(segment_data_validate, gene_data_validate, on=['sample_id', 'chrom', 'start', 'end'], how='outer').fillna(0)

        validate_df['length'] = validate_df['end'] - validate_df['start']

        # parse data where expected counts do not match gene counts
        validate_df['mismatch'] = validate_df['exp_counts'] != validate_df['obs_counts']
        validate_df_mismatch = validate_df[validate_df['mismatch']]
        if not validate_df_mismatch.empty:
            runner_logger.error(f"Mismatch found in expected counts and gene counts for {len(validate_df_mismatch)} segments")
            logger.error(f"Mismatch found in expected counts and gene counts for {len(validate_df_mismatch)} segments")
            logger.error(f"Please check the mismatch validation file for more details")
            validate_df_mismatch.to_csv(f"validate_df_mismatch_{dt}.tsv", sep="\t", index=False)
        else:
            runner_logger.info("No mismatches found in expected counts and gene counts")

        #save the validate_df to a file
        validate_df.to_csv(f"validate_df_{dt}.tsv", sep="\t", index=False)
    
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