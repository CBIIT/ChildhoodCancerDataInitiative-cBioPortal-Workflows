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

@task(name="install-genome-nexus-annotation", log_prints=True)
def install_nexus():
    """Installation steps for genome nexus annotation tool
    """

    logger = get_run_logger()
    logger.info("Installing Genome Nexus Annotation tool...")
    shell_op = ShellOperation(
        commands=[
            "java -version",
            "git clone --branch v1.0.6 https://github.com/genome-nexus/genome-nexus-annotation-pipeline.git",
            "cp genome-nexus-annotation-pipeline/annotationPipeline/src/main/resources/application.properties.EXAMPLE genome-nexus-annotation-pipeline/annotationPipeline/src/main/resources/application.properties",
            "cp genome-nexus-annotation-pipeline/annotationPipeline/src/main/resources/log4j.properties.console.EXAMPLE genome-nexus-annotation-pipeline/annotationPipeline/src/main/resources/log4j.properties",
            "cd genome-nexus-annotation-pipeline/",
            "mvn clean install -DskipTests -X",
            "cd .."
        ]
    )
    shell_op.run()
    
    

@task(name="get_md5", log_prints=True)
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
    name="vcf_dl_task",
    task_run_name="vcf_dl_task_{dl_parameter[file_name]}", 
    log_prints=True,
    tags=["vcf_dl_task-tag"],
    retries=3,
    retry_delay_seconds=1,
)
def vcf_dl_task(dl_parameter: dict, runner_logger):
    """Download vcf files from S3

    Args:
        dl_parameter (dict): Dictionary of parameters for downloading the file
        logger: Logger object 
        runner_logger: Prefect logger object
    """
    runner_logger = get_run_logger()
    
    # Set the s3 resource object for local or remote execution
    bucket = dl_parameter['file_url'].split("/")[2]
    file_key = "/".join(dl_parameter['file_url'].split("/")[3:]) #file path in bucket
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
        raise

    # check if file was downloaded successfully
    if os.path.exists(filename):
        runner_logger.info(f"File {filename} downloaded successfully")
    else:
        runner_logger.error(f"File {filename} not downloaded successfully")
        raise ValueError(f"File {filename} not downloaded successfully")
    
    # check if md5sum matches
    # get md5sum of downloaded file
    md5sum = get_md5(filename)
    # check if md5sum matches
    if md5sum != dl_parameter['md5sum']:
        runner_logger.error(f"MD5 checksum does not match for file {filename}")
        raise ValueError(f"MD5 checksum does not match for file {filename}")
    else:
        runner_logger.info(f"MD5 checksum matches for file {filename}")
    
    return "completed"



# flow to download cnv data from S3 and verify md5 checksum
# use concurrency pool to download multiple files in parallel
@flow(name="download-vcf-flow", task_runner=ConcurrentTaskRunner(), log_prints=True)
def download_vcf(manifest_df: pd.DataFrame) -> None:
    """Download vcf files from S3 and verify md5 checksum

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
        row["bucket"] = row["file_url"].split("/", 3)[2]
        row["file_path"] = "/".join(row["file_url"].split("/", 3)[3:])
        row["file_name"] = os.path.basename(row["file_url"])
        f_name = os.path.basename(row["file_url"])

        if f_name != row["file_name"]:
            runner_logger.error(
                f"Expected file name {row['file_name']} does not match observed file name in s3 url, {f_name}, not downloading file"
            )

        else:
            submit_list.append(row.to_dict())


    file_downloads = vcf_dl_task.map(submit_list, unmapped(runner_logger))
    
    return file_downloads.result()


@flow(name="annotate-vcf-flow", task_runner=ConcurrentTaskRunner(), log_prints=True)
def annotator_flow(manifest_df: pd.DataFrame, download_dir: str, output_dir: str, reference_genome: str, logger) -> None:
    """Annotate vcf files

    Args:
        manifest_df (pd.DataFrame): Dataframe of the manifest file
        bucket (str): S3 bucket name
        download_dir (str): Directory where vcf files are downloaded
        output_dir (str): Directory where annotated vcf files will be saved
        reference_genome (str): Reference genome to use for annotation
        logger: Logger object
    """

    # throttle submission of tasks to avoid overwhelming the system
    time.sleep(2)

    #setup with list of dicts to iterate over and then run with map
    submit_list = []
    for _, row in manifest_df.iterrows():
        file_name = os.path.basename(row["file_url"])
        sample_barcode = row['sample']
        submit_list.append({
            'vcf_file': file_name,
            'sample_barcode': sample_barcode,
            'download_dir': download_dir,
            'output_dir': output_dir,
            'reference_genome': reference_genome
        })
        
    # run parallelized annotation
    annotation = annotator.map(submit_list, unmapped(logger))
    
    return annotation.result()

@task(name="version_check", log_prints=True)
def version_check():
    """Check version of genome nexus annotation tool
    """
    runner_logger = get_run_logger()
    
    shell_op = ShellOperation(
        commands=[
            "cat /etc/os-release",
        ]
    )
    shell_op.run()
    
    runner_logger.info("Checking version of java...")
    shell_op = ShellOperation(
        commands=[
            "java -version"
        ]
    )
    shell_op.run()

    runner_logger.info("Checking version of mvn...")
    shell_op = ShellOperation(
        commands=[
            "mvn -version"
        ]
    )
    shell_op.run()

@task(name="vcf_annotator", log_prints=True, tags=["vcf_dl_task-tag"])
def annotator(anno_parameter: dict, logger) -> None:
    """Annotate vcf file using genome nexus annotation tool

    Args:
        vcf_file (str): Path to the vcf file
        sample_barcode (str): Sample barcode
        download_dir (str): Path to the download directory
        output_dir (str): Path to the output directory
        reference_genome (str): Reference genome to use for annotation
    """
    runner_logger = get_run_logger()
    
    # load in anno params
    vcf_file = anno_parameter['vcf_file']
    sample_barcode = anno_parameter['sample_barcode']
    download_dir = anno_parameter['download_dir']
    output_dir = anno_parameter['output_dir']
    reference_genome = anno_parameter['reference_genome']
    
    vcf_path = os.path.join(download_dir, vcf_file)
    
    runner_logger.info(f"Annotating vcf file: {vcf_file}")
    if vcf_file.endswith(".gz"):
        shell_op = ShellOperation(
                    commands=[
                        f"gunzip {vcf_path}"
                    ]
                )
        shell_op.run()
        vcf_file = vcf_file.replace(".gz", "")
        vcf_path = os.path.join(download_dir, vcf_file)
        runner_logger.info(f"Gunzipped vcf file: {vcf_file}")
    
    # read in file and ignore lines starting with ##
    vcf = pd.read_csv(vcf_path, comment='#', header=None, sep='\t')
    
    # filter PASS filter
    vcf = vcf[vcf[6] == 'PASS']
    
    # select columns 0, 1, 3, 4
    vcf = vcf[[0, 1, 3, 4]]
    
    # replace 'chr' in column 0
    vcf[0] = vcf[0].str.replace('chr', '')
    
    # rename columns to Chromosome, Start_Position, Reference_Allele, Tumor_Seq_Allele1
    vcf.columns = ["Chromosome", "Start_Position", "Reference_Allele", "Tumor_Seq_Allele1"]
    
    def end_position(row):
        """Calculate end position for vcf file

        Args:
            row (pd.Series): Row of the vcf file
        Returns:
            int: End position
        """
        return row['Start_Position'] + len(row['Reference_Allele']) - 1
    
    # annotate end position
    vcf['End_Position'] = vcf.apply(end_position, axis=1)
    
    # write to new vcf file
    vcf.to_csv(vcf_path, sep='\t', index=False)
    
    if reference_genome == "GRCh37":
        try:
            shell_op = ShellOperation(
                commands=[
                    f"java -jar genome-nexus-annotation-pipeline/annotationPipeline/target/annotationPipeline-*.jar --filename {vcf_path} --output-filename {output_dir}/{os.path.basename(vcf_file).replace('.vcf', '_annotated.maf')} -e {output_dir}/{os.path.basename(vcf_file).replace('.vcf', '_annotated.maf.log')} --isoform-override mskcc"
                ]
            )
            shell_op.run()
        except Exception as e:
            runner_logger.error(f"Error annotating vcf file {vcf_file} with GRCh37: {e}")
            logger.error(f"Error annotating vcf file {vcf_file} with GRCh37: {e}")
            raise
    else:
        try:
            shell_op = ShellOperation(
                commands=[
                    'export GENOMENEXUS_BASE="https://grch38.genomenexus.org"',
                    'echo $GENOMENEXUS_BASE',
                    f"java -jar genome-nexus-annotation-pipeline/annotationPipeline/target/annotationPipeline-*.jar --filename {vcf_path} --output-filename {output_dir}/{os.path.basename(vcf_file).replace('.vcf', '_annotated.maf')} -e {output_dir}/{os.path.basename(vcf_file).replace('.vcf', '_annotated.maf.log')} --isoform-override mskcc"
                ]
            )
            shell_op.run()
        except Exception as e:
            runner_logger.error(f"Error annotating vcf file {vcf_file} with GRCh38: {e}")
            logger.error(f"Error annotating vcf file {vcf_file} with GRCh38: {e}")
            raise
    
    # replace sample barcode in output file
    anno_maf = pd.read_csv(f"{output_dir}/{os.path.basename(vcf_file).replace('.vcf', '_annotated.maf')}", sep='\t', comment='#')
    anno_maf['Tumor_Sample_Barcode'] = sample_barcode
    anno_maf.to_csv(f"{output_dir}/{os.path.basename(vcf_file).replace('.vcf', '_annotated.maf')}", sep='\t', index=False)
    
    runner_logger.info(f"Annotation completed for vcf file: {vcf_file}")

DropDownChoices = Literal["GRCh37", "GRCh38"]
DropDownChoices2 = Literal["yes", "no"]

@flow(name="cbio-vcf-annotation-flow", log_prints=True)
def vcf_anno_flow(bucket: str, runner: str, manifest_path: str, reference_genome: DropDownChoices, cleanup: DropDownChoices2) -> None:
    """Flow to annotate VCF files using Genome Nexus annotation tool

    Args:
        bucket (str): bucket name
        runner (str): runner name and destination path in s3
        manifest_path (str): path to csv file with cols for sample, md5sum and file_url of VCFs
        reference_genome (Literal['GRCh37', 'GRCh38']): reference genome to use for annotation
        cleanup (Literal["yes", "no"]): If 'yes', instead of running annotation, cleans up existing vcf annotation folder on mnt drive
    """
    if cleanup == "yes":
        # cleanup vcf annotation folder on mnt drive
        vcf_anno_path = "/usr/local/data/vcf_annotation"
        if os.path.exists(vcf_anno_path):
            shutil.rmtree(vcf_anno_path)
            runner_logger = get_run_logger()
            runner_logger.info(f"Cleaned up existing vcf annotation folder at {vcf_anno_path}")
            return None
    
    dt = get_time()

    runner_logger = get_run_logger()
    runner_logger.info("Starting VCF annotation flow...")
    
    # print current directory
    runner_logger.info(f"Current directory: {os.getcwd()}")
    home_dir = os.getcwd()
    
    # check versions of tools
    runner_logger.info("Checking versions of tools...")
    version_check()
    
    # install genome nexus annotation tool
    runner_logger.info("Installing Genome Nexus Annotation tool...")
    install_nexus()
    
    # download manifest file from S3
    runner_logger.info(f"Downloading manifest file from S3: {manifest_path}")
    file_dl(bucket, manifest_path)
    
    # read in file 
    runner_logger.info(f"Reading manifest file: {os.path.basename(manifest_path)}")
    manifest_df = pd.read_csv(os.path.basename(manifest_path))
    exp_num_files = len(manifest_df)
    runner_logger.info(f"Expected number of files downloaded: {exp_num_files}")
    

    # download vcf files from S3
    # change working directory to mounted drive 
    output_path = os.path.join("/usr/local/data/vcf_annotation", "vcf_run_"+dt)
    os.makedirs(output_path, exist_ok=True)

    download_path = os.path.join(output_path, "vcf_downloads_"+dt)
    os.makedirs(download_path, exist_ok=True)
    
    # create logger
    log_filename = f"{output_path}/cbio_vcf_annotation.log"
    logger = get_logger(f"{output_path}/cbio_vcf_annotation", "info")
    logger.info(f"Output path: {output_path}")
    logger.info(f"Expected number of files downloaded: {exp_num_files}")

    logger.info(f"Logs beginning at {get_time()}")
    
    # change working directory to download path
    runner_logger.info(f"Download path: {download_path}")
    os.chdir(download_path)
    
    runner_logger.info("Downloading VCF files from S3...")
    for i in range(0, len(manifest_df), 500):
        batch_df = manifest_df.iloc[i:i+500]
        runner_logger.info(f"Downloading batch {i//500 + 1} of {len(manifest_df)//500 + 1} VCF files...")
        download_vcf(batch_df)
    
    # count number of files downloaded
    num_files = len(os.listdir(download_path))
    runner_logger.info(f"Actual number of files downloaded: {num_files}")
    logger.info(f"Actual number of files downloaded: {num_files}")
    
    if exp_num_files != num_files:
        runner_logger.error("Number of files downloaded does not match expected number of files")
        logger.error("Number of files downloaded does not match expected number of files")

    # mk output path
    runner_logger.info(f"Output path: {output_path}")
    
    os.chdir(home_dir)
    
    # annotate vcf files
    runner_logger.info("Annotating VCF files...")
    for i in range(0, len(manifest_df), 500):
        batch_df = manifest_df.iloc[i:i+500]
        runner_logger.info(f"Annotating batch {i//500 + 1} of {len(manifest_df)//500 + 1} VCF files...")
        annotator_flow(batch_df, download_path, output_path, reference_genome, logger=logger)

    # remove downloaded VCF files by removing download path
    shutil.rmtree(download_path)
    runner_logger.info(f"Removed downloaded VCF files from {download_path}")
    logger.info(f"Removed downloaded VCF files from {download_path}")
        
    # upload annotated files to S3
    os.rename(log_filename, log_filename.replace(".log", "_"+dt+".log"))
    
    upload_folder_to_s3(
        local_folder=output_path,
        bucket=bucket,
        destination=runner,
        sub_folder=""
    )