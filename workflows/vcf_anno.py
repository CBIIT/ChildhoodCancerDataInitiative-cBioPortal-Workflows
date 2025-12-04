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

@task(name="fin-install-java", log_prints=True)
def install_java():
    """Installation steps for Java 21
    """
    logger = get_run_logger()
    logger.info("Installing Java 21...")
    shell_op = ShellOperation(
        commands=[
            "update-alternatives --install /usr/bin/java java /usr/local/data/jvm/jdk-21/bin/java 1",
            "update-alternatives --install /usr/bin/javac javac /usr/local/data/jvm/jdk-21/bin/javac 1",
            "update-alternatives --config java",
            "update-alternatives --config javac"
            "java -version"
        ]
    )
    shell_op.run()
    

@task(name="install-genome-nexus-annotation", log_prints=True)
def install_nexus():
    """Installation steps for genome nexus annotation tool
    """
    # Set Java and Maven
    #os.environ["JAVA_HOME"] = "/usr/local/data/jvm/jdk-21"
    
    #version_check()

    logger = get_run_logger()
    logger.info("Installing Genome Nexus Annotation tool...")
    shell_op = ShellOperation(
        commands=[
            "update-alternatives --set /usr/bin/java java /usr/local/data/jvm/jdk-21/bin/java 1",
            "update-alternatives --set /usr/bin/javac javac /usr/local/data/jvm/jdk-21/bin/javac 1",
            #"update-alternatives --config java",
            #"update-alternatives --config javac"
            "java -version"
            "git clone --branch v1.0.6 https://github.com/genome-nexus/genome-nexus-annotation-pipeline.git",
            "cp genome-nexus-annotation-pipeline/annotationPipeline/src/main/resources/application.properties.EXAMPLE genome-nexus-annotation-pipeline/annotationPipeline/src/main/resources/application.properties",
            "cp genome-nexus-annotation-pipeline/annotationPipeline/src/main/resources/log4j.properties.console.EXAMPLE genome-nexus-annotation-pipeline/annotationPipeline/src/main/resources/log4j.properties",
            "cd genome-nexus-annotation-pipeline/",
            #'bash -c "export JAVA_HOME=/usr/local/data/jvm/jdk-21"; "export PATH=$JAVA_HOME/bin:$PATH";',# "export MAVEN_HOME=/usr/local/data/maven/apache-maven-3.9.11"; export PATH=$MAVEN_HOME/bin:$PATH"',
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
        row["bucket"] = row["s3_url"].split("/", 3)[2]
        row["file_path"] = "/".join(row["s3_url"].split("/", 3)[3:])
        row["file_name"] = os.path.basename(row["s3_url"])
        f_name = os.path.basename(row["s3_url"])

        if f_name != row["file_name"]:
            runner_logger.error(
                f"Expected file name {row['file_name']} does not match observed file name in s3 url, {f_name}, not downloading file"
            )

        else:
            submit_list.append(row.to_dict())


    file_downloads = vcf_dl_task.map(submit_list, unmapped(runner_logger))
    
    return file_downloads.result()

@task(name="version_check", log_prints=True)
def version_check():
    """Check version of genome nexus annotation tool
    """
    runner_logger = get_run_logger()
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

@task(name="vcf_annotator", log_prints=True)
def annotator(vcf_file: str, output_dir: str) -> None:
    """Annotate vcf file using genome nexus annotation tool

    Args:
        vcf_file (str): Path to the vcf file
        output_dir (str): Path to the output directory
    """
    runner_logger = get_run_logger()
    
    runner_logger.info(f"Annotating vcf file: {vcf_file}")
    shell_op = ShellOperation(
        commands=[
            'bash -c "export JAVA_HOME=/usr/local/data/jvm/jdk-21"; "export PATH=$JAVA_HOME/bin:$PATH";',
            f"java -jar genome-nexus-annotation/genome-nexus-annotation-pipeline-1.0.6.jar --filename {vcf_file} --output-filename {output_dir}/{os.path.basename(vcf_file).replace('.vcf', '_annotated.vcf')} --isoform-override mskcc"
        ]
    )
    shell_op.run()
    runner_logger.info(f"Annotation completed for vcf file: {vcf_file}")

@flow(name="cbio-vcf-annotation-flow", log_prints=True)
def vcf_anno_flow(bucket: str, runner:str, manifest_path: str):
    """_summary_

    Args:
        bucket (str): bucket name
        runner (str): runner name and destination path in s3
        manifest_path (str): path to csv file with cols for sample and s3_url of VCFs
    """
    
    dt = get_time()

    
    runner_logger = get_run_logger()
    runner_logger.info("Starting VCF annotation flow...")
    
    # print current directory
    runner_logger.info(f"Current directory: {os.getcwd()}")
    
    #install_java()
    
    # check java install
    shell_op = ShellOperation(
        commands=[
            "print uname -a",
        ]
    )
    shell_op.run()
    
    # install genome nexus annotation tool
    runner_logger.info("Installing Genome Nexus Annotation tool...")
    #install_nexus()
    
    #version_check()
    
    # download manifest file from S3
    runner_logger.info(f"Downloading manifest file from S3: {manifest_path}")
    file_dl(bucket, manifest_path)
    
    # read in file 
    runner_logger.info(f"Reading manifest file: {os.path.basename(manifest_path)}")
    manifest_df = pd.read_csv(os.path.basename(manifest_path))
    num_files = len(manifest_df)
    runner_logger.info(f"Expected number of files downloaded: {num_files}")

    # download vcf files from S3
    # change working directory to mounted drive 
    output_path = os.path.join("/usr/local/data/vcf_annotation", "vcf_run_"+dt)
    os.makedirs(output_path, exist_ok=True)

    download_path = os.path.join(output_path, "vcf_downloads_"+dt)
    os.makedirs(download_path, exist_ok=True)
    
    # change working directory to download path
    runner_logger.info(f"Download path: {download_path}")
    os.chdir(download_path)
    
    runner_logger.info("Downloading VCF files from S3...")
    download_vcf(manifest_df)
    
    # count number of files downloaded
    num_files = len(os.listdir(download_path))
    runner_logger.info(f"Actual number of files downloaded: {num_files}")

    # change to output path
    os.chdir(output_path)
    runner_logger.info(f"Output path: {output_path}")
    
    # annotate vcf files
    runner_logger.info("Annotating VCF files...")
    for vcf_file in os.listdir(download_path):
        if vcf_file.endswith(".gz"):
            
            # gunzip the file
            runner_logger.info(f"Gunzipping file: {vcf_file}")
            shell_op = ShellOperation(
                commands=[
                    f"gunzip {os.path.join(download_path, vcf_file)}"
                ]
            )
            shell_op.run()
            vcf_file = vcf_file.replace(".gz", "")
            
        vcf_file_path = os.path.join(download_path, vcf_file)
        annotator(vcf_file_path, output_path)

    # remove downloaded JSON files by removing download path
    shutil.rmtree(download_path)
    runner_logger.info(f"Removed downloaded JSON files from {download_path}")
        
    # upload annotated files to S3

    upload_folder_to_s3(
        local_folder=output_path,
        bucket=bucket,
        destination=runner,
        sub_folder=""
    )
    
    #TODO: add log file output and upload to S3
    #TODO: add error handling for failed downloads or annotations
    