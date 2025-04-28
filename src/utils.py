import subprocess, os
from datetime import datetime
from pytz import timezone
from prefect import task
import boto3
from botocore.exceptions import ClientError


def get_time() -> str:
    """Returns the current time"""
    tz = timezone("EST")
    now = datetime.now(tz)
    dt_string = now.strftime("%Y%m%d_T%H%M%S")
    return dt_string

@task(name="get_secret")
def get_secret(env_name: str):
    """Get the secret from AWS Secrets Manager.

    Args:
        env_name (str): Environment name (e.g., dev, prod)

    Raises:
        e: ClientError
        ValueError: If the environment name is invalid.

    Returns:
        dict: JSON object with credentials
    """

    region_name = "us-east-1"

    if env_name == "dev":
        secret_name = "ccdicbio-dev-rds"
    else:
        raise ValueError("Invalid environment name. Please use one of: ['dev'].")
        
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    # retreive the secret
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]
    return secret

@task(name="create_dump")
def create_dump(
    host: str,
    username: str,
    password: str,
    dbClusterIdentifier: str,
    engine: str = "mysql",
    port=3306,
    output_dir="/usr/local/data/dumps",
):
    """Creates a dump of the database using mysqldump.

    Args:
        host (str): Host name of the database.
        username (str): Username for the database.
        password (str): Password for the database.
        dbClusterIdentifier (str): Database name/cluster identifier.
        engine (str, optional): Database engine. Defaults to "mysql".
        port (int, optional): Port number of mysql database. Defaults to 3306.
        output_dir (str, optional): Local path of output directory. Defaults to "/usr/local/data/dumps".

    Raises:
        subprocess.CalledProcessError: If the mysqldump command fails.

    Returns:
        str: Path to the dump file.
    """

    os.makedirs(output_dir, exist_ok=True)
    timestamp = get_time()
    dump_file = os.path.join(output_dir, f"{dbClusterIdentifier}_dump_{timestamp}.sql")

    command = [
        "mysqldump",
        f"--host={host}",
        f"--port={port}",
        f"--user={username}",
        f"--password={password}",
        "--single-transaction",
        "--skip-lock-tables",
        "--databases",
        dbClusterIdentifier,
    ]

    try:
        with open(dump_file, "w") as f:
            process = subprocess.run(
                command, stdout=f, stderr=subprocess.PIPE, check=False, shell=False
            )
            if process.returncode != 0:
                print(f"Error code: {process.returncode}")
                print(f"Error message: {process.stderr.decode()}")
                raise subprocess.CalledProcessError(
                    process.returncode, command, process.stderr
                )

        print(f"✅ Dump successful: {dump_file}")
        return dump_file
    except Exception as err:
        print(f"❌ mysqldump failed: {err}")
        raise

@task(name="upload_to_s3")
def upload_to_s3(file_path : str, bucket_name: str, region_name="us-east-1"):
    """
    Uploads a file to an S3 bucket.

    Args:
        file_path (str): Local path to the file to upload.
        bucket_name (str): S3 bucket name.
        region_name (str, optional): AWS region name. Defaults to "us-east-1".

    Raises:
        ClientError: If the upload fails.
    """

    s3 = boto3.client("s3", region_name=region_name)
    file_name = file_path.split("/")[-1]
    s3_key = f"dump_folder/{file_name}"

    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"✅ Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
        raise