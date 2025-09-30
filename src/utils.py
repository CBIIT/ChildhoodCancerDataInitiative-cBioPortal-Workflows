import subprocess, os, json
from datetime import datetime
from pytz import timezone
from prefect import task, flow, task
import mysql.connector
import pandas as pd
import boto3
import re
from botocore.exceptions import ClientError
import logging


def get_time() -> str:
    """Returns the current time"""
    tz = timezone("EST")
    now = datetime.now(tz)
    dt_string = now.strftime("%Y%m%d_T%H%M%S")
    return dt_string

def set_s3_resource():
    """This method sets the s3_resource object to either use localstack
    for local development if the LOCALSTACK_ENDPOINT_URL variable is
    defined and returns the object
    """
    localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT_URL")
    if localstack_endpoint != None:
        AWS_REGION = "us-east-1"
        AWS_PROFILE = "localstack"
        ENDPOINT_URL = localstack_endpoint
        boto3.setup_default_session(profile_name=AWS_PROFILE)
        s3_resource = boto3.resource(
            "s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL
        )
    else:
        s3_resource = boto3.resource("s3")
    return s3_resource

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
    elif env_name == "qa":
        secret_name = "ccdicbio-qa-rds"
    elif env_name == "stage":
        secret_name = "ccdicbiostage-db-credentials"
    elif env_name == "prod":
        secret_name = "ccdicbioprod-db-credentials"
    else:
        raise ValueError("Invalid environment name. Please use one of: ['dev', 'qa', 'stage', 'prod'].")
        
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

@task(name="Download file", task_run_name="download_file_{filepath}", log_prints=True)
def file_dl(bucket, filepath):
    """File download using bucket name and filename
    filepath is the key path in bucket
    file is the basename

    Args:
        bucket (str): S3 bucket name
        filepath (str): Path to the file in the S3 bucket
    Raises:
        ClientError: If the download fails
    Returns:
        None
    """
    # Set the s3 resource object for local or remote execution

    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    file_key = filepath
    file_name = os.path.basename(filepath)
    try:
        source.download_file(file_key, file_name)
    except ClientError as ex:
        ex_code = ex.response["Error"]["Code"]
        ex_message = ex.response["Error"]["Message"]
        print(
            f"ClientError occurred while downloading file {filepath} from bucket {bucket}:\n{ex_code}, {ex_message}"
        )
        raise

@task(name="create_dump", log_prints=True)
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
        raise err

@task(name="upload_to_s3", log_prints=True)
def upload_to_s3(file_path : str, bucket_name: str, output_path="dump_folder", region_name="us-east-1"):
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
    s3_key = f"{output_path}/{file_name}"

    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"✅ Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
        raise

@task(name="restore_dump", log_prints=True)
def restore_dump(
    host: str,
    username: str,
    password: str,
    dbClusterIdentifier: str,
    dump_file: str,
    engine: str = "mysql",
    port=3306,
):
    """Creates a dump of the database using mysqldump.

    Args:
        host (str): Host name of the database.
        username (str): Username for the database.
        password (str): Password for the database.
        dbClusterIdentifier (str): Database name/cluster identifier.
        engine (str, optional): Database engine. Defaults to "mysql".
        port (int, optional): Port number of mysql database. Defaults to 3306.
        dump_file (str): Local path of dump file to restore.

    Raises:
        subprocess.CalledProcessError: If the mysqldump command fails.

    Returns:
        None
    """

    command = [
        "mysql",
        f"--host={host}",
        f"--port={port}",
        f"--user={username}",
        f"--password={password}",
        "--database",
        dbClusterIdentifier,
    ]

    try:
        with open(dump_file, "rb") as f:
            process = subprocess.run(
                command,
                stdin=f,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                shell=False
            )
        
        if process.returncode != 0:
            print(f"Error code: {process.returncode}")
            print(f"Error message: {process.stderr.decode()}")
            raise subprocess.CalledProcessError(process.returncode, command, process.stderr)
        else:
            print("Restore completed successfully.")
            return True
    except subprocess.CalledProcessError as e:
        print("Restore failed.")
        print("STDOUT:", e.stdout.decode())
        print("STDERR:", e.stderr.decode())
        raise
    
    
@flow(name="db_counter", log_prints=True)
def db_counter(db_type: str, dump_file: str = None):
    """Count columns and rows in a MySQL dump file.

    Args:
        db_type (str): Type of database to count ('dump' or env speficied in ['dev', 'qa', 'stage', 'prod']).
        dump_file (str): Path to database dump file to perform 'expected' counts on. Optional if db_type is not 'dump'.

    Returns:
        pd.DataFrame: DataFrame containing table names, column counts, and row counts.
    """
    if db_type == "dump": 
    
        # Check if dump_file is provided
        if dump_file is None:
            raise ValueError("dump_file must be provided when db_type is 'dump'.")
        
        # Check if the dump file exists
        if not os.path.exists(dump_file):
            raise FileNotFoundError(f"Dump file {dump_file} does not exist.")
        
        # read in dump file
        with open(dump_file, "r") as file:
            dump_data = file.read()

        stats = []

        # Use regex to find the CREATE TABLE statements and extract column names
        table_column_counts = {}
        
        table_pattern = re.compile(r"CREATE TABLE\s+`(\w+)`\s+\((.*?)\)\s+ENGINE=", re.DOTALL)
        matches = table_pattern.findall(dump_data)

        for table_name, columns_block in matches:
            # Split lines and filter out keys and constraints
            lines = columns_block.strip().splitlines()
            column_lines = [
                line for line in lines
                if not re.search(r'^\s*(PRIMARY|UNIQUE|KEY|CONSTRAINT|FOREIGN)', line.strip(), re.IGNORECASE)
            ]
            table_column_counts[table_name] = len(column_lines)

        # Count rows in the dump file
        table_row_counts = {}
        
        # different handling due to complex nature of row entries dump file
        # read file line by line
        # record the number of rows in each table
        # this method may not be as efficient but handles nested parantheses, ad hoc semi-colons and other
        # unforeseen syntax issues that may arise in the dump file in the future that cannot be handled by regex
        for line in dump_data.splitlines():
            if line.startswith("INSERT INTO"):
                # Extract table name and row count
                table_name = re.search(r'INSERT INTO `(\w+)`', line).group(1)
                if table_name not in table_row_counts.keys():
                    table_row_counts[table_name] = 0
            elif line.startswith("("):
                table_row_counts[table_name] += 1  

        for table in table_column_counts:
            column_count = table_column_counts[table]
            row_count = table_row_counts.get(table, 0)  # In case the table isn't found
            stats.append([table, column_count, row_count])
        
        # Create DataFrame
        df = pd.DataFrame(stats)
        df.columns = ['table_name', 'column_count', 'row_count']

        return df

    elif db_type in ["dev", "qa", "stage", "prod"]: #adding dev for local testing or for count checks on dev db
        restore_creds = json.loads(get_secret(db_type))

        config = {
            'host': restore_creds["host"],
            'user': restore_creds["username"],
            'password': restore_creds["password"],
            'database': restore_creds["dbClusterIdentifier"]
        }
    else:
        raise ValueError("Invalid db_type. Use 'dump' for dump file or env in ['dev', 'qa', 'stage', 'prod'].")

    # count columns and rows in the dump file
    print("Counting columns and rows in the database...")
    try:
        conn = mysql.connector.connect(**config)

        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]

        stats = []

        print(f"Tables in the database: {tables}")
        # Iterate through each table
        for table in tables:
            # Count columns
            print(f"Counting columns in table: {table}")
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (config['database'], table))
            column_count = cursor.fetchone()[0]

            # Count rows
            print(f"Counting rows in table: {table}")
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            row_count = cursor.fetchone()[0]

            stats.append({
                'table_name': table,
                'column_count': column_count,
                'row_count': row_count
            })

        # Create DataFrame and name columns
        df = pd.DataFrame(stats)
        df.columns = ['table_name', 'column_count', 'row_count']
        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return pd.DataFrame()
    """finally:
        if conn.is_connected():
            cursor.close()
            conn.close()"""
    
def get_logger(loggername: str, log_level: str):
    """Returns a basic logger with a logger name using a std format
    log level can be set using one of the values in log_levels.
    """
    log_levels = {  # sorted level
        "notset": logging.NOTSET,  # 00
        "debug": logging.DEBUG,  # 10
        "info": logging.INFO,  # 20
        "warning": logging.WARNING,  # 30
        "error": logging.ERROR,  # 40
    }

    logger_filename = loggername + ".log"
    logger = logging.getLogger(loggername)
    logger.setLevel(log_levels[log_level])

    # set the file handler
    file_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    file_handler = logging.FileHandler(logger_filename, mode="w")
    file_handler.setFormatter(logging.Formatter(file_FORMAT, "%H:%M:%S"))
    file_handler.setLevel(log_levels["info"])

    # set the stream handler
    # stream_handler = logging.StreamHandler(sys.stdout)
    # stream_handler.setFormatter(logging.Formatter(file_FORMAT, "%H:%M:%S"))
    # stream_handler.setLevel(log_levels["info"])

    # logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger

@task(name="Upload folder", task_run_name="upload_folder_{local_folder}", log_prints=True)
def upload_folder_to_s3(
    local_folder: str, bucket: str, destination: str, sub_folder: str
) -> None:
    """This function uploads all the files from a folder
    and preserves the original folder structure
    """
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    folder_basename = os.path.basename(local_folder)
    for root, _, files in os.walk(local_folder):
        for filename in files:
            # construct local path
            local_path = os.path.join(root, filename)

            # construct the full dst path
            relative_path = os.path.relpath(local_path, local_folder)
            s3_path = os.path.join(
                destination, sub_folder, folder_basename, relative_path
            )

            # upload file
            # this should overwrite file if file exists in the bucket
            source.upload_file(local_path, s3_path)

@task(name="Restart ECS service")
def restart_ecs_service(env_name: str):
    """
    Forces a new deployment of an ECS service to restart its tasks.
    """
    
    cluster_name = f"cbio-{env_name}-Cluster" 
    service_name = f"cbio-{env_name}-Fargate-Service" 

    # Initialize the Boto3 client for ECS
    ecs_client = boto3.client("ecs")

    try:
        # Call update_service with force_new_deployment=True
        ecs_client.update_service(
            cluster=cluster_name,
            service=service_name,
            forceNewDeployment=True
        )
        print(f"Successfully triggered new deployment for service '{service_name}'.")
    except Exception as e:
        print(f"Failed to restart service '{service_name}': {e}")
        raise