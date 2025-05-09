import subprocess, os, json
from datetime import datetime
from pytz import timezone
from prefect import task, flow
import mysql.connector
import pandas as pd
import boto3
from botocore.exceptions import ClientError


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
    else:
        raise ValueError("Invalid environment name. Please use one of: ['dev', 'qa'].")
        
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

@task(name="restore_dump", log_prints=True)
def restore_dump(
    host: str,
    username: str,
    password: str,
    dbClusterIdentifier: str,
    engine: str = "mysql",
    port=3306,
    dump_file: str = None,
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
        "--databases",
        dbClusterIdentifier,
        "<",
        dump_file,
    ]

    try:
        # Run the MySQL command using subprocess
        subprocess.run(" ".join(command), shell=False, check=True)

        print(f"✅ Dump successful: {dump_file}")
        return True
    except Exception as err:
        print(f"❌ Error importing dump file: {err}")
        raise err
    
@flow(name="db_counter", log_prints=True)
def db_counter(db_type: str, dump_file: str = None, **kwargs):
    """Count columns and rows in a MySQL dump file.

    Args:
        db_type (str): Type of database to count ('dump' or 'restore')
        dump_file (str): Path to database dump file to perform 'expected' counts on. Optional if db_type is 'restore'.
        **kwargs: database creds for restored database to perform 'observed' counts on

    Returns:
        pd.DataFrame: DataFrame containing table names, column counts, and row counts.
    """
    if db_type == "dump": 
    
        # make mock db to load in dump file to assess counts
        """conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password=''
        )

        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS count_db")
        conn.close()"""

        # create a new database for the dump file
        """print("Creating database for dump file...")
        #env = os.environ.copy()
        #env["MYSQL_PWD"] = ""
        sql = "CREATE DATABASE IF NOT EXISTS count_db;"
        subprocess.run(
            ["mysql", "-u", "root", "--password=", "-e", sql],
            shell=False,
            check=True,
            #env=env,
        )"""

        #load in dump file locally to get counts
        """print(f"Loading dump file {dump_file} into local database")
        command = ["mysql", "-u", "root", "-p", "count_db", "<", dump_file]
        subprocess.run(command, shell=False, check=True)

        config = {
            'host': 'localhost',
            'user': 'root',
            'password': '',
            'database': 'count_db'
            }"""
        
        dev_creds = get_secret("dev")
        dev_creds = json.loads(dev_creds)

        config = {
            'host': dev_creds["host"],
            'user': dev_creds["username"],
            'password': dev_creds["password"],
            'database': dev_creds["dbClusterIdentifier"]
        }


    elif db_type == "restore":
        config = {
            'host': kwargs["host"],
            'user': kwargs["username"],
            'password': kwargs["password"],
            'database': kwargs["dbClusterIdentifier"]
        }
    else:
        raise ValueError("Invalid db_type. Use 'dump' or 'restore'.")

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

        # Create DataFrame
        df = pd.DataFrame(stats)
        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return pd.DataFrame()
    """finally:
        if conn.is_connected():
            cursor.close()
            conn.close()"""