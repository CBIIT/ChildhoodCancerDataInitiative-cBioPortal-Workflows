import os
import json
from prefect import flow
from src.utils import create_dump, get_secret, upload_to_s3


@flow(name="cbio-dump-flow", log_prints=True)
def export_db(
    env_tier: str, bucket_name: str
):
    """Execute database export and upload to S3.

    Args:
        env_tier (str): Tier to perform export on (e.g., dev, qa, prod)
        bucket_name (str, optional): Bucket name to upload to (e.g. cbio-backup-dev)
    """
    
    # retrieve and load creds
    creds_string = get_secret(env_tier)
    creds = json.loads(creds_string)

    #dump the database
    dump_file_path = create_dump(**creds)

    #upload the dump file to S3
    upload_to_s3(dump_file_path, bucket_name)

    #remove the dump file
    os.remove(dump_file_path)
    print(f"✅ Removed dump file: {dump_file_path}")


if __name__ == "__main__":
    export_db()
