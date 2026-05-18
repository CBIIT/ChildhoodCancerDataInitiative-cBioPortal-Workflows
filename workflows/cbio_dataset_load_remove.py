import os
import json
from prefect import flow, task, get_run_logger
from pathlib import Path
from src.utils import get_secret, upload_to_s3, folder_dl, get_time, restart_ecs_service
from typing import Literal
from prefect_shell import ShellOperation


@task(name="validate_study", log_prints=True)
def validate_study(
    cbio_home: str,
    portal_home: str,
    portal_info_dir: str,
    study_dir: str,
    html_table_file_path: str,
    error_file_path: str,
):
    """
    Runs cBioPortal validation/import dry run against configured RDS database.
    """
    logger = get_run_logger()

    importer_script = f"{cbio_home}/scripts/importer/validateData.py"

    if not Path(importer_script).exists():
        raise RuntimeError(f"Importer script not found at {importer_script}")

    env = os.environ.copy()
    env.update(
        {
            "CBIOPORTAL_HOME": cbio_home,
            "PORTAL_HOME": portal_home,
            "PORTAL_INFO_DIR": portal_info_dir,
        }
    )

    cmd = [
        f"python3 {importer_script} --study_directory {study_dir} --portal_info_dir {portal_info_dir} --html {html_table_file_path} --error_file {error_file_path} -v"
    ]

    logger.info(f"Validating study: {study_dir}")
    shell_op = ShellOperation(commands=cmd, env=env)
    try:
        result = shell_op.run()
        return_code = 0  # if no exception, it's 0

    except RuntimeError as e:
        # Extract return code from Prefect error
        msg = str(e)
        logger.warning(f"Validation returned non-zero exit: {msg}")

        # crude parse (works reliably for Prefect error format)
        if "return code" in msg:
            return_code = int(msg.split("return code")[-1].strip().strip("."))
        else:
            raise  # unknown failure, rethrow

    # Handle allowed non-zero codes
    if return_code in [0, 3]:
        logger.info(f"Validation completed with code {return_code} (acceptable)")
    else:
        raise RuntimeError(f"Unexpected validation failure (code {return_code})")

    logger.info("Study validation complete")


@task(name="import_study", log_prints=True)
def import_study(
    cbio_home: str,
    portal_home: str,
    portal_info_dir: str,
    study_dir: str,
    html_table_file_path: str,
):
    """
    Imports study into AWS RDS-backed cBioPortal DB using configured credentials.
    """
    logger = get_run_logger()

    importer_script = f"{cbio_home}/scripts/importer/metaImport.py"

    if not Path(importer_script).exists():
        raise RuntimeError(f"Importer script not found at {importer_script}")

    env = os.environ.copy()
    env.update(
        {
            "CBIOPORTAL_HOME": cbio_home,
            "PORTAL_HOME": portal_home,
            "PORTAL_INFO_DIR": portal_info_dir,
        }
    )

    cmd = [
        f"python3 {importer_script} --study_directory {study_dir} --portal_info_dir {portal_info_dir} --html {html_table_file_path} --override_warning --verbose"
    ]

    logger.info(f"Importing study: {study_dir}")
    shell_op = ShellOperation(commands=cmd, env=env, stream_output=True)
    try:
        result = shell_op.run()
        return_code = 0  # if no exception, it's 0

    except Exception as e:
        # Extract return code from Prefect error
        msg = str(e)
        logger.warning(f"Import returned non-zero exit: {msg}")

        # crude parse (works reliably for Prefect error format)
        if "return code" in msg:
            return_code = int(msg.split("return code")[-1].strip().strip("."))
            error = e
        else:
            raise  # unknown failure, rethrow

    # Handle allowed non-zero codes
    if return_code == 0:
        logger.info("Import completed successfully with code 0")
    elif return_code in [1, 2, 3]:
        logger.info(
            f"Import completed with code {return_code} (acceptable), but error raised: {error}"
        )
        logger.info(f"Warning/error message: {msg}")
    else:
        raise RuntimeError(f"Unexpected import failure (code {return_code})")

    logger.info("Study import successful")


@task(name="remove_study", log_prints=True)
def remove_study(
    cbio_home: str,
    portal_home: str,
    study_id: str,
):
    """
    Removes study from AWS RDS-backed cBioPortal DB using configured credentials.
    """
    logger = get_run_logger()

    remover_script = f"{cbio_home}/scripts/importer/cbioportalImporter.py"

    if not Path(remover_script).exists():
        raise RuntimeError(f"Remover script not found at {remover_script}")

    env = os.environ.copy()
    env.update(
        {
            "CBIOPORTAL_HOME": cbio_home,
            "PORTAL_HOME": portal_home,
        }
    )

    cmd = [f"python3 {remover_script} -c remove-study -id {study_id}"]

    logger.info(f"Removing study: {study_id}")
    shell_op = ShellOperation(commands=cmd, env=env, stream_output=True)
    try:
        result = shell_op.run()
        return_code = 0  # if no exception, it's 0

    except Exception as e:
        # Extract return code from Prefect error
        msg = str(e)
        logger.warning(f"Remove returned non-zero exit: {msg}")

        # crude parse (works reliably for Prefect error format)
        if "return code" in msg:
            return_code = int(msg.split("return code")[-1].strip().strip("."))
            error = e
        else:
            raise  # unknown failure, rethrow

    # Handle allowed non-zero codes
    if return_code == 0:
        logger.info("Remove completed successfully with code 0")
    else:
        raise RuntimeError(f"Unexpected remove failure (code {return_code}): {msg}")


@task(name="set_app_props", log_prints=True)
def app_props(cbio_home: str, portal_home: str, creds: dict):
    """Writes database credentials to cBioPortal config files for RDS database connection.

    Args:
        cbio_home (str): Path to the cBioPortal home directory
        portal_home (str): Path to the portal home directory
        creds (dict): Dictionary containing RDS credentials (host, port, username, password, dbname/dbClusterIdentifier)

    Raises:
        RuntimeError: If credentials are missing or config files cannot be written
    """
    logger = get_run_logger()

    # Validate required credentials
    required_creds = ["host", "port", "username", "password", "dbClusterIdentifier"]
    missing = [k for k in required_creds if k not in creds]
    if missing:
        raise RuntimeError(f"Missing required credentials: {missing}")

    # Set paths to config files
    core_app_props_path = f"{cbio_home}/src/main/resources/application.properties"
    app_props_path = f"{portal_home}/application.properties"

    # MySQL JDBC parameters for RDS compatibility
    jdbc_params = (
        "allowPublicKeyRetrieval=true&allowLoadLocalInfile=true&serverTimezone=UTC"
    )

    # Build connection string using JDBC MySQL format
    conn_string = f"jdbc:mysql://{creds['host']}:{creds['port']}/{creds['dbClusterIdentifier']}?{jdbc_params}"

    # cBioPortal application.properties format (Spring Data properties)
    config_lines = [
        f"spring.datasource.url={conn_string}",
        f"spring.datasource.username={creds['username']}",
        f"spring.datasource.password={creds['password']}",
        "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
        "spring.jpa.database-platform=org.hibernate.dialect.MySQL8Dialect",
        "spring.jpa.hibernate.ddl-auto=validate",
    ]

    config_content = "\n".join(config_lines)

    # Write to portal info directory
    try:
        with open(app_props_path, "w") as f:
            f.write(config_content)
        logger.info(f"Wrote config to {app_props_path}")
    except IOError as e:
        raise RuntimeError(f"Failed to write {app_props_path}: {e}")

    # Write to core cBioPortal directory
    try:
        with open(core_app_props_path, "w") as f:
            f.write(config_content)
        logger.info(f"Wrote config to {core_app_props_path}")
    except IOError as e:
        raise RuntimeError(f"Failed to write {core_app_props_path}: {e}")

    # install maven
    env = os.environ.copy()
    env.update(
        {
            "MAVEN_OPTS": "-Dmaven.repo.local=/tmp/m2_clean",
        }
    )
    cmd = [
        f"cd {cbio_home} && mvn clean install -DskipTests -U -Dmaven.repo.local=/tmp/m2_clean"
    ]
    shell_op = ShellOperation(commands=cmd, env=env)
    try:
        result = shell_op.run()
    except Exception as e:
        msg = str(e)
        logger.warning(f"Maven install returned non-zero exit: {msg}")
        raise RuntimeError(f"Failed to install maven dependencies: {msg}")
    logger.info("cBioPortal application properties configured successfully")


DropDownRunChoice = Literal[
    "load_dataset", "validate_dataset", "remove_dataset", "clear_working_dir"
]
DropDownEnvChoice = Literal["dev", "qa"]


@flow(name="cbio-dataset-load-remove-flow", log_prints=True)
def main_flow(
    target_env_name: DropDownEnvChoice,
    run_type: DropDownRunChoice,
    validation_portal_files_dir_uri: str = None,
    source_files_dir_uri: str = None,
    output_dir_uri: str = None,
    remove_study_id: str = None,
) -> None:
    """workflow to load, dryrun validate or remove cBioPortal datasets from a tier, or clear working directory of any files

    Args:
        target_env_name (DropDownEnvChoice): Environment to run workflow in (e.g., dev, qa)
        run_type (DropDownRunChoice): Type of operation to perform (e.g., load_dataset, validate_dataset, remove_dataset, clear_working_dir)
        validation_portal_files_dir_uri (str, optional): URI to validation portal files. Defaults to None. Example: s3://cbio-backup-dev/v6.4.1_load_json_validation
        source_files_dir_uri (str, optional): URI to source files directory. Defaults to None. Example: s3://cbio-backup-dev/study_files_to_load/study1
        output_dir_uri (str, optional): URI to output directory. Defaults to None. Example: s3://cbio-backup-dev/study_files_to_load/study1/validation_output
        remove_study_id (str, optional): Study ID to remove from the database. Defaults to None. Example: lgg_ucsf_2014
    """

    dt = get_time()
    print(f"Workflow started at {dt}")

    # elif load dataset:
    if run_type == "clear_working_dir":
        print(">>> Performing data cleanup ...")
        ShellOperation(
            commands=[
                "ls -l /usr/local/data/",
                "rm -r /usr/local/data/dataset_load_*",
                "ls -l /usr/local/data/",  # confirm removal of dataset_load working dirs
            ]
        ).run()
        print(">>> Data clean up completed, exiting workflow ....")
        return None

    elif run_type in ["load_dataset", "validate_dataset", "remove_dataset"]:

        # grab creds from secrets manager
        creds_string = get_secret(target_env_name)
        creds = json.loads(creds_string)

        # set CBIOPORTAL_HOME env variable for use in validation and import tasks
        CBIOPORTAL_HOME = "/opt/prefect/cbioportal-core-main"
        PORTAL_HOME = "/opt/prefect/cbioportal-v6.4.1"
        os.environ["CBIOPORTAL_HOME"] = CBIOPORTAL_HOME
        os.environ["PORTAL_HOME"] = PORTAL_HOME

        # write db creds to app props file
        app_props(CBIOPORTAL_HOME, PORTAL_HOME, creds)
        
        if run_type in ["load_dataset", "validate_dataset"]:
            # get cwd for future use
            home_dir = os.getcwd()

            # set working directory and make dir in /usr/local/data for dumps if it doesn't exist
            working_dir = f"/usr/local/data/dataset_load_{dt}"
            os.makedirs(working_dir, exist_ok=True)

            # download study files from s3 to working directory
            os.chdir(working_dir)
            source_files_dir = source_files_dir_uri.split("://")[1].split("/", 1)[1]
            folder_dl(source_files_dir_uri.split("://")[1].split("/", 1)[0], source_files_dir)

            # download validation portal files from s3 to working directory if running validation dry run
            validation_portal_files_dir = validation_portal_files_dir_uri.split("://")[1].split("/", 1)[1]
            folder_dl(validation_portal_files_dir_uri.split("://")[1].split("/", 1)[0], validation_portal_files_dir)
            PORTAL_INFO_DIR = os.path.join(
                working_dir, validation_portal_files_dir)
            os.environ["PORTAL_INFO_DIR"] = PORTAL_INFO_DIR

            os.chdir(home_dir)

            study_dir = os.path.join(working_dir, source_files_dir)

        # run data loader to load data into db or run validation dry run
        if run_type == "load_dataset":
            import_study(
                CBIOPORTAL_HOME,
                PORTAL_HOME,
                PORTAL_INFO_DIR,
                study_dir,
                f"{working_dir}/validation_output_{dt}.html",
            )
        elif run_type == "validate_dataset":
            validate_study(
                CBIOPORTAL_HOME,
                PORTAL_HOME,
                PORTAL_INFO_DIR,
                study_dir,
                f"{working_dir}/validation_output_{dt}.html",
                f"{working_dir}/validation_errors_{dt}.txt",
            )
        elif run_type == "remove_dataset":
            # check that remove_study_id is provided
            if not remove_study_id:
                raise ValueError(
                    "remove_study_id must be provided for remove_dataset run_type"
                )

            remove_study(
                CBIOPORTAL_HOME,
                PORTAL_HOME,
                remove_study_id,
            )
        else:
            raise ValueError(f"Invalid run_type: {run_type}")

        # upload log files to s3
        if run_type in ["validate_dataset", "load_dataset"]:
            output_bucket = output_dir_uri.split("://")[1].split("/", 1)[0]
            output_path = output_dir_uri.split("://")[1].split("/", 1)[1]
            upload_to_s3(
                f"{working_dir}/validation_output_{dt}.html",
                output_bucket,
                f"{output_path}/validation_output_{dt}",
            )
            if os.path.exists(f"{working_dir}/validation_errors_{dt}.txt"):
                upload_to_s3(
                    f"{working_dir}/validation_errors_{dt}.txt",
                    output_bucket,
                    f"{output_path}/validation_output_{dt}",
                )

        # restart ecs service to restart cBioPortal and apply changes
        if run_type in ["load_dataset", "remove_dataset"]:
            restart_ecs_service(target_env_name)

        return None

    else:
        raise ValueError(f"Invalid run_type: {run_type}")
