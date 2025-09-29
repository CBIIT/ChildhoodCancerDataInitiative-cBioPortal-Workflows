from prefect import flow, task
import boto3

@task
def restart_ecs_service(cluster_name: str, service_name: str):
    """
    Forces a new deployment of an ECS service to restart its tasks.
    """
    print(f"Restarting ECS service '{service_name}' in cluster '{cluster_name}'...")
    
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

@flow
def restart_backend_ecs_service():
    """
    A flow that orchestrates the restart of a specific ECS service.
    """
    cluster = "cbio-dev-Cluster" 
    service = "cbio-dev-Fargate-Service" 
    
    restart_ecs_service(cluster, service)

if __name__ == "__main__":
    restart_backend_ecs_service()