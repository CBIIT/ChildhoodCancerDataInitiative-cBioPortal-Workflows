from prefect import flow
from src.utils import restart_ecs_service

@flow
def restart_backend_ecs_service(env_name: str):
    """
    A flow that orchestrates the restart of a specific ECS service.
    """
    restart_ecs_service(env_name)

if __name__ == "__main__":
    restart_backend_ecs_service()