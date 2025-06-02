import logging
from prefect import flow, task, get_run_logger
from extract import extract_cars
from transform import transform_cars
from load import load_cars

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

@task(retries=2, retry_delay_seconds=10)
def task_extract_cars():
    get_run_logger().info("EXTRAYENDO AUTOS")
    return extract_cars()

@task
def task_transform_cars(raw_list):
    get_run_logger().info("TRANSFORMANDO DATOS")
    return transform_cars(raw_list)

@task
def task_load_cars(cars_list):
    get_run_logger().info("CARGANDO A LA BD")
    load_cars(cars_list)

@flow(name="etl_neoauto_flow")
def etl_neoauto_flow():
    raw_cars = task_extract_cars()
    transformed_cars = task_transform_cars(raw_cars)
    task_load_cars(transformed_cars)

if __name__ == "__main__":
    etl_neoauto_flow()