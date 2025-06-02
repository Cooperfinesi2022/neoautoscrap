import logging
from extract import extract_cars
from transform import transform_cars
from load import load_cars

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

def main():
    raw_list = extract_cars()
    transformed = transform_cars(raw_list)
    logger.info(f"Antes de la carga: lista transformada con {len(transformed)} autos.")
    load_cars(transformed)

if __name__ == "__main__":
    main()

