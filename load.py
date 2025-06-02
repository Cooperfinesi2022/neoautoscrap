import os
import logging
import mysql.connector
from mysql.connector import errorcode

logger = logging.getLogger(__name__)

def load_cars(cars_list):
    logger.info("INICIO DE CARGA DE AUTOS A MySQL")

    db_host     = os.getenv("DB_HOST", "localhost")
    db_user     = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASS", "root")
    db_name     = os.getenv("DB_NAME", "neoauto")

    try:
        conn = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        cursor = conn.cursor()

        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"
        )
        cursor.execute(f"USE `{db_name}`;")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS autos (
                id           INT           AUTO_INCREMENT PRIMARY KEY,
                titulo       VARCHAR(255)  NOT NULL,
                precio       DOUBLE        NOT NULL,
                kilometraje  VARCHAR(50),
                ubicacion    VARCHAR(255),
                combustible  VARCHAR(50),
                transmision  VARCHAR(50),
                enlace       VARCHAR(500)  NOT NULL,
                fecha        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY (enlace)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        insert_q = """
            INSERT IGNORE INTO autos 
              (titulo, precio, kilometraje, ubicacion, combustible, transmision, enlace)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        inserted = 0
        for car in cars_list:
            cursor.execute(
                insert_q,
                (
                    car["titulo"],
                    car["precio"],
                    car["kilometraje"],
                    car["ubicacion"],
                    car["combustible"],
                    car["transmision"],
                    car["enlace"]
                )
            )
            if cursor.rowcount == 1:
                inserted += 1

        conn.commit()
    except mysql.connector.Error as err:
        pass
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
