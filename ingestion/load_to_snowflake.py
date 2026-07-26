import os
from pathlib import Path

import snowflake.connector


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DATA_DIRECTORY = REPOSITORY_ROOT / "data" / "jaffle-data"

CUSTOMERS_FILE = DATA_DIRECTORY / "source_customers.csv"
ORDERS_FILE = DATA_DIRECTORY / "source_orders.csv"


def required_environment() -> dict[str, str]:
    names = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
    ]

    missing = [name for name in names if not os.getenv(name)]

    if missing:
        raise RuntimeError(
            f"Missing environment variables: {', '.join(missing)}"
        )

    return {name: os.environ[name] for name in names}


def validate_files() -> None:
    missing = [
        str(path)
        for path in [CUSTOMERS_FILE, ORDERS_FILE]
        if not path.exists()
    ]

    if missing:
        raise FileNotFoundError(
            f"Missing ingestion files: {', '.join(missing)}"
        )


def load_data() -> None:
    validate_files()
    environment = required_environment()

    print("Input files validated")
    print("Connecting to Snowflake")

    connection = snowflake.connector.connect(
        account=environment["SNOWFLAKE_ACCOUNT"],
        user=environment["SNOWFLAKE_USER"],
        password=environment["SNOWFLAKE_PASSWORD"],
        role="DBT_ROLE",
        warehouse="TRANSFORMING_WH",
        database="ANALYTICS",
        schema="RAW",
    )

    try:
        with connection.cursor() as cursor:
            print("Connected to Snowflake")

            cursor.execute(
                """
                create file format if not exists INGESTION_CSV_FORMAT
                    type = csv
                    skip_header = 1
                    field_optionally_enclosed_by = '"'
                    empty_field_as_null = true
                """
            )

            cursor.execute(
                """
                create stage if not exists INGESTION_STAGE
                    file_format = (
                        format_name = 'INGESTION_CSV_FORMAT'
                    )
                """
            )

            cursor.execute(
                """
                create table if not exists RAW_CUSTOMERS (
                    CUSTOMER_ID varchar,
                    CUSTOMER_NAME varchar
                )
                """
            )

            cursor.execute(
                """
                create table if not exists RAW_ORDERS (
                    ORDER_ID varchar,
                    CUSTOMER_ID varchar,
                    ORDERED_AT timestamp_ntz,
                    STORE_ID varchar,
                    SUBTOTAL number,
                    TAX_PAID number,
                    ORDER_TOTAL number
                )
                """
            )

            cursor.execute("truncate table RAW_CUSTOMERS")
            cursor.execute("truncate table RAW_ORDERS")
            cursor.execute("remove @INGESTION_STAGE")

            for path in [CUSTOMERS_FILE, ORDERS_FILE]:
                cursor.execute(
                    f"put file://{path.as_posix()} "
                    "@INGESTION_STAGE "
                    "auto_compress=true overwrite=true"
                )

            print("Files uploaded to Snowflake stage")

            cursor.execute(
                """
                copy into RAW_CUSTOMERS
                from @INGESTION_STAGE
                pattern = '.*source_customers.*'
                force = true
                on_error = 'abort_statement'
                """
            )

            cursor.execute(
                """
                copy into RAW_ORDERS
                from @INGESTION_STAGE
                pattern = '.*source_orders.*'
                force = true
                on_error = 'abort_statement'
                """
            )

            cursor.execute("select count(*) from RAW_CUSTOMERS")
            customer_count = cursor.fetchone()[0]

            cursor.execute("select count(*) from RAW_ORDERS")
            order_count = cursor.fetchone()[0]

            print(f"Loaded {customer_count:,} customers")
            print(f"Loaded {order_count:,} orders")

    finally:
        connection.close()


if __name__ == "__main__":
    load_data()
