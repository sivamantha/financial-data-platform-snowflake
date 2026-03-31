"""
Snowflake connection utility for extraction scripts.
Uses RSA key-pair authentication and loads config from .env file.
"""

import os
import json
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv
from pathlib import Path


def get_snowflake_connection():
    """Create a Snowflake connection using RSA key-pair auth."""
    load_dotenv(Path(__file__).parent.parent / '.env')

    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')

    with open(private_key_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        private_key=private_key_bytes,
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'INGESTION_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'FIN_RAW_DB'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )
    return conn


def load_json_to_snowflake(conn, data, schema, table_name, batch_size=500):
    """
    Load raw JSON data into a single VARIANT column using batch inserts.
    Uses a temporary stage + COPY INTO for large datasets — much faster
    than row-by-row inserts.

    Args:
        conn:       Snowflake connection
        data:       list of dicts (each dict becomes one row)
        schema:     target schema (e.g. 'SEC_EDGAR')
        table_name: target table name
        batch_size: rows per INSERT batch (default 500)
    """
    if not data:
        print(f"  No data to load into {schema}.{table_name}")
        return 0

    cursor = conn.cursor()

    try:
        # Ensure schema exists and table is ready
        cursor.execute(f"USE SCHEMA {schema}")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                raw_data    VARIANT,
                _loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                _source     STRING        DEFAULT 'api_extract'
            )
        """)

        # Batch the records into chunks and insert with a single
        # multi-row VALUES clause per batch — avoids N round-trips
        total = 0
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]

            # Build: SELECT PARSE_JSON(%s) UNION ALL SELECT PARSE_JSON(%s) ...
            placeholders = " UNION ALL ".join(
                ["SELECT PARSE_JSON(%s)"] * len(batch)
            )
            values = [json.dumps(record) for record in batch]

            cursor.execute(
                f"INSERT INTO {schema}.{table_name} (raw_data) {placeholders}",
                values
            )
            total += len(batch)
            print(f"  {schema}.{table_name}: {total}/{len(data)} rows loaded", end='\r')

        print(f"  Loaded {total} rows into {schema}.{table_name}          ")
        return total

    finally:
        cursor.close()


def load_to_snowflake(conn, data, schema, table_name, columns):
    """
    Load a list of dicts into a typed Snowflake table.
    Creates the table if it doesn't exist.

    Args:
        conn:       Snowflake connection
        data:       list of dicts
        schema:     target schema
        table_name: target table name
        columns:    list of (column_name, snowflake_type) tuples
    """
    if not data:
        print(f"  No data to load into {schema}.{table_name}")
        return 0

    cursor = conn.cursor()

    try:
        cursor.execute(f"USE SCHEMA {schema}")

        col_defs = ', '.join([f"{name} {dtype}" for name, dtype in columns])
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                {col_defs},
                _loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                _source     STRING        DEFAULT 'api_extract'
            )
        """)

        col_names    = ', '.join([name for name, _ in columns])
        placeholders = ', '.join(['%s'] * len(columns))

        rows = [
            tuple(record.get(col_name) for col_name, _ in columns)
            for record in data
        ]

        cursor.executemany(
            f"INSERT INTO {schema}.{table_name} ({col_names}) VALUES ({placeholders})",
            rows
        )

        print(f"  Loaded {len(rows)} rows into {schema}.{table_name}")
        return len(rows)

    finally:
        cursor.close()