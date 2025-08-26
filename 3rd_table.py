import calendar
from io import BytesIO
from datetime import datetime, timedelta
import pandas as pd
import boto3
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy.exc import SQLAlchemyError

# CONFIG - SOURCE (Postgres Dummy)
PG_HOST = '127.0.0.1'
PG_PORT = 5000
PG_DATABASE = 'source_db'
PG_USER = 'username'
PG_PASSWORD = 'password'
PG_TABLE = 'public.3rd_table'

# CONFIG - MINIO
MINIO_ENDPOINT = 'http://127.0.0.1:9000'
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'
MINIO_BUCKET = '3rd-table'

# CONFIG - TIMESCALE
TS_HOST = '127.0.0.1'
TS_PORT = 5432
TS_DB = 'ts_db'
TS_USER = 'username'
TS_PASSWORD = 'password'
TS_TABLE = 'public.3rd_table'

# Schema (Timescale)
TS_EXPECTED_COLUMNS = {
    "USERID": "int64",
    "SESSIONID": "string",
    "PAGE_URL": "string",
    "CLICKS": "int64",
    "SCROLL_DEPTH": "float64",
    "EVENT_TYPE": "string",
    "EVENT_SUB_TYPE": "string",
    "EVENT_TIMESTAMP": "datetime64[ns]",
    "EVENT_DATE": "string",   # export as string for parquet
    "DEVICE_TYPE": "string",
    "BROWSER": "string",
    "LATENCY_MS": "float64",
    "NETWORK_SPEED": "float64",
    "REFERRER": "string",
    "UUID": "string",
    "UPDATED_AT": "datetime64[ns]",
    "UPDATED_DATE": "string"  # export as string for parquet
}

# Clients 
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

engine_pg = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{quote_plus(PG_PASSWORD)}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)

engine_ts = create_engine(
    f"postgresql+psycopg2://{TS_USER}:{quote_plus(TS_PASSWORD)}@{TS_HOST}:{TS_PORT}/{TS_DB}"
)


def ensure_minio_bucket():
    """Check if MinIO bucket exists, else create it."""
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Created bucket: {MINIO_BUCKET}")


def extract_day_data(target_date):
    """Extract dummy data from Postgres."""
    q = f'''
        SELECT * FROM {PG_TABLE}
        WHERE "EVENT_DATE" = '{target_date}'
        ORDER BY "EVENT_DATE" ASC
    '''
    df = pd.read_sql(q, con=engine_pg)
    if df.empty:
        print(f"[INFO] No data found for {target_date}")
        return None
    return df


def write_temp_parquet(df, target_date):
    """Write a temp parquet file into MinIO."""
    buf = BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    temp_key = f"3rd_table/temp/{target_date}.parquet"
    s3.put_object(Bucket=MINIO_BUCKET, Key=temp_key, Body=buf.getvalue())
    print(f"[INFO] Temp parquet written → {temp_key}")
    return temp_key


def save_to_monthly_parquet(df, target_date):
    """Append data into monthly partitioned parquet in MinIO."""
    year = target_date.year
    month_name = calendar.month_name[target_date.month].lower()
    key = f"3rd_table/{year}/{month_name}_raw.parquet"

    try:
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        existing_df = pd.read_parquet(BytesIO(obj["Body"].read()))
        df = pd.concat([existing_df, df], ignore_index=True)
        print(f"[INFO] Appending to existing {key}")
    except Exception:
        print(f"[INFO] Creating new monthly file {key}")

    buf = BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buf.getvalue())
    print(f"[INFO] Saved {len(df)} rows into MinIO → {key}")
    return key


def load_to_timescale(df):
    """Load data into TimescaleDB with dummy schema mapping."""
    for col in TS_EXPECTED_COLUMNS.keys():
        if col not in df.columns:
            df[col] = None
    df = df[list(TS_EXPECTED_COLUMNS.keys())]

    # Fix datetime fields
    if 'EVENT_TIMESTAMP' in df.columns:
        df['EVENT_TIMESTAMP'] = pd.to_datetime(df['EVENT_TIMESTAMP'], errors='coerce')

    if 'UPDATED_AT' in df.columns:
        df['UPDATED_AT'] = pd.to_datetime(df['UPDATED_AT'], errors='coerce')

    try:
        df.to_sql(
            TS_TABLE.split(".")[-1],
            engine_ts,
            schema=TS_TABLE.split(".")[0],
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )
        print(f"[INFO] Inserted {len(df)} rows into TimescaleDB.")
    except SQLAlchemyError as e:
        print(f"[ERROR] Insert into Timescale failed: {e}")
        with engine_ts.connect() as conn:
            conn.rollback()


def delete_temp_parquet(temp_key):
    """Delete temporary parquet file from MinIO."""
    try:
        s3.delete_object(Bucket=MINIO_BUCKET, Key=temp_key)
        print(f"[INFO] Deleted temp parquet → {temp_key}")
    except Exception as e:
        print(f"[WARN] Could not delete temp file {temp_key}: {e}")


# MAIN 
if __name__ == '__main__':
    ensure_minio_bucket()

    today = datetime.now().date()
    target_date = today - timedelta(days=2)

    df = extract_day_data(target_date)
    if df is not None and not df.empty:
        temp_key = write_temp_parquet(df, target_date)
        load_to_timescale(df)
        save_to_monthly_parquet(df, target_date)
        delete_temp_parquet(temp_key)
    else:
        print("[INFO] Nothing to process.")
