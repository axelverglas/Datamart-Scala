from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlalchemy

# Connexions source et destination
SRC_CONN = 'postgresql://postgres:admin@data-warehouse:5432/taxi'
DST_CONN = 'postgresql://postgres:admin@data-mart:5432/taxi_olap'

def migrate_dim_vendor(**kwargs):
    query = 'SELECT DISTINCT vendorid AS vendor_id FROM yellow_tripdata WHERE vendorid IS NOT NULL'
    df = pd.read_sql_query(query, con=SRC_CONN)

    try:
        existing = pd.read_sql_query('SELECT vendor_id FROM dim_vendor', con= DST_CONN)
        df = df[~df['vendor_id'].isin(existing['vendor_id'])]
    except Exception:
        pass

    if not df.empty:
        df.to_sql('dim_vendor', con= DST_CONN, if_exists='append', index=False)


def migrate_dim_rate_code(**kwargs):
    query = 'SELECT DISTINCT ratecodeid AS rate_code_id FROM yellow_tripdata WHERE ratecodeid IS NOT NULL'
    df = pd.read_sql_query(query, con=SRC_CONN)

    try:
        existing = pd.read_sql_query('SELECT rate_code_id FROM dim_rate_code', con= DST_CONN)
        df = df[~df['rate_code_id'].isin(existing['rate_code_id'])]
    except Exception:
        pass

    if not df.empty:
        df.to_sql('dim_rate_code', con= DST_CONN, if_exists='append', index=False)


def migrate_dim_payment_type(**kwargs):
    query = 'SELECT DISTINCT payment_type AS payment_type_id FROM yellow_tripdata WHERE payment_type IS NOT NULL'
    df = pd.read_sql_query(query, con=SRC_CONN)

    try:
        existing = pd.read_sql_query('SELECT payment_type_id FROM dim_payment_type', con= DST_CONN)
        df = df[~df['payment_type_id'].isin(existing['payment_type_id'])]
    except Exception:
        pass

    if not df.empty:
        df.to_sql('dim_payment_type', con= DST_CONN, if_exists='append', index=False)


def migrate_dim_location(**kwargs):
    q1 = 'SELECT DISTINCT pulocationid AS location_id FROM yellow_tripdata WHERE pulocationid IS NOT NULL'
    q2 = 'SELECT DISTINCT dolocationid AS location_id FROM yellow_tripdata WHERE dolocationid IS NOT NULL'
    df1 = pd.read_sql_query(q1, con=SRC_CONN)
    df2 = pd.read_sql_query(q2, con=SRC_CONN)
    df = pd.concat([df1, df2], ignore_index=True).drop_duplicates(subset=['location_id'])

    try:
        existing = pd.read_sql_query('SELECT location_id FROM dim_location', con= DST_CONN)
        df = df[~df['location_id'].isin(existing['location_id'])]
    except Exception:
        pass

    if not df.empty:
        df.to_sql('dim_location', con= DST_CONN, if_exists='append', index=False)


def migrate_dim_date(**kwargs):
    # Récupération des dates de pickup et dropoff
    engine_src = sqlalchemy.create_engine(SRC_CONN)
    engine_dst = sqlalchemy.create_engine(DST_CONN)
    try:
        q1 = ("""
            SELECT DISTINCT CAST(tpep_pickup_datetime AS DATE) AS full_date
            FROM yellow_tripdata
            WHERE tpep_pickup_datetime IS NOT NULL
        """)
        q2 = ("""
            SELECT DISTINCT CAST(tpep_dropoff_datetime AS DATE) AS full_date
            FROM yellow_tripdata
            WHERE tpep_dropoff_datetime IS NOT NULL
        """)
        df1 = pd.read_sql_query(q1, con=engine_src)
        df2 = pd.read_sql_query(q2, con=engine_src)

        # Concaténation et déduplication
        df = pd.concat([df1, df2], ignore_index=True).drop_duplicates(subset=['full_date'])

        # Conversion en datetime pour .dt
        df['full_date'] = pd.to_datetime(df['full_date'])

        # Calcul des dimensions temporelles
        df['year']         = df['full_date'].dt.year
        df['month']        = df['full_date'].dt.month
        df['day']          = df['full_date'].dt.day
        df['day_of_week']  = df['full_date'].dt.weekday
        df['day_name']     = df['full_date'].dt.day_name()
        df['month_name']   = df['full_date'].dt.month_name()
        df['quarter']      = df['full_date'].dt.quarter
        df['is_weekend']   = df['day_of_week'] >= 5

        # Exclusion des dates déjà existantes
        try:
            existing = pd.read_sql_query('SELECT full_date FROM dim_date', con=engine_dst)
            existing['full_date'] = pd.to_datetime(existing['full_date'])
            df = df[~df['full_date'].isin(existing['full_date'])]
        except Exception:
            pass

        # Insertion
        if not df.empty:
            df.to_sql('dim_date', con=engine_dst, if_exists='append', index=False)
    finally:
        engine_src.dispose()
        engine_dst.dispose()


def migrate_fact_trips(**kwargs):
    # ... (reste de votre fonction inchangé) ...
    pass

with DAG(
    dag_id='migrate_datawarehouse_to_datamart',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['migration', 'postgres'],
) as dag:
    t1 = PythonOperator(task_id='migrate_dim_vendor',       python_callable=migrate_dim_vendor)
    t2 = PythonOperator(task_id='migrate_dim_rate_code',    python_callable=migrate_dim_rate_code)
    t3 = PythonOperator(task_id='migrate_dim_payment_type', python_callable=migrate_dim_payment_type)
    t4 = PythonOperator(task_id='migrate_dim_location',     python_callable=migrate_dim_location)
    t5 = PythonOperator(task_id='migrate_dim_date',         python_callable=migrate_dim_date)
    t6 = PythonOperator(task_id='migrate_fact_trips',       python_callable=migrate_fact_trips)

    [t1, t2, t3, t4, t5] >> t6
