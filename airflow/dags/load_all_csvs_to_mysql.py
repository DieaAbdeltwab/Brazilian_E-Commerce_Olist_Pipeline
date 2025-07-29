from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import pandas as pd
from sqlalchemy import create_engine

file_table_map = {
    "olist_orders_dataset.csv": "orders",
    "olist_customers_dataset.csv": "customers",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "product_category_translation"
}

def create_task(file_name, table_name):
    def download_and_load():
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
        )
        bucket = 'ecommerce-raw'
        local_path = f'/tmp/{file_name}'
        s3.download_file(bucket, file_name, local_path)
        df = pd.read_csv(local_path)
        engine = create_engine('mysql+pymysql://myuser:mypassword@mysql:3306/mydb')
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    return PythonOperator(
        task_id=f'load_{table_name}_table',
        python_callable=download_and_load
    )

with DAG(
    dag_id='load_all_csvs_to_mysql',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ecommerce", "minio", "mysql"]
) as dag:

    # 🔄 إنشاء المهام وربطهم
    previous_task = None
    for file_name, table_name in file_table_map.items():
        task = create_task(file_name, table_name)
        if previous_task:
            previous_task >> task
        previous_task = task
