from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import papermill as pm
import clickhouse_connect
import os

def run_clickhouse_sql(sql_file_path="/opt/airflow/dags/sql/ecommerce_init_clickhouse.sql"):
    import os
    from clickhouse_connect import get_client

    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.getenv("CLICKHOUSE_PORT", 8123))
    username = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "123")

    print(f"🔄 Connecting to ClickHouse at {host}:{port} as user '{username}'...")
    client = get_client(host=host, port=port, username=username, password=password)

    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"❌ SQL file not found: {sql_file_path}")

    print(f"📄 Reading SQL file: {sql_file_path}")
    with open(sql_file_path, "r") as file:
        sql_script = file.read().strip()

    # ✅ حذف التعليقات
    lines = [line for line in sql_script.splitlines() if not line.strip().startswith("--")]
    sql_cleaned = "\n".join(lines)

    print("📤 Splitting SQL statements...")
    statements = [stmt.strip() for stmt in sql_cleaned.split(";") if stmt.strip()]
    print(f"▶️ Executing {len(statements)} SQL statements...")

    for i, stmt in enumerate(statements, start=1):
        try:
            print(f"➡️ Statement {i}/{len(statements)}:")
            print(stmt.splitlines()[0][:80] + "...")
            client.command(stmt)
        except Exception as e:
            print(f"❌ Error in statement {i}: {e}")
            raise



#==================================================================================

def run_notebook():
    pm.execute_notebook(
        '/opt/airflow/dags/notebooks/ecommerce_etl_job.ipynb',     # input
        '/opt/airflow/dags/notebooks/ecommerce_output.ipynb'                      # output
    )

with DAG(
    dag_id='run_jupyter_notebook_etl',
    start_date=datetime(2023, 1, 1),  # ضروري جداً
    schedule_interval=None,       # أو None لو عايزه manual
    catchup=False,
    tags=['etl', 'notebook']
) as dag:
    
    init_clickhouse_task = PythonOperator(
        task_id='run_sql_script_on_clickhouse',
        python_callable=run_clickhouse_sql,
    )


    run_etl = PythonOperator(
        task_id='run_notebook_etl',
        python_callable=run_notebook
    )



    init_clickhouse_task >> run_etl  