from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# Timezone setup (This is optional, you can remove local_tz if you configure default timezone in airflow.cfg)
local_tz = pendulum.timezone("Asia/Bangkok")

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG without the timezone argument
dag = DAG(
    'ndjson_to_csv_to_bigquery_dag',
    default_args=default_args,
    description='DAG to process ndjson to csv and then load to BigQuery',
    schedule='0 0 * * *',  # This runs daily at 0:00 UTC (equivalent to 7:00 AM UTC+7)
    catchup=False
)

# Task 1: Run ndjson_to_archive_csv.py
run_ndjson_task = BashOperator(
    task_id='run_ndjson_to_csv_script',
    bash_command='python3 /home/iamhuy_bach99/nexar_project/ndjson_to_archive_csv.py auto',
    dag=dag,
)

# Task 2: Run csv_to_table.py after the first task
run_csv_to_table_task = BashOperator(
    task_id='run_csv_to_table_script',
    bash_command='python3 /home/iamhuy_bach99/nexar_project/csv_to_table.py auto',
    dag=dag,
)

# Set up task dependencies
run_ndjson_task >> run_csv_to_table_task
