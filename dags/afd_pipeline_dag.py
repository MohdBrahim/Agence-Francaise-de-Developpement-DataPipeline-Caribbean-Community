from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_kafka_producer():
    """Run Kafka Producer"""
    subprocess.run(['python', '/opt/airflow/scripts/API_kafka.py'], check=True)

def run_spark_processor():
    """Run Spark Processor"""
    # You can use BashOperator or SparkSubmitOperator here
    subprocess.run(['spark-submit', '/opt/airflow/scripts/spark_processor.py'], check=True)

with DAG(
    'afd_data_pipeline',
    default_args=default_args,
    description='AFD Data Processing Pipeline',
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:

    start = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "Starting AFD Pipeline"',
    )

    kafka_producer = PythonOperator(
        task_id='run_kafka_producer',
        python_callable=run_kafka_producer,
    )

    spark_processor = PythonOperator(
        task_id='run_spark_processor',
        python_callable=run_spark_processor,
    )

    end = BashOperator(
        task_id='end_pipeline',
        bash_command='echo "Pipeline completed successfully"',
    )

    start >> kafka_producer >> spark_processor >> end
