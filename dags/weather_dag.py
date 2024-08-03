from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import psycopg2



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_data_quality():
    conn = psycopg2.connect(
        dbname="weatherdb",
        user="USER_NAME",
        password="PASSWORD",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Check for null values
    cur.execute("""
        SELECT COUNT(*) 
        FROM weather_data 
        WHERE city IS NULL OR temperature IS NULL OR humidity IS NULL
    """)
    null_count = cur.fetchone()[0]

    # Check for out-of-range values
    cur.execute("""
        SELECT COUNT(*) 
        FROM weather_data 
        WHERE temperature < -100 OR temperature > 100 OR humidity < 0 OR humidity > 100
    """)
    out_of_range_count = cur.fetchone()[0]

    conn.close()

    if null_count > 0 or out_of_range_count > 0:
        raise ValueError(
            f"Data quality check failed: {null_count} null values and {out_of_range_count} out-of-range values found")

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A DAG for the weather data pipeline',
    schedule_interval=None,
)

start_kafka_producer = BashOperator(
    task_id='start_kafka_producer',
    bash_command='python /weather-data-pipeline/scripts/weather_data_producer.py',
    dag=dag,
)

start_spark_job = BashOperator(
    task_id='start_spark_job',
    bash_command='/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.7.3 --jars /weather-data-pipeline/lib/postgresql-42.7.3.jar --driver-class-path /weather-data-pipeline/lib/postgresql-42.7.3.jar /weather-data-pipeline/scripts/weather_data_processor.py',
    env={
        'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-amd64',
        'SPARK_HOME': '/opt/spark',
        'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-11-openjdk-amd64/bin:/opt/spark/bin'
    },
    dag=dag,
)

generate_visual = BashOperator(
    task_id='generate_visual',
    bash_command='python /weather-data-pipeline/scripts/weather_visualizations.py',
    dag=dag,
)

check_data_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)


# Define the new task dependencies
start_kafka_producer >> check_data_quality >> generate_visual
start_spark_job >> check_data_quality 