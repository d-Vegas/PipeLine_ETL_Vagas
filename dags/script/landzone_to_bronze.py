from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
}

dag = DAG(
    'bronze_dag',
    default_args=default_args,
    description='Move data from landzonevagas to bronzevagas bucket',
    schedule_interval='@daily',  
)


configure_minio = BashOperator(
    task_id='configure_minio',
    bash_command="""
    mc alias set minio http://minio:9000 minioadmin minioadmin
    """,
    dag=dag,
)


move_to_bronze = BashOperator(
    task_id='move_to_bronze',
    bash_command="""
    mc cp minio/landzonevagas/vagas_scrap.csv minio/bronzevagas/
    """,
    retries=3,
    retry_delay=timedelta(minutes=1),
    dag=dag,
)

configure_minio >> move_to_bronze
