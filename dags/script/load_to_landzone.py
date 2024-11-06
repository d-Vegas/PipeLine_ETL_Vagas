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
    'load_to_landzone',
    default_args=default_args,
    description='Move CSV file to MinIO landzonevagas bucket',
    schedule_interval='@daily',  # Executa uma vez por dia
)

# Tarefa para configurar o alias do MinIO
configure_minio = BashOperator(
    task_id='configure_minio',
    bash_command="""
    mc alias set minio http://minio:9000 minioadmin minioadmin
    """,
    dag=dag,
)

# Tarefa para mover o arquivo para o bucket 'landzonevagas'
move_to_landzone = BashOperator(
    task_id='move_to_landzone',
    bash_command="""
    mc cp /opt/airflow/dags/scraping/vagas_scrap.csv minio/landzonevagas/
    """,
    retries=3,
    retry_delay=timedelta(minutes=1),
    dag=dag,
)

configure_minio >> move_to_landzone
