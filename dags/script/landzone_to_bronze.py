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
    'landzone_to_bronze',
    default_args=default_args,
    description='Move data from landzonevagas to bronzevagas bucket',
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

# Tarefa para mover o arquivo para o bucket 'bronzevagas'
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
