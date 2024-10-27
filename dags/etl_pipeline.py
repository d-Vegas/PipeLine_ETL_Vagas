from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import boto3

# Função para executar o scraping
def extract_data():
    # Caminho para o script de scraping
    script_path = '/opt/airflow/dags/Scraping/scraping_vagas_selenium.py'
    
    # Executar o script de scraping para gerar o CSV
    os.system(f'python {script_path}')

    # Caminho do arquivo CSV gerado
    csv_file_path = '/opt/airflow/dags/Scraping/vagas_scrap.csv'

    # Fazer upload do CSV para o MinIO
    upload_to_minio(csv_file_path)

# Função para fazer upload para o MinIO
def upload_to_minio(file_path):
    minio_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',  # URL do MinIO
        aws_access_key_id='minioadmin',    # Usuário do MinIO
        aws_secret_access_key='minioadmin' # Senha do MinIO
    )

    bucket_name = 'landzone'
    object_name = os.path.basename(file_path)

    try:
        # Verificar se o bucket existe, se não, criar
        if not minio_client.list_buckets():
            minio_client.create_bucket(Bucket=bucket_name)

        # Fazer o upload do arquivo CSV para o bucket 'landzone'
        minio_client.upload_file(file_path, bucket_name, object_name)
        print(f'Arquivo {file_path} enviado para o bucket {bucket_name} com sucesso.')
    except Exception as e:
        print(f'Erro ao enviar o arquivo para o MinIO: {e}')

# Definição do DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 27),
    'retries': 1,
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL com PySpark',
    schedule='@daily', 
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)
