from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace, when
from pyspark.sql.types import StringType
import re
import unicodedata

def process_to_silver():
    try:
        bucket_name = 'bronzevagas'
        input_key = 'vagas_scrap.csv'
        output_bucket = 'silverbucket'
        output_key = 'vagas_silver.parquet'

        s3_hook = S3Hook(aws_conn_id='minio_connnn')
        response = s3_hook.get_key(input_key, bucket_name)
        
        spark = SparkSession.builder.appName("process_silver").getOrCreate()

        csv_bytes = response.get()['Body'].read()
        df = spark.read.csv(BytesIO(csv_bytes), sep=';', header=True, inferSchema=True)

        def remove_accents(text):
            if not text:
                return text
            nfkd_form = unicodedata.normalize('NFKD', text)
            return "".join([c for c in nfkd_form if not unicodedata.combining(c)])
        
        remove_accents_udf = udf(remove_accents, StringType())
        df = df.select([remove_accents_udf(col(c)).alias(c) if df.schema[c].dataType == StringType() else col(c) for c in df.columns])
        
        df = df.dropDuplicates()

        relevant_roles = [
            "Engenheiro de Dados", "Engenharia de Dados", "Analista de Dados", "Analista de Engenharia de Dados",
            "Cientista de Dados", "Data Scientist", "Data Analyst", "Data Engineer", "engenharia de dados", 
            "bolsista", "estagio", "estagiario", "pesquisador de dados", "data developer", 
            "engenheiro(a) de dados", "engenheiro big data", "analista big data", "cientista big data",
            "engenheiro de machine learning", "machine learning engineer", "estagiario de dados",
            "analista de business intelligence", "bi analyst", "analista de bi", "engenheiro de inteligencia artificial",
            "engenheiro de aprendizado de maquina", "inteligencia de dados", "desenvolvedor de dados",
            "engenheiro de dados sênior", "engenheiro de dados pleno", "engenheiro de dados jr",
            "engenheiro de dados junior", "especialista em dados", "consultor de dados", "data consultant",
            "coordenador de dados", "gerente de dados", "gestor de dados", "engenheiro de armazenamento de dados",
            "engenheiro de banco de dados", "arquiteto de dados", "administrador de dados",
            "analista de qualidade de dados", "analista de sistemas de dados"
        ]
        
        df = df.filter(df['nome_vaga'].rlike('|'.join(re.escape(role) for role in relevant_roles)))

        df = df.withColumn('empresa', col('empresa').cast(StringType()).upper())

        today = datetime.today()

        def calculate_date(text):
            if not text:
                return None
            if "publicada" in text.lower():
                text = re.sub(r'publicada\s*em\s*', '', text, flags=re.IGNORECASE)
            
            if re.match(r"\d{4}-\d{2}-\d{2}", text):
                return datetime.strptime(text, "%Y-%m-%d").strftime('%d/%m/%Y')
            elif "ha" in text.lower():
                days_ago = int(re.search(r'(\d+)', text).group(1))
                return (today - timedelta(days=days_ago)).strftime('%d/%m/%Y')
            elif "ontem" in text.lower():
                return (today - timedelta(days=1)).strftime('%d/%m/%Y')
            elif "hoje" in text.lower():
                return today.strftime('%d/%m/%Y')
            return text
        
        calculate_date_udf = udf(calculate_date, StringType())
        df = df.withColumn('data_publicacao', calculate_date_udf(col('data_publicacao')))

        def standardize_contract(model):
            if not model:
                return model
            model = model.lower()
            if "clt" in model or "profissional" in model:
                return "CLT"
            elif "pessoa juridica" in model or "pj" in model:
                return "PJ"
            elif "estagio" in model:
                return "ESTÁGIO"
            return model

        standardize_contract_udf = udf(standardize_contract, StringType())
        df = df.withColumn('modelo_contrato', standardize_contract_udf(col('modelo_contrato')))
        
        df = df.fillna({'nivel_experiencia': "Pleno"})

        # Função para padronizar localizações
        def process_location(loc):
            if not loc:
                return loc
            loc = re.sub(r"\b[A-Z]{2}\b$", "", loc).strip()
            loc = loc.split(',')[0].strip()
            loc = re.sub(r"\d{2}:\d{2}(?:\s*-\s*\d{2}:\d{2})*", "", loc).strip()
            if re.search(r'\d+h', loc):
                return "100% Home Office"
            return loc

        process_location_udf = udf(process_location, StringType())
        df = df.withColumn('localizacao', process_location_udf(col('localizacao')))
        df = df.fillna({'beneficios': "Não informado"})

        # Salvando o DataFrame como Parquet na camada Silver do MinIO
        output_path = f"s3a://{output_bucket}/{output_key}"
        df.write.mode("overwrite").parquet(output_path)

    except Exception as e:
        print(f"Erro ao processar os dados para a camada Silver: {e}")
        raise e

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'silver_dag',
    default_args=default_args,
    description='Processa os dados da camada Bronze para Silver usando Spark',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 12),
    catchup=False,
) as dag:
   
    wait_for_bronze = ExternalTaskSensor(
        task_id='wait_for_bronze',
        external_dag_id='bronze_dag',
        external_task_id=None,
        mode='poke',
        poke_interval=30,
        timeout=600
    )

    process_silver_task = PythonOperator(
        task_id='process_silver',
        python_callable=process_to_silver,
    )

    wait_for_bronze >> process_silver_task
