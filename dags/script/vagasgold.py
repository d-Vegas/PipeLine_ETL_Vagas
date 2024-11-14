from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, when, avg, count

def process_to_gold():
    try:
        # Configuração de bucket e arquivos
        bucket_name = 'silverbucket'
        input_key = 'vagas_silver.parquet'
        output_bucket = 'goldbucket'
        output_key = 'vagas_gold.parquet'
        
        # Criando a sessão Spark
        spark = SparkSession.builder \
            .appName("Process Silver to Gold") \
            .getOrCreate()
        
        # Lendo dados parquet do S3
        s3_hook = S3Hook(aws_conn_id='minio_connnn')
        s3_path = f"s3a://{bucket_name}/{input_key}"
        df = spark.read.parquet(s3_path)
        
        habilidadesRelevantes = [
            "python", "sql", "machineLearning", "bigData", "powerBi", "tableau",
            "spark", "aws", "gcp", "azure", "hadoop", "kafka", "dataWarehouse",
            "etl", "dataVisualization", "statistics", "dataMining", "dataEngineering",
            "dataAnalysis", "businessIntelligence", "deepLearning", "docker", "kubernetes"
        ]
        
        skills_expr = [when(lower(col("descricao_vaga")).contains(skill.lower()), skill) for skill in habilidadesRelevantes]
        df = df.withColumn("principais_habilidades", explode(split(col("descricao_vaga"), "\\s+")))
        df = df.filter(col("principais_habilidades").isin(habilidadesRelevantes)).groupBy("descricao_vaga").agg(collect_list("principais_habilidades"))

        if 'salario' in df.columns:
            salario_por_cargo = df.groupBy("nome_vaga").agg(avg("salario").alias("media_salarial"))
            df = df.join(salario_por_cargo, on="nome_vaga", how="left")

        localizacao_counts = df.groupBy("localizacao").agg(count("*").alias("quantidade_vagas_por_localizacao"))
        df = df.join(localizacao_counts, on="localizacao", how="left")

        contrato_counts = df.groupBy("modelo_contrato").agg(count("*").alias("quantidade_por_contrato"))
        df = df.join(contrato_counts, on="modelo_contrato", how="left")

        experiencia_counts = df.groupBy("nivel_experiencia").agg(count("*").alias("quantidade_por_nivel_experiencia"))
        df = df.join(experiencia_counts, on="nivel_experiencia", how="left")

        df = df.withColumn(
            "modalidade_trabalho",
            when(lower(col("localizacao")).contains("home office"), "Home Office")
            .when(lower(col("localizacao")).contains("híbrido"), "Híbrido")
            .otherwise("Presencial")
        )
        
        modalidade_counts = df.groupBy("modalidade_trabalho").agg((count("*") / df.count()).alias("percentual_vagas"))
        df = df.join(modalidade_counts, on="modalidade_trabalho", how="left")

        demanda_por_cargo = df.groupBy("nome_vaga").agg(count("*").alias("quantidade_vagas_por_cargo"))
        df = df.join(demanda_por_cargo, on="nome_vaga", how="left")

        output_path = f"s3a://{output_bucket}/{output_key}"
        df.write.mode("overwrite").parquet(output_path)
        
        spark.stop()

    except Exception as e:
        print(f"Erro ao processar os dados para a camada Gold: {e}")
        raise e

# Definição da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gold_dag',
    default_args=default_args,
    description='Processa os dados da camada Silver para Gold usando Spark',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 13),
    catchup=False,
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id='wait_for_silver',
        external_dag_id='silver_dag',
        external_task_id=None,
        mode='poke',
        poke_interval=30,
        timeout=600
    )

    process_gold_task = PythonOperator(
        task_id='process_gold',
        python_callable=process_to_gold,
    )

    wait_for_silver >> process_gold_task
