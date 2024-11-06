# Use a imagem oficial do Airflow
FROM apache/airflow:2.5.1

# Configurar o usuário root para instalação de dependências do sistema
USER root

# Instale o comando ps e defina JAVA_HOME
RUN apt-get update && \
    apt-get install -y procps openjdk-11-jdk && \
    apt-get clean

# Baixar o Hadoop AWS e a dependência do Amazon SDK
RUN curl -o /opt/spark/jars/hadoop-aws-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

# Defina o JAVA_HOME e adicione ao PATH
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Instalar o MinIO Client e outras dependências necessárias
RUN apt-get install -y curl && \
    curl -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x /usr/local/bin/mc

# Alterar para o usuário airflow antes de instalar pacotes Python
USER airflow

# Instalar o PySpark e o provider para Spark no Airflow usando o requirements.txt
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copiar as DAGs para o contêiner
COPY ./dags /opt/airflow/dags
COPY ./plugins /opt/airflow/plugins
COPY ./scraping /opt/airflow/dags/scraping

# Expor as portas necessárias
EXPOSE 8080 9001

# Configurações de inicialização do Airflow
ENTRYPOINT ["bash", "-c", "airflow db init && airflow webserver & airflow scheduler"]
