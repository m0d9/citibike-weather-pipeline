FROM apache/airflow:2.10.5-python3.10

USER root

# Java, so Airflow can run spark-submit
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/java-17

ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow

# same Spark version as the cluster (apache/spark:3.5.9)
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pyspark==3.5.9

# jars for MinIO (s3a) and PostgreSQL (jdbc)
RUN JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && cd "$JARS" \
    && curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

# dbt in its own virtualenv so it does not clash with Airflow's packages
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.7 dbt-postgres==1.11.0
