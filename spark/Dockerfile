FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root

# jars for MinIO (s3a) and PostgreSQL (jdbc)
RUN cd /opt/spark/jars \
    && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    && chown spark:spark hadoop-aws-3.3.4.jar aws-java-sdk-bundle-1.12.262.jar postgresql-42.7.4.jar

USER spark
