import os

from pyspark.sql import SparkSession

# lake locations (MinIO buckets); can be changed to local folders for testing
RAW = os.environ.get("LAKE_RAW", "s3a://raw")
CLEAN = os.environ.get("LAKE_CLEAN", "s3a://clean")


def get_spark(app_name):
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
    )

    endpoint = os.environ.get("MINIO_ENDPOINT")
    if endpoint:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    return builder.getOrCreate()
