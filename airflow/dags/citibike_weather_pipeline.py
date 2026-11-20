"""Monthly batch pipeline: Citi Bike trips + NYC daily weather.

download -> MinIO (raw) -> Spark clean -> MinIO (clean) -> Spark load -> Postgres (staging)
-> dbt (core + marts) -> Metabase dashboard
"""
import os
import shutil
import tempfile
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

TRIPS_URL = "https://s3.amazonaws.com/tripdata/{name}"
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
CENTRAL_PARK = {"latitude": 40.7812, "longitude": -73.9665}
WEATHER_VARS = [
    "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
    "precipitation_sum", "rain_sum", "snowfall_sum", "wind_speed_10m_max",
]

# month to process: the run's data interval, or a month given when triggering by hand
MONTH = "{{ params.month or data_interval_start.strftime('%Y-%m') }}"


def get_month(context):
    return context["params"].get("month") or context["data_interval_start"].strftime("%Y-%m")


def minio():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        region_name="us-east-1",
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


def create_buckets():
    s3 = minio()
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    for bucket in ["raw", "clean"]:
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
            print(f"created bucket {bucket}")


def extract_csvs(zip_path, out_dir):
    # the monthly zip holds one or more csv files (sometimes inside another zip)
    out_dir.mkdir(parents=True, exist_ok=True)
    files = []
    with zipfile.ZipFile(zip_path) as z:
        for member in z.namelist():
            name = Path(member).name
            if member.startswith("__MACOSX") or name.startswith("."):
                continue
            if name.endswith(".csv") or name.endswith(".zip"):
                target = out_dir / name
                with z.open(member) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                if name.endswith(".csv"):
                    files.append(target)
                else:
                    files += extract_csvs(target, out_dir / name[:-4])
    return files


def download_trips(**context):
    month = get_month(context)
    ym = month.replace("-", "")
    prefix = f"citibike/{month}/"
    s3 = minio()

    with tempfile.TemporaryDirectory() as tmp:
        zip_path = Path(tmp) / "trips.zip"
        for name in [f"{ym}-citibike-tripdata.zip", f"{ym}-citibike-tripdata.csv.zip"]:
            response = requests.get(TRIPS_URL.format(name=name), stream=True, timeout=120)
            if response.status_code == 200:
                print(f"downloading {name}")
                with open(zip_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
                break
        else:
            raise FileNotFoundError(f"no Citi Bike file found for {month}")

        csv_files = extract_csvs(zip_path, Path(tmp) / "csv")
        if not csv_files:
            raise ValueError(f"no csv files in the zip for {month}")

        # replace anything uploaded before for this month
        for obj in s3.list_objects_v2(Bucket="raw", Prefix=prefix).get("Contents", []):
            s3.delete_object(Bucket="raw", Key=obj["Key"])
        for csv_file in csv_files:
            s3.upload_file(str(csv_file), "raw", prefix + csv_file.name)
        print(f"uploaded {len(csv_files)} files to raw/{prefix}")


def download_weather(**context):
    month = get_month(context)
    start = date.fromisoformat(f"{month}-01")
    end = (start + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    params = {
        **CENTRAL_PARK,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(WEATHER_VARS),
        "timezone": "America/New_York",
    }
    response = requests.get(WEATHER_URL, params=params, timeout=60)
    response.raise_for_status()

    minio().put_object(Bucket="raw", Key=f"weather/{month}.json", Body=response.content)
    print(f"uploaded raw/weather/{month}.json")


SPARK_SUBMIT = (
    "spark-submit --master $SPARK_MASTER_URL "
    "--conf spark.driver.host=$SPARK_DRIVER_HOST "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC "
    "--conf spark.executor.extraJavaOptions=-Duser.timezone=UTC "
    "--conf spark.executor.memory=1g "
    "--conf spark.executor.cores=2 "
)
DBT = "/home/airflow/dbt-venv/bin/dbt"

with DAG(
    dag_id="citibike_weather_pipeline",
    description="Citi Bike trips and NYC weather: MinIO -> Spark -> Postgres -> dbt -> Metabase",
    schedule="@monthly",
    start_date=datetime.strptime(os.environ.get("PIPELINE_START_MONTH", "2025-01"), "%Y-%m"),
    end_date=datetime.strptime(os.environ.get("PIPELINE_END_MONTH", "2025-06"), "%Y-%m"),
    catchup=True,
    max_active_runs=1,
    params={"month": ""},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["citibike"],
) as dag:
    buckets = PythonOperator(task_id="create_buckets", python_callable=create_buckets)

    trips = PythonOperator(task_id="download_trips", python_callable=download_trips)

    weather = PythonOperator(task_id="download_weather", python_callable=download_weather)

    clean = BashOperator(
        task_id="spark_clean",
        bash_command=SPARK_SUBMIT + "/opt/airflow/jobs/clean_data.py --month " + MONTH,
    )

    load = BashOperator(
        task_id="spark_load_warehouse",
        bash_command=SPARK_SUBMIT + "/opt/airflow/jobs/load_warehouse.py --month " + MONTH,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd /opt/airflow/dbt && {DBT} build --profiles-dir . --target-path /tmp/dbt/target --log-path /tmp/dbt/logs",
    )

    dashboard = BashOperator(
        task_id="publish_dashboard",
        bash_command="python /opt/airflow/metabase/setup_dashboard.py",
    )

    buckets >> [trips, weather] >> clean >> load >> dbt_build >> dashboard
