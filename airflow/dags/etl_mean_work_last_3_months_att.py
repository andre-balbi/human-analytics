import datetime
from io import BytesIO

import pandas as pd
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from minio import Minio

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2021, 1, 13),
}

dag = DAG(
    "etl_mean_work_last_3_months_att",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
)

data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

client = Minio(
    data_lake_server,
    access_key=data_lake_login,
    secret_key=data_lake_password,
    secure=False,
)


def extract():

    """The function is responsible for extracting data from Excel files, concatenating
    them into a DataFrame, and then persisting the resulting DataFrame as a CSV file."""

    # # List objects in the "landing" location with the given prefix and recursion settings
    df_working_hours = pd.DataFrame(data=None, columns=["emp_id", "data", "hora"])

    # List objects in the "landing" location with the given prefix and recursion settings
    objects = client.list_objects("landing", prefix="working-hours", recursive=True)

    # Iterate through the list of objects
    for obj in objects:
        # Displaying a message indicating that a file is being downloaded
        print("Downloading file...")
        print(obj.bucket_name, obj.object_name.encode("utf-8"))

        # Retrieving the object using the client and its encoded object name
        obj = client.get_object(
            obj.bucket_name,
            obj.object_name.encode("utf-8"),
        )
        data = obj.read()

        # Using Pandas to read the Excel data into a DataFrame
        df_ = pd.read_excel(data)

        # Concatenating the downloaded DataFrame with the existing DataFrame (df_working_hours)
        df_working_hours = pd.concat([df_working_hours, df_])

    # Persisting the dataset in the Staging area as a CSV file.
    df_working_hours.to_csv("/tmp/mean_work_last_3_months.csv", index=False)


def transform():

    """The function is responsible for transforming data stored in a CSV file, calculating
    the average working hours for the last 3 months, and then persisting the transformed
    data."""

    # Read the data from the Staging area CSV file into a DataFrame named "df_".
    df_ = pd.read_csv("/tmp/mean_work_last_3_months.csv")

    # Convert the "hora" column to numeric format and the "data" column to datetime format.
    df_["hora"] = pd.to_numeric(df_["hora"])
    df_["data"] = pd.to_datetime(df_["data"])

    # Filter only the records from the last 3 months.
    df_last_3_month = df_[(df_["data"] > datetime.datetime(2020, 9, 30))]

    # Calculate the average working hours for the last 3 months, grouped by "emp_id".
    mean_work_last_3_months = df_last_3_month.groupby("emp_id")["hora"].agg("sum") / 3

    # Create a DataFrame with the transformed data.
    mean_work_last_3_months = pd.DataFrame(data=mean_work_last_3_months)
    mean_work_last_3_months.rename(
        columns={"hora": "mean_work_last_3_months"}, inplace=True
    )

    # Persist the transformed data in the Staging area as a CSV file.
    mean_work_last_3_months.to_csv("/tmp/mean_work_last_3_months.csv", index=False)


def load():

    """The function is responsible for loading transformed data from a CSV file,
    converting it to the Parquet format, and then uploading it to a Data Lake."""

    # Load the data from the Staging area CSV file into a DataFrame named "df_".
    df_ = pd.read_csv("/tmp/mean_work_last_3_months.csv")

    # Convert the data to the Parquet format and save it to a Parquet file.
    df_.to_parquet("/tmp/mean_work_last_3_months.parquet", index=False)

    # Load the Parquet file to the Data Lake using the client's "fput_object" method.
    client.fput_object(
        "processing",
        "mean_work_last_3_months.parquet",
        "/tmp/mean_work_last_3_months.parquet",
    )


extract_task = PythonOperator(
    task_id="extract_file_from_data_lake",
    provide_context=True,
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data", provide_context=True, python_callable=transform, dag=dag
)

load_task = PythonOperator(
    task_id="load_file_to_data_lake",
    provide_context=True,
    python_callable=load,
    dag=dag,
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.json;rm -f /tmp/*.parquet;",
    dag=dag,
)

extract_task >> transform_task >> load_task >> clean_task
