import math
from datetime import date, datetime, timedelta
from io import BytesIO

import pandas as pd
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from minio import Minio
from sqlalchemy.engine import create_engine

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 13),
}

dag = DAG(
    "etl_time_in_company_att", default_args=DEFAULT_ARGS, schedule_interval="@once"
)

data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

database_server = Variable.get("database_server")
database_login = Variable.get("database_login")
database_password = Variable.get("database_password")
database_name = Variable.get("database_name")


url_connection = "mysql+pymysql://{}:{}@{}/{}".format(
    str(database_login),
    str(database_password),
    str(database_server),
    str(database_name),
)

engine = create_engine(url_connection)

client = Minio(
    data_lake_server,
    access_key=data_lake_login,
    secret_key=data_lake_password,
    secure=False,
)


def extract():

    """Extracts hire date data from a database using a SQL query and saves it to a
    CSV file in the Staging area."""

    # Define a SQL query to retrieve the hire date from the "employees" table.
    query = """SELECT hire_date
            FROM employees;"""

    # Execute the SQL query using the provided database engine and load the result into
    # a DataFrame named "df_".
    df_ = pd.read_sql_query(query, engine)

    # # Persist the DataFrame into a CSV file in the Staging area with the filename
    # "time_in_company.csv".
    df_.to_csv("/tmp/time_in_company.csv", index=False)


def transform():

    """Transforms hire date data, calculates time in company, and saves the result
    to a CSV file."""

    # Load the data from the Staging area CSV file into a DataFrame named "df_".
    df_ = pd.read_csv("/tmp/time_in_company.csv")

    # Convert the hire date data to the datetime format.
    df_["hire_date"] = pd.to_datetime(df_["hire_date"])

    # Define a reference date.
    date_referencia = date(2021, 1, 1)

    # Calculate the difference in days between the hire date and the reference date.
    days_diff = []
    for d in df_["hire_date"]:
        diff = date_referencia - d.date()
        days_diff.append(diff.days)

    # Convert the difference to years by dividing days by 365 and rounding up.
    nyears = []
    for ndays in days_diff:
        nyears.append(int(math.ceil(ndays / 365)))

    # Assign the calculated time in company data to the DataFrame.
    df_["time_in_company"] = nyears

    # Persist the DataFrame containing the time in company data back to the Staging
    # area CSV file.
    df_[["time_in_company"]].to_csv("/tmp/time_in_company.csv", index=False)


def load():

    """Loads data from a CSV file, converts to Parquet format, and uploads to
    a Data Lake."""

    # Load the data from the Staging area CSV file into a DataFrame named "df_".
    df_ = pd.read_csv("/tmp/time_in_company.csv")

    # Convert the data to the Parquet format and save it to a Parquet file.
    df_.to_parquet("/tmp/time_in_company.parquet", index=False)

    # Load the Parquet file to the Data Lake using the client's "fput_object" method.
    client.fput_object(
        "processing",  # Destination where the file will be saved
        "time_in_company.parquet",  # Name of the file
        "/tmp/time_in_company.parquet",  # File address
    )


extract_task = PythonOperator(
    task_id="extract_data_from_database",
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
