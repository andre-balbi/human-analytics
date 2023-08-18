from datetime import date, datetime, timedelta
from io import BytesIO

import pandas as pd
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from minio import Minio
from sqlalchemy.engine import create_engine

from airflow import DAG

# Define default arguments to be used across tasks in the DAG.
DEFAULT_ARGS = {
    "owner": "Airflow",  # Owner of the DAG
    "depends_on_past": False,  # Whether tasks depend on past runs (not in this case)
    "start_date": datetime(
        2021, 1, 13
    ),  # Start date of the DAG (lower than the current date)
}

# Create the DAG instance with the specified ID, default arguments, and schedule interval.
dag = DAG(
    "etl_department_salary_left_att",  # ID of the DAG
    default_args=DEFAULT_ARGS,  # Default arguments to be used
    schedule_interval="@once",  # How often the DAG should run (in this case, run only once)
)

# Retrieving connection information for the data lake from Airflow Variables.
data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

# Retrieving connection information for the database from Airflow Variables.
database_server = Variable.get("database_server")
database_login = Variable.get("database_login")
database_password = Variable.get("database_password")
database_name = Variable.get("database_name")

# Creating a URL connection string for the database using retrieved variables.
url_connection = "mysql+pymysql://{}:{}@{}/{}".format(
    str(database_login),
    str(database_password),
    str(database_server),
    str(database_name),
)

# Creating a database engine using the connection string.
engine = create_engine(url_connection)

# Establishing a connection with the Minio data lake service using the retrieved variables.
client = Minio(
    data_lake_server,
    access_key=data_lake_login,
    secret_key=data_lake_password,
    secure=False,
)


def extract():

    """The function performs data extraction from a database using a SQL query, and then
    persists the extracted data into a CSV file in the Staging area."""

    # SQL query to retrieve data.
    query = """SELECT emp.department as department,sal.salary as salary, emp.left
            FROM employees emp
            INNER JOIN salaries sal
            ON emp.emp_no = sal.emp_id;"""

    # Execute the SQL query and load the result into a DataFrame named "df_" using the
    # provided database engine.
    df_ = pd.read_sql_query(query, engine)

    # Persist the DataFrame into a CSV file in the Staging area.
    df_.to_csv("/tmp/department_salary_left.csv", index=False)


def load():

    """The function is responsible for loading data from a CSV file in the Staging area,
    converting it to the Parquet format, and then uploading it to a Data Lake."""

    # Load the data from the Staging area CSV file into a DataFrame named "df_".
    df_ = pd.read_csv("/tmp/department_salary_left.csv")

    # Convert the data to the Parquet format and save it to a Parquet file.
    df_.to_parquet("/tmp/department_salary_left.parquet", index=False)

    # Load the Parquet file to the Data Lake using the client's "fput_object" method.
    client.fput_object(
        "processing",  # Where will be saved the file
        "department_salary_left.parquet",  # Name of the file
        "/tmp/department_salary_left.parquet",  # File Address
    )


# Create a task named "extract_task" using the PythonOperator.
extract_task = PythonOperator(
    task_id="extract_data_from_database",  # Task ID
    provide_context=True,  # Provide context to the Python callable
    python_callable=extract,  # The Python function to call
    dag=dag,  # Specify the DAG
)

# Create a task named "load_task" using the PythonOperator.
load_task = PythonOperator(
    task_id="load_file_to_data_lake",  # Task ID
    provide_context=True,  # Provide context to the Python callable
    python_callable=load,  # The Python function to call
    dag=dag,  # Specify the DAG
)

# This task runs a Bash command to clean temporary files in the Staging area.
clean_task = BashOperator(
    task_id="clean_files_on_staging",  # Task ID
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.json;rm -f /tmp/*.parquet;",  # Bash command to clean files
    dag=dag,  # Specify the DAG
)

# Set task dependencies: "extract_task" must complete before "load_task",
# and "load_task" must complete before "clean_task".
extract_task >> load_task >> clean_task
