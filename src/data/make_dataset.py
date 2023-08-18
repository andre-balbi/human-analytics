import datetime
import glob
import math

import pandas as pd
import pymysql
from sqlalchemy import text
from sqlalchemy.engine import create_engine

# **Note:** Initially I will not connect directly to the minIO
# This is just a test to verify if the connection and inspect the files

# --------------------------------------------------------------
# Connecting with the database (in loco)
# --------------------------------------------------------------

mysql_server = "127.0.0.1"  # IP address of the database server
mysql_login = "root"  # Login name of the database server
mysql_password = "0000"  # Password of the database server
mysql_name = "employees"  # Name of the database

engine = create_engine(
    f"mysql+pymysql://{mysql_login}:{mysql_password}@{mysql_server}:3307/{mysql_name}"
)

# --------------------------------------------------------------
# Creating the features: satisfaction_level e last_evaluation
# --------------------------------------------------------------

df_performance_evaluation = pd.read_json(
    "../../datalake/landing/performance-evaluation/employee_performance_evaluation.json",
    orient="records",
    lines=True,
)

df_performance_evaluation.head()

# --------------------------------------------------------------
# Defining query to return the number of projects per employee
# --------------------------------------------------------------

# This SQL query above calculates the number of projects for each employee (emp_id)
# in the "projects_emp" table.
# It counts the occurrences of each unique emp_id and presents the result as
# "number_projects".

query = """SELECT emp_id, Count(PROJECT_ID) as number_projects
FROM projects_emp
GROUP BY (emp_id);"""

df_number_projects = pd.read_sql_query(query, engine)

df_number_projects.head()

# --------------------------------------------------------------
# Creating the feature: mean_work_last_3_months
# --------------------------------------------------------------

# Average hours worked by each employee in the last 3 mese
df_sistema_ponto = pd.DataFrame(data=None, columns=["emp_id", "data", "hora"])  # empty

# Read the .xlsx spreadsheet data
for sheet in glob.glob("../../datalake/landing/working-hours/*.xlsx"):
    print(sheet)
    df_ = pd.read_excel(sheet)
    df_sistema_ponto = pd.concat([df_sistema_ponto, df_])

df_sistema_ponto.tail()


# Converting the attribute to the Datetime
df_sistema_ponto["hora"] = pd.to_numeric(df_sistema_ponto["hora"])
df_sistema_ponto.info()

# Filtering only records of the last 3 months
df_last_3_month = df_sistema_ponto[
    (df_sistema_ponto["data"] > datetime.datetime(2020, 9, 30))
]
df_last_3_month.head()

# Checking the counting of records per employee
df_last_3_month.groupby("emp_id").count()

# Finally, calculating the average value of the amount of hours in the last 3 months.
mean_work_last_3_months = df_last_3_month.groupby("emp_id")["hora"].agg("sum") / 3
mean_work_last_3_months.head()

# --------------------------------------------------------------
# Creating the feature: time_in_company
# --------------------------------------------------------------

# Calculating the time each employee is in the company

# Defining a reference date
date_referencia = datetime.date(2021, 1, 1)

# Defining a query to return data from the "employees" table
query = """SELECT hire_date
FROM employees;"""

df_hire_date = pd.read_sql_query(query, engine)
df_hire_date.head()

# Converting the type of data to Datetime.
df_hire_date["hire_date"] = pd.to_datetime(df_hire_date["hire_date"])
df_hire_date.info()

# Calculating the difference in days from the employee's hiring date to the reference date
days_diff = []
for d in df_hire_date["hire_date"]:  # Looping thrrough the employee's hiring date
    diff = date_referencia - d.date()  # Reference date - employee's hiring date
    days_diff.append(diff.days)

days_diff[:10]

# Number of days in number of years
nyears = []
for ndays in days_diff:
    nyears.append(int(math.ceil(ndays / 365)))

nyears[:20]

# Adding the new column to the dataframe
df_hire_date["time_in_company"] = nyears
df_hire_date.head()

# --------------------------------------------------------------
# Creating the feature: work_accident (binary)
# --------------------------------------------------------------

# Loading the data from the database
df_employees = pd.read_sql_table("employees", engine)
df_accident = pd.read_sql_table("accident", engine)

print(df_employees.head(2))
print(df_accident.head(2))

# Verifying which employees had an accident (1-yes; 0-no)
work_accident = []
# Iterating through each emp_no in the df_employees DataFrame
for emp in df_employees["emp_no"]:
    # Checking if the emp_no exists in the list of emp_no values from df_accident
    if emp in df_accident["emp_no"].to_list():
        # Appending 1 to the work_accident list if there is a work accident for the emp_no
        work_accident.append(1)
    else:
        # Appending 0 to the work_accident list if there is no work accident for the emp_no
        work_accident.append(0)

# Adding the new column to the mew dataframe
df_work_accident = pd.DataFrame(data=None, columns=["work_accident"])
df_work_accident["work_accident"] = work_accident
df_work_accident.groupby(work_accident).count()

# --------------------------------------------------------------
# Creating the features: departament, salary and left
# --------------------------------------------------------------

# This query combines data from two tables, "employees" and "salaries", using an
# inner join. The result will include the "department" column from the "employees"
# table, the "salary" column from the "salaries" table, and the "left" column
# (employment status) from the "employees" table. The join is established by matching
# the "emp_no" column from the "employees" table with the "emp_id" column from the
# "salaries" table. This way, the query presents a comprehensive view of employee
# department, salary, and employment status in a single result set.

query = """SELECT emp.department as department,sal.salary as salary, emp.left
FROM employees emp
INNER JOIN salaries sal
ON emp.emp_no = sal.emp_id;
"""

df_department_salary_left = pd.read_sql_query(query, engine)
df_department_salary_left.head()
