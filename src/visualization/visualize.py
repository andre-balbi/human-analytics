import datetime
import glob

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import sweetviz as sv
from minio import Minio

# --------------------------------------------------------------
# Connecting to Data Lake
# --------------------------------------------------------------

client = Minio(
    "localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
)

# --------------------------------------------------------------
# Downloading Dataset
# --------------------------------------------------------------

client.fget_object(
    "processing",
    "employees_dataset.parquet",
    "temp_.parquet",
)
df = pd.read_parquet("temp_.parquet")

# Organization of the dataset
df = df[
    [
        "department",
        "salary",
        "mean_work_last_3_months",
        "number_projects",
        "satisfaction_level",
        "last_evaluation",
        "time_in_company",
        "work_accident",
        "left",
    ]
]

df.head()

# --------------------------------------------------------------
# Utilizing Sweetviz for initial data exploration
# --------------------------------------------------------------
my_report = sv.analyze(df, "turnover")
my_report.show_html()

# --------------------------------------------------------------
# Verify missing values
# --------------------------------------------------------------

# Verification of missing values
df.isnull().sum()

# Drop all the missing values (just one)
df.dropna(inplace=True)

# --------------------------------------------------------------
# Change the type of the columns
# --------------------------------------------------------------

df["number_projects"] = df["number_projects"].astype(int)
df["mean_work_last_3_months"] = df["mean_work_last_3_months"].astype(int)
df["time_in_company"] = df["time_in_company"].astype(int)
df["work_accident"] = df["work_accident"].astype(int)
df["left"] = df["left"].astype(int)

df.info()

# --------------------------------------------------------------
# Renames the columns
# --------------------------------------------------------------

df = df.rename(
    columns={
        "satisfaction_level": "satisfaction",
        "last_evaluation": "evaluation",
        "number_projects": "projectCount",
        "mean_work_last_3_months": "averageMonthlyHours",
        "time_in_company": "yearsAtCompany",
        "work_accident": "workAccident",
        "left": "turnover",
    }
)

df.head()


# --------------------------------------------------------------
# Calculating the turnover rate
# --------------------------------------------------------------

df.describe()

turnover_rate = df["turnover"].value_counts() / len(df)
turnover_rate

# --------------------------------------------------------------
# Turnover statistics overview
# --------------------------------------------------------------

turnover_summary = df.groupby("turnover").mean()
turnover_summary

# --------------------------------------------------------------
# Calculating the correlation matrix
# --------------------------------------------------------------

corr = df.corr(numeric_only=True)
plt.figure(figsize=(12, 8))
sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
plt.title("Correlation Matrix")
plt.show()

# --------------------------------------------------------------
# Calculating attribute distributions.
# --------------------------------------------------------------

fig, axes = plt.subplots(ncols=3, figsize=(15, 6))

sns.histplot(data=df, x="satisfaction", kde=True, color="g", ax=axes[0]).set_title(
    "Employee Satisfaction"
)
axes[0].set_ylabel("Employee Count")

sns.histplot(data=df, x="evaluation", kde=True, color="r", ax=axes[1]).set_title(
    "Employee Evaluation"
)
axes[1].set_ylabel("Employee Count")

sns.histplot(
    data=df, x="averageMonthlyHours", kde=True, color="b", ax=axes[2]
).set_title("Employee Average Monthly Hours")
axes[2].set_ylabel("Employee Count")
plt.show()

# --------------------------------------------------------------
# Checking the turnover based o salary
# --------------------------------------------------------------

f, ax = plt.subplots(figsize=(15, 4))
sns.countplot(y="salary", hue="turnover", data=df).set_title("Employee Salary Turnover")

# --------------------------------------------------------------
# Checking the turnover in relation to department
# --------------------------------------------------------------

f, ax = plt.subplots(figsize=(15, 5))

plt.xticks(rotation=-45)
sns.countplot(x="department", data=df).set_title("Distribution by departments")

f, ax = plt.subplots(figsize=(15, 5))
sns.countplot(y="department", hue="turnover", data=df).set_title(
    "Departament vs Turnover"
)

# Group by "department" and calculate the percentage of turnover (1)
department_turnover_percentage = (
    df[df["turnover"] == 1].groupby("department")["turnover"].sum()
    / df.groupby("department")["turnover"].count()
    * 100
).sort_values(ascending=False)

department_turnover_percentage

# --------------------------------------------------------------
# Checking the turnover in relation to the number of projects.
# --------------------------------------------------------------

fig = plt.figure(
    figsize=(8, 4),
)
ax = sns.barplot(
    x="projectCount",
    y="projectCount",
    hue="turnover",
    data=df,
    estimator=lambda x: len(x) / len(df) * 100,
)
ax.set(ylabel="Percent")
plt.title("Turnover vs Total Projects")
plt.plot()

# --------------------------------------------------------------
# Checking the turnover in relation to the evaluation note
# --------------------------------------------------------------

fig = plt.figure(figsize=(15, 4))
ax = sns.kdeplot(
    df.loc[df["turnover"] == 0, "evaluation"], color="b", fill=True, label="No Turnover"
)
ax = sns.kdeplot(
    df.loc[df["turnover"] == 1, "evaluation"], color="r", fill=True, label="Turnover"
)
ax.set(xlabel="Employee Evaluation", ylabel="Frequency")
plt.title("Distribution of Employee Evaluation - Turnover vs. No Turnover")
plt.legend()
plt.plot()

# --------------------------------------------------------------
# Verifying the turnover regarding employee satisfaction.
# --------------------------------------------------------------

plt.figure(figsize=(15, 4))
sns.kdeplot(
    data=df,
    x="satisfaction",
    hue="turnover",
    common_norm=False,
    palette=["b", "r"],
    fill=True,
)
plt.title("Employee Satisfaction Distribution - Turnover V.S. No Turnover")
plt.xlabel("Satisfaction")
plt.ylabel("Density")
plt.legend(title="Turnover", labels=["No Turnover", "Turnover"])
plt.show()

# --------------------------------------------------------------
# Checking the relationship between company time and turnover
# --------------------------------------------------------------

fig = plt.figure(figsize=(10, 6))
ax = sns.barplot(
    x="yearsAtCompany",
    y="yearsAtCompany",
    hue="turnover",
    data=df,
    estimator=lambda x: len(x) / len(df) * 100,
)
ax.set(ylabel="Percent")
plt.title("Turnover V.S. YearsAtCompany")
plt.show()


# --------------------------------------------------------------
# Checking the relationship between number of projects and the
# employee's evaluation note
# --------------------------------------------------------------

fig = plt.figure(figsize=(12, 8))
sns.boxplot(x="projectCount", y="evaluation", hue="turnover", data=df)
plt.title("Number of projects and evaluation note")
plt.show()

# Grouping the data and calculating mean evaluation scores
grouped_data = df.groupby(["projectCount", "turnover"]).evaluation.mean().reset_index()
grouped_data

# Plotting the data using a bar graph
plt.figure(figsize=(12, 6))
sns.barplot(x="projectCount", y="evaluation", hue="turnover", data=grouped_data)
plt.title("Mean Evaluation Scores by Project Count and Turnover")
plt.xlabel("Project Count")
plt.ylabel("Mean Evaluation Score")
plt.legend(title="Turnover", loc="upper right")
plt.show()

# --------------------------------------------------------------
# Checking the relationship between employee satisfaction and
# its assessment.
# --------------------------------------------------------------

sns.lmplot(x="satisfaction", y="evaluation", data=df, fit_reg=False, hue="turnover")
plt.title("Satisfaction vs. Evaluation")
plt.xlabel("Satisfaction")
plt.ylabel("Evaluation")
plt.show()

# --------------------------------------------------------------
# Export dataset
# --------------------------------------------------------------

df.to_pickle("../../data/interim/01_cleaned_data.pkl")
