# Importing necessary libraries
import joblib
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
from minio import Minio
from pycaret.classification import load_model, predict_model

# Connecting to Minio Data Lake
client = Minio(
    "localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
)

# Downloading necessary files from Data Lake
client.fget_object("curated", "model.pkl", "model.pkl")  # Downloading the trained model
client.fget_object("curated", "dataset.csv", "dataset.csv")  # Downloading the dataset
client.fget_object(
    "curated", "cluster.joblib", "cluster.joblib"
)  # Downloading the cluster model

# Variables for model, dataset, and cluster
var_model = "model"  # Name of the trained model file
var_model_cluster = "cluster.joblib"  # Name of the cluster model file
var_dataset = "dataset.csv"  # Name of the dataset file

# Loading trained model and cluster
model = load_model(var_model)  # Loading the trained model
model_cluster = joblib.load(var_model_cluster)  # Loading the cluster model

# Loading the dataset
dataset = pd.read_csv(var_dataset)  # Reading the dataset from the CSV file

# Printing the first few rows of the dataset
print(dataset.head())

# Title of the app
st.title("Human Resource")

# Subtitle introducing the app's purpose
st.markdown(
    "This is a Data App used to showcase the Machine Learning solution for the Human Resource turnover problem."
)
st.markdown("---")

# Displaying original dataset header
st.markdown("**Original Dataset**")
st.dataframe(dataset.head())  # Displaying the first few rows of the dataset
st.markdown("****")

# Define colors for employee groups based on cluster labels
kmeans_colors = [
    "green" if c == 0 else "red" if c == 1 else "blue" for c in model_cluster.labels_
]

# Sidebar to input employee attributes for prediction
st.sidebar.subheader("Define the employee attributes for predicting turnover.")
# Input fields for employee attributes
satisfaction = st.sidebar.number_input(
    "satisfaction", value=dataset["satisfaction"].mean()
)
evaluation = st.sidebar.number_input("evaluation", value=dataset["evaluation"].mean())
averageMonthlyHours = st.sidebar.number_input(
    "averageMonthlyHours", value=dataset["averageMonthlyHours"].mean()
)
yearsAtCompany = st.sidebar.number_input(
    "yearsAtCompany", value=dataset["yearsAtCompany"].mean()
)

# Button to perform classification
btn_predict = st.sidebar.button("Perform Classification")

# Check if the button is clicked
if btn_predict:
    # Create a dataframe with test data
    data_teste = pd.DataFrame()
    data_teste["satisfaction"] = [satisfaction]
    data_teste["evaluation"] = [evaluation]
    data_teste["averageMonthlyHours"] = [averageMonthlyHours]
    data_teste["yearsAtCompany"] = [yearsAtCompany]

    # Display magenta indicator and test data
    st.markdown(
        "**The magenta indicator represents the employee's position in relation to potential turnover groups.**"
    )
    # Print the test data
    print(data_teste)

    # Perform prediction using the loaded model
    result = predict_model(model, data=data_teste)
    st.write(result)  # Display prediction result

    # Plotting employee groups
    fig = plt.figure(figsize=(10, 6))
    plt.scatter(
        x="satisfaction",
        y="evaluation",
        data=dataset[dataset.turnover == 1],
        alpha=0.25,
        color=kmeans_colors,
    )
    plt.xlabel("Satisfaction")
    plt.ylabel("Evaluation")

    # Scatter plot for cluster centers and magenta indicator
    plt.scatter(
        x=model_cluster.cluster_centers_[:, 0],
        y=model_cluster.cluster_centers_[:, 1],
        color="black",
        marker="X",
        s=100,
    )
    plt.scatter(x=[satisfaction], y=[evaluation], color="magenta", marker="X", s=300)

    plt.title("Employee Groups - Satisfaction vs Evaluation.")
    plt.show()  # Display the plot
    st.pyplot(fig)  # Display the plot in the Streamlit app
