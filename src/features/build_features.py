import joblib
import matplotlib.pyplot as plt
import pandas as pd
from minio import Minio
from sklearn.cluster import KMeans

# --------------------------------------------------------------
# Connecting to Data Lake
# --------------------------------------------------------------

client = Minio(
    "localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
)

# --------------------------------------------------------------
# Load data
# --------------------------------------------------------------

df = pd.read_pickle("../../data/interim/01_cleaned_data.pkl")

# --------------------------------------------------------------
# Creating clusters using KMeans
# --------------------------------------------------------------

# Initialize the KMeans object with 3 clusters, using a specific random state for
# reproducibility, and performing a maximum of 10 initialization attempts.
kmeans = KMeans(n_clusters=3, random_state=2, n_init=10)

# Defining the data set.
# Selecting the data of employees who left the company and the columns "satisfaction" and "evaluation".
df_turnover = df[df.turnover == 1][["satisfaction", "evaluation"]]

# Fit the KMeans model to the data.
kmeans.fit(df_turnover)

# Assign colors to data points based on cluster labels.
kmeans_colors = [
    "green" if c == 0 else "red" if c == 1 else "blue" for c in kmeans.labels_
]

# Create a scatter plot to visualize the data points and clusters.
fig = plt.figure(figsize=(10, 6))
plt.scatter(
    x="satisfaction", y="evaluation", data=df_turnover, alpha=0.25, color=kmeans_colors
)

# Add labels to the axes.
plt.xlabel("Satisfaction")
plt.ylabel("Evaluation")

# Plot cluster centers as black X markers.
plt.scatter(
    x=kmeans.cluster_centers_[:, 0],
    y=kmeans.cluster_centers_[:, 1],
    color="black",
    marker="X",
    s=100,
)

# Add title and display the plot.
plt.title("Employee Groups - Satisfaction vs Evaluation")
plt.show()

# --------------------------------------------------------------
# Transfering to Data Lake
# --------------------------------------------------------------

# Persisting the cluster object for the disk
joblib.dump(kmeans, "cluster.joblib")

# Connecting to Data Lake
client = Minio(
    "localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
)

# Transferring the file to Data Lake
client.fput_object("curated", "cluster.joblib", "cluster.joblib")
