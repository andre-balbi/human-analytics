import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from minio import Minio
from pycaret.classification import *
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    auc,
    classification_report,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.tree import DecisionTreeClassifier

warnings.filterwarnings("ignore")

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

df.info()

df.head()

# --------------------------------------------------------------
# Converting the categorical attributes to numerical values
# --------------------------------------------------------------

df["department"] = df["department"].astype("category").cat.codes
df["salary"] = df["salary"].astype("category").cat.codes

df.head()

# --------------------------------------------------------------
# Splitting the data into X and y
# --------------------------------------------------------------

target_name = "turnover"
X = df.drop("turnover", axis=1)
y = df[target_name]

# --------------------------------------------------------------
# Transforming dataset
# --------------------------------------------------------------

# Normalizing the range of features so that they all have a consistent scale
scaler = MinMaxScaler()
X = scaler.fit_transform(X)

# --------------------------------------------------------------
# Splitting the data into train and test
# --------------------------------------------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=123, stratify=y
)

# --------------------------------------------------------------
# Using Decision Tree model
# --------------------------------------------------------------

# Fitting the model to the training data
dtree = DecisionTreeClassifier()
dtree = dtree.fit(X_train, y_train)

# Importances of the features
importances = dtree.feature_importances_
feat_names = df.drop(["turnover"], axis=1).columns

# Plotting the importances
indices = np.argsort(importances)[::-1]
plt.figure(figsize=(12, 4))
plt.title("Feature importances by DecisionTreeClassifier")
plt.bar(range(len(indices)), importances[indices], color="lightblue", align="center")
plt.xticks(range(len(indices)), feat_names[indices], rotation="vertical", fontsize=14)
plt.xlim([-1, len(indices)])
plt.show()

# --------------------------------------------------------------
# Filtering only the relevant attributes.
# --------------------------------------------------------------

# Selected features
X = df[["satisfaction", "evaluation", "averageMonthlyHours", "yearsAtCompany"]]

# Scalling the selected features
scaler = MinMaxScaler()
X = scaler.fit_transform(X)

# Splitting the data into train and test
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=123, stratify=y
)

X

# --------------------------------------------------------------
# Base Line model
# --------------------------------------------------------------


def base_rate_model(X):
    y = np.zeros(X.shape[0])
    return y


# --------------------------------------------------------------
# Model evaluation metrics
# --------------------------------------------------------------
def accuracy_result(y_test, y_predict):
    # Calculate the accuracy score by comparing predicted values (y_predict) with
    # actual values (y_test).
    acc = accuracy_score(y_test, y_predict)
    # Print the accuracy score with a format of two decimal places.
    print("Accuracy = %2.2f" % acc)


def roc_classification_report_results(model, y_test, y_predict):
    # Calculate the ROC AUC score by comparing predicted values (y_predict) with
    # actual values (y_test).
    roc_ = roc_auc_score(y_test, y_predict)
    # Generate a classification report to evaluate model performance.
    classification_report_str = classification_report(y_test, y_predict)

    # Print the model name, ROC AUC score, and the classification report.
    print("\n{} ROC AUC = {}\n".format(model, roc_))
    print(classification_report_str)


# --------------------------------------------------------------
# ROC Curve
# --------------------------------------------------------------


def plot_roc_curve(y_test, y_predict):
    """
    Plots the Receiver Operating Characteristic (ROC) curve for a binary classification model.

    Parameters:
    y_test (array-like): True binary labels.
    y_predict (array-like): Predicted probabilities of the positive class.
    """
    fpr, tpr, _ = roc_curve(
        y_test, y_predict
    )  # Calculate the ROC curve values (false positive rate and true positive rate)
    roc_auc = auc(fpr, tpr)  # Calculate the Area Under the Curve (AUC) for ROC

    # Create a new figure
    plt.figure()
    # Plot the ROC curve using dark orange color and label it with AUC value
    plt.plot(
        fpr, tpr, color="darkorange", lw=2, label="ROC curve (area = %0.4f)" % roc_auc
    )
    # Plot the diagonal dashed line representing random guessing
    plt.plot([0, 1], [0, 1], color="navy", lw=2, linestyle="--")
    # Set the x and y axis limits
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    # Set x and y axis labels and title
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("Receiver Operating Characteristic (ROC)")
    # Add a legend in the lower right corner
    plt.legend(loc="lower right")
    # Display the plot
    plt.show()


# --------------------------------------------------------------
# Analyzing the performance of the baseline model
# --------------------------------------------------------------

# Generate predictions using the base rate model for the test data
y_predict = base_rate_model(X_test)

# Calculate and display the accuracy of the predictions
accuracy_result(y_test, y_predict)

# Calculate and display the ROC AUC and classification report for the base model
roc_classification_report_results("Base Model", y_test, y_predict)

# Plot the ROC curve for the base model using the predicted probabilities
plot_roc_curve(y_test, y_predict)


# --------------------------------------------------------------
# Logistics Regression Model
# --------------------------------------------------------------

# Initialize a LogisticRegression model.
lreg = LogisticRegression()

# Fit the model to the training data.
lreg.fit(X_train, y_train)

# Use the trained model to make predictions on the test data.
y_predict = lreg.predict(X_test)

# Calculate and print the accuracy of the predictions using the accuracy_result function.
accuracy_result(y_test, y_predict)

# Generate and print the ROC AUC score and classification report using roc_classification_report_results function.
roc_classification_report_results("Logistic Regression", y_test, y_predict)

# Call the function to plot the ROC curve
plot_roc_curve(y_test, y_predict)
bbdd

# --------------------------------------------------------------
# Decision Tree Model
# --------------------------------------------------------------

# Initialize and train a Decision Tree Classifier
dt = DecisionTreeClassifier()
dt = dt.fit(X_train, y_train)

# Generate predictions using the trained Decision Tree model for the test data
y_predict = dt.predict(X_test)

# Calculate and display the accuracy of the predictions
accuracy_result(y_test, y_predict)

# Calculate and display the ROC AUC and classification report for the Decision Tree model
roc_classification_report_results("Decision Tree", y_test, y_predict)

# Plot the ROC curve for the Decision Tree model using the predicted probabilities
plot_roc_curve(y_test, y_predict)


# --------------------------------------------------------------
# Random Tree Model (Random Forest)
# --------------------------------------------------------------

# Initialize and train a Random Forest Classifier
rf = RandomForestClassifier()
rf = rf.fit(X_train, y_train)

# Generate predictions using the trained Random Forest model for the test data
y_predict = rf.predict(X_test)

# Calculate and display the accuracy of the predictions
accuracy_result(y_test, y_predict)

# Calculate and display the ROC AUC and classification report for the Random Forest model
roc_classification_report_results("Random Forest", y_test, y_predict)

# --------------------------------------------------------------
# PyCaret (Automated Machine Learning - AutoML)
# --------------------------------------------------------------

# Defining pycaret setup
s = setup(
    df[
        [
            "satisfaction",
            "evaluation",
            "averageMonthlyHours",
            "yearsAtCompany",
            "turnover",
        ]
    ],
    target="turnover",  # The target variable for prediction is "turnover".
    numeric_features=[
        "yearsAtCompany"
    ],  # Specify that "yearsAtCompany" is a numeric feature.
    normalize=True,  # Normalize the data to bring all features on the same scale.
    normalize_method="minmax",  # Use the Min-Max normalization method.
    data_split_stratify=True,  # Stratify the data during train-test split for balanced classes.
    fix_imbalance=True,  # Apply methods to fix class imbalance in the dataset.
)

# Compare multiple machine learning models and select the best based on AUC.
best = compare_models(
    fold=5,  # Perform 5-fold cross-validation during model comparison.
    sort="AUC",  # Sort the models based on their AUC scores.
)

# Create a Gradient Boosting Classifier model using PyCaret.
gbc = create_model(
    "gbc",  # Specifies the model to be created (Gradient Boosting Classifier).
    fold=5,  # Perform 5-fold cross-validation during model creation.
)

# Fine-tune a Gradient Boosting Classifier model using PyCaret.
tuned_gbc = tune_model(
    gbc,  # The previously created Gradient Boosting Classifier model.
    fold=5,  # Perform 5-fold cross-validation during tuning.
    custom_grid={  # Define an extended custom grid of hyperparameters to search through.
        "learning_rate": [0.01, 0.1, 0.2, 0.5],
        "n_estimators": [50, 100, 200, 500, 1000],
        "min_samples_split": [2, 5, 10],
        "min_samples_leaf": [1, 2, 4],
        "max_depth": [3, 5, 7, 9],
        "subsample": [0.8, 0.9, 1.0],
        "max_features": ["auto", "sqrt", "log2"],
    },
    optimize="AUC",  # Optimize the model based on the Area Under the Curve (AUC) metric.
)

# Create the final model using the tuned Gradient Boosting Classifier (gbc) model using
# all datasset
final_model = finalize_model(tuned_gbc)

# Save the final model to a file named "model" using the save_model function.
save_model(
    final_model,  # The trained final model that you want to save.
    "model",  # Specify the name of the directory where the model will be saved.
)


# --------------------------------------------------------------
# Transfering to Data Lake
# --------------------------------------------------------------

# Upload the machine learning model to an object storage location using the client's
# fput_object function.
client.fput_object(
    "curated",  # Specify the destination bucket r where the object will be stored.
    "model.pkl",  # Specify the name to be given to the uploaded object in the destination.
    "model.pkl",  # Specify the local file path of the object to be uploaded.
)

# Save the DataFrame to a CSV file named "dataset.csv".
df.to_csv(
    "../../data/processed/dataset.csv", index=False
)  # Exclude the index column from being saved in the CSV.

# Upload the "dataset.csv" file to the "curated" directory in the storage system.
client.fput_object(
    "curated",  # Target directory ("curated" in this case).
    "dataset.csv",  # Name of the file to upload.
    "../../data/processed/dataset.csv",  # Local path of the file to upload.
)
