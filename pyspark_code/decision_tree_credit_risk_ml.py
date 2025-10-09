import pyspark
from delta import *
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.classification import DecisionTreeClassifier, DecisionTreeClassificationModel
from pyspark.ml import Pipeline
import os

# Set environment variables for better local execution
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

# Create Spark Session
builder = pyspark.sql.SparkSession.builder.appName("CreditRiskPrediction") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.execution.arrow.pyspark.enabled", True) \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000") \
    .config('spark.sql.debug.maxToStringFields', 2000) \
    .config('spark.debug.maxToStringFields', 2000)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 80)
print("CREDIT RISK PREDICTION USING DECISION TREE")
print("=" * 80)

# Create sample credit risk dataset
credit_data = [
    (1, 35, 50000, 3, 2, 0.2, "Employed", "Good", 0),      # No Default
    (2, 42, 75000, 5, 1, 0.15, "Employed", "Excellent", 0),
    (3, 28, 35000, 2, 4, 0.45, "Employed", "Fair", 1),     # Default
    (4, 50, 90000, 8, 0, 0.1, "Employed", "Excellent", 0),
    (5, 23, 25000, 1, 5, 0.6, "Unemployed", "Poor", 1),
    (6, 38, 60000, 4, 2, 0.25, "Employed", "Good", 0),
    (7, 45, 80000, 6, 1, 0.12, "Self-Employed", "Excellent", 0),
    (8, 31, 40000, 3, 3, 0.35, "Employed", "Fair", 1),
    (9, 29, 32000, 2, 4, 0.5, "Unemployed", "Poor", 1),
    (10, 55, 95000, 10, 0, 0.08, "Employed", "Excellent", 0),
    (11, 26, 28000, 1, 5, 0.55, "Employed", "Poor", 1),
    (12, 48, 85000, 7, 1, 0.11, "Self-Employed", "Excellent", 0),
    (13, 33, 45000, 3, 3, 0.4, "Employed", "Fair", 1),
    (14, 39, 65000, 5, 1, 0.18, "Employed", "Good", 0),
    (15, 44, 72000, 6, 2, 0.2, "Self-Employed", "Good", 0),
    (16, 25, 30000, 1, 6, 0.65, "Unemployed", "Poor", 1),
    (17, 52, 88000, 9, 0, 0.09, "Employed", "Excellent", 0),
    (18, 30, 38000, 2, 4, 0.48, "Employed", "Fair", 1),
    (19, 41, 70000, 5, 1, 0.16, "Employed", "Good", 0),
    (20, 27, 33000, 2, 5, 0.52, "Unemployed", "Poor", 1),
    (21, 36, 55000, 4, 2, 0.22, "Employed", "Good", 0),
    (22, 49, 82000, 7, 1, 0.13, "Self-Employed", "Excellent", 0),
    (23, 32, 42000, 3, 3, 0.38, "Employed", "Fair", 1),
    (24, 46, 78000, 6, 1, 0.14, "Employed", "Good", 0),
    (25, 24, 27000, 1, 6, 0.62, "Unemployed", "Poor", 1),
    (26, 51, 91000, 8, 0, 0.1, "Employed", "Excellent", 0),
    (27, 34, 48000, 3, 3, 0.36, "Employed", "Fair", 0),
    (28, 40, 68000, 5, 2, 0.19, "Employed", "Good", 0),
    (29, 37, 58000, 4, 2, 0.24, "Self-Employed", "Good", 0),
    (30, 28, 31000, 2, 5, 0.58, "Unemployed", "Poor", 1),
]

schema = ["CustomerID", "Age", "Income", "YearsEmployed", "NumDefaultAccounts",
          "DebtToIncomeRatio", "EmploymentStatus", "CreditScore", "DefaultRisk"]

credit_df = spark.createDataFrame(credit_data, schema)

print("\n--- Original Credit Data Sample ---")
credit_df.show(10)

print("\n--- Data Statistics ---")
credit_df.describe().show()

print("\n--- Default Risk Distribution ---")
credit_df.groupBy("DefaultRisk").count().show()

# Feature Engineering
print("\n--- Feature Engineering ---")

# Index categorical variables
employment_indexer = StringIndexer(inputCol="EmploymentStatus", outputCol="EmploymentStatusIndex")
credit_score_indexer = StringIndexer(inputCol="CreditScore", outputCol="CreditScoreIndex")

# Select features for the model
feature_columns = ["Age", "Income", "YearsEmployed", "NumDefaultAccounts",
                   "DebtToIncomeRatio", "EmploymentStatusIndex", "CreditScoreIndex"]

# Assemble features into a vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Split data into training and test sets (80-20 split)
train_df, test_df = credit_df.randomSplit([0.8, 0.2], seed=42)

print(f"Training set size: {train_df.count()}")
print(f"Test set size: {test_df.count()}")
train_df.show()
test_df.show()

# Create Decision Tree Classifier
dt = DecisionTreeClassifier(
    featuresCol="features",
    labelCol="DefaultRisk",
    maxDepth=5,
    minInstancesPerNode=1,
    impurity="gini",
    seed=42
)

# Create Pipeline
pipeline = Pipeline(stages=[employment_indexer, credit_score_indexer, assembler, dt])

# Train the model
print("\n--- Training Decision Tree Model ---")
model = pipeline.fit(train_df)

# Make predictions on test data
predictions = model.transform(test_df)

print("\n--- Predictions on Test Data ---")
predictions.select("CustomerID", "Age", "Income", "CreditScore", "DefaultRisk",
                   "prediction", "probability").show(truncate=False)

# Evaluate the model
print("\n--- Model Evaluation ---")

# Binary Classification Metrics
binary_evaluator = BinaryClassificationEvaluator(labelCol="DefaultRisk", metricName="areaUnderROC")
auc = binary_evaluator.evaluate(predictions)
print(f"Area Under ROC Curve (AUC): {auc:.4f}")

binary_evaluator_pr = BinaryClassificationEvaluator(labelCol="DefaultRisk", metricName="areaUnderPR")
auc_pr = binary_evaluator_pr.evaluate(predictions)
print(f"Area Under PR Curve: {auc_pr:.4f}")

# Multi-class Metrics
accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol="DefaultRisk", predictionCol="prediction", metricName="accuracy")
accuracy = accuracy_evaluator.evaluate(predictions)
print(f"Accuracy: {accuracy:.4f}")

precision_evaluator = MulticlassClassificationEvaluator(
    labelCol="DefaultRisk", predictionCol="prediction", metricName="weightedPrecision")
precision = precision_evaluator.evaluate(predictions)
print(f"Weighted Precision: {precision:.4f}")

recall_evaluator = MulticlassClassificationEvaluator(
    labelCol="DefaultRisk", predictionCol="prediction", metricName="weightedRecall")
recall = recall_evaluator.evaluate(predictions)
print(f"Weighted Recall: {recall:.4f}")

f1_evaluator = MulticlassClassificationEvaluator(
    labelCol="DefaultRisk", predictionCol="prediction", metricName="f1")
f1 = f1_evaluator.evaluate(predictions)
print(f"F1 Score: {f1:.4f}")

# Extract Decision Tree Model
dt_model = model.stages[-1]

# Feature Importance
print("\n--- Feature Importance ---")
feature_importance = dt_model.featureImportances
feature_names = feature_columns

for idx, importance in enumerate(feature_importance):
    if importance > 0:
        print(f"{feature_names[idx]}: {importance:.4f}")

# Decision Tree Details
print("\n--- Decision Tree Model Details ---")
print(f"Number of nodes: {dt_model.numNodes}")
print(f"Tree depth: {dt_model.depth}")
print(f"\nDecision Tree Structure:")
print(dt_model.toDebugString)

# Confusion Matrix
print("\n--- Confusion Matrix ---")

predictions.groupBy("DefaultRisk", "prediction").count().orderBy("DefaultRisk", "prediction").show()

# Calculate detailed metrics from confusion matrix
cm = predictions.groupBy("DefaultRisk", "prediction").count().collect()
tn = fp = fn = tp = 0
for row in cm:
    if row.DefaultRisk == 0 and row.prediction == 0.0: tn = row['count']
    if row.DefaultRisk == 0 and row.prediction == 1.0: fp = row['count']
    if row.DefaultRisk == 1 and row.prediction == 0.0: fn = row['count']
    if row.DefaultRisk == 1 and row.prediction == 1.0: tp = row['count']

print(f"\nTrue Negatives (TN): {tn}")
print(f"False Positives (FP): {fp}")
print(f"False Negatives (FN): {fn}")
print(f"True Positives (TP): {tp}")

if (tp + fp) > 0:
    print(f"\nPrecision (TP/(TP+FP)): {tp/(tp+fp):.4f}")
if (tp + fn) > 0:
    print(f"Recall/Sensitivity (TP/(TP+FN)): {tp/(tp+fn):.4f}")
if (tn + fp) > 0:
    print(f"Specificity (TN/(TN+FP)): {tn/(tn+fp):.4f}")


# Example: Predict for new customers
print("\n--- Predictions for New Customers ---")
new_customers = [
    (31, 45000, 3, 2, 0.3, "Employed", "Fair"),
    (50, 95000, 10, 0, 0.08, "Employed", "Excellent"),
    (25, 28000, 1, 6, 0.7, "Unemployed", "Poor"),
]

new_schema = ["Age", "Income", "YearsEmployed", "NumDefaultAccounts",
              "DebtToIncomeRatio", "EmploymentStatus", "CreditScore"]

new_df = spark.createDataFrame(new_customers, new_schema)
new_predictions = model.transform(new_df)

new_predictions.select("Age", "Income", "EmploymentStatus", "CreditScore",
                       "prediction", "probability").show(truncate=False)

# Interpretation
print("\n" + "=" * 80)
print("MODEL INTERPRETATION")
print("=" * 80)
print("prediction = 0: Low Risk (Customer likely to NOT default)")
print("prediction = 1: High Risk (Customer likely to default)")
print("=" * 80)

spark.stop()