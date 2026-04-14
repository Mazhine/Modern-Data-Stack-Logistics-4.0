"""
Logistics 4.0 ML Pipeline Trainer.
Handles the extraction, algorithmic modeling, and MLflow tracing of
the Late Delivery Risk predictor via PySpark MLLib.
"""

import os
import shutil

import mlflow
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# ==============================================================================
# MLFLOW TRACKING CONFIG
# ==============================================================================
MLFLOW_URI = "http://13_mlflow:5000"
EXPERIMENT_NAME = "Logistics_Delay_Prediction"
MODEL_EXPORT_PATH = "/opt/logistics/spark_models/late_delivery_rf"

# Hyperparameters
HYPERPARAMETERS = {
    "numTrees": 20,
    "maxDepth": 5
}


class SparkModelTrainer:
    """Orchestrates Spark ML training runs."""

    def __init__(self):
        mlflow.set_tracking_uri(MLFLOW_URI)
        mlflow.set_experiment(EXPERIMENT_NAME)
        
        self.spark = SparkSession.builder \
            .appName("Logistics40_Model_Training") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def _load_data(self) -> "pyspark.sql.DataFrame":
        """Load and curate the training features."""
        print("[INFO] Loading historical dataset from /tmp/DataCoSupplyChainDataset.csv...")
        df = self.spark.read.csv(
            "/tmp/DataCoSupplyChainDataset.csv", 
            header=True, 
            inferSchema=True, 
            mode="DROPMALFORMED"
        )
        
        # We explicitly omit "Days for shipping (real)" to prevent Target Leakage.
        selected_data = df.select(
            col("Shipping Mode"),
            col("Days for shipment (scheduled)").cast("integer"),
            col("Order Item Total").cast("double"),
            col("Late_delivery_risk").cast("integer").alias("label")
        ).dropna()
        
        return selected_data

    def _build_pipeline(self) -> Pipeline:
        """Constructs the Spark ML Pipeline."""
        indexer = StringIndexer(inputCol="Shipping Mode", outputCol="shipping_idx", handleInvalid="keep")
        assembler = VectorAssembler(
            inputCols=["shipping_idx", "Days for shipment (scheduled)", "Order Item Total"],
            outputCol="features"
        )
        rf = RandomForestClassifier(
            labelCol="label", 
            featuresCol="features", 
            numTrees=HYPERPARAMETERS["numTrees"], 
            maxDepth=HYPERPARAMETERS["maxDepth"]
        )
        return Pipeline(stages=[indexer, assembler, rf])

    def execute_training(self) -> None:
        """Main training lifecycle integrating MLflow tracking."""
        df = self._load_data()
        pipeline = self._build_pipeline()
        
        train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
        
        with mlflow.start_run():
            print("[INFO] Initiating Machine Learning Model Training (Random Forest)...")
            model: PipelineModel = pipeline.fit(train_data)
            
            print("[INFO] Evaluating predictive accuracy...")
            predictions = model.transform(test_data)
            
            acc_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
            f1_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
            
            accuracy = acc_evaluator.evaluate(predictions)
            f1_score = f1_evaluator.evaluate(predictions)
            
            print(f"[METRIC] Accuracy: {accuracy:.4f}")
            print(f"[METRIC] F1-Score: {f1_score:.4f}")
            
            # Log to MLflow
            for key, value in HYPERPARAMETERS.items():
                mlflow.log_param(key, value)
                
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("f1_score", f1_score)
            
            # Persist artifact
            mlflow.spark.log_model(model, "random_forest_model")
            self._save_internal_model(model)

    def _save_internal_model(self, model: PipelineModel) -> None:
        """Handles physical model overwrite for Spark Streaming consumption."""
        try:
            if os.path.exists(MODEL_EXPORT_PATH):
                shutil.rmtree(MODEL_EXPORT_PATH)
        except Exception as e:
            print(f"[WARN] Failed to delete existing path locally: {e}")
            
        print(f"[INFO] Serializing PipelineModel to: {MODEL_EXPORT_PATH}")
        model.write().overwrite().save(MODEL_EXPORT_PATH)
        print("[SUCCESS] Training execution complete.")


if __name__ == "__main__":
    trainer = SparkModelTrainer()
    trainer.execute_training()
