from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import sys

# Configurations de Sink Postgres (Pipeline Couche Silver)
DB_URL = "jdbc:postgresql://postgres:5432/logistics_db"
DB_PROPERTIES = {
    "user": "admin_logistics",
    "password": "securepassword123",
    "driver": "org.postgresql.Driver"
}

def load_to_silver_layer(batch_df, batch_id):
    """
    Traitement micro-batch pour alimenter la couche Silver dans PostgreSQL.
    """
    # Nettoyage et slection des colonnes
    silver_df = batch_df.select(
        col("`Order Id`").alias("order_id"),
        col("`Customer Id`").alias("customer_id"),
        col("`order date (DateOrders)`").alias("order_date"), # Dja ts par le readStream JSON schema
        col("`shipping date (DateOrders)`").cast(TimestampType()).alias("shipping_date"),
        col("`Days for shipping (real)`").alias("days_for_shipping_real"),
        col("`Days for shipment (scheduled)`").alias("days_for_shipment_scheduled"),
        col("`Order Item Total`").alias("order_item_total"),
        col("`Order Status`").alias("order_status"),
        col("`Delivery Status`").alias("delivery_status"),
        col("`Shipping Mode`").alias("shipping_mode"),
        col("delivery_delay_risk"), # Retard absolu
        col("delay_probability"), # Prédiction MLlib (probabilité %)
        col("is_delayed_prediction") # Prédiction MLlib (Binaire)
    )
    
    silver_df.write.jdbc(url=DB_URL, table="silver_orders", mode="append", properties=DB_PROPERTIES)

def main():
    print("--- DEMARRAGE SPARK MEDALLION ARCHITECTURE ---")
    # Initialisation Engine Structur avec Packages S3 et JDBC
    spark = SparkSession.builder \
        .appName("Logistics40_Streaming_Medallion") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
        .config("spark.jars.ivy", "/tmp/.ivy") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Contrat de Donnes Strict pour parser JSON Kafka
    schema = StructType([
        StructField("Order Id", IntegerType(), True),
        StructField("Customer Id", IntegerType(), True),
        StructField("Product Card Id", IntegerType(), True),
        StructField("order date (DateOrders)", TimestampType(), True),
        StructField("shipping date (DateOrders)", StringType(), True),
        StructField("Days for shipping (real)", IntegerType(), True),
        StructField("Days for shipment (scheduled)", IntegerType(), True),
        StructField("Benefit per order", DoubleType(), True),
        StructField("Sales per customer", DoubleType(), True),
        StructField("Order Item Total", DoubleType(), True),
        StructField("Order Item Discount", DoubleType(), True),
        StructField("Product Price", DoubleType(), True),
        StructField("Order Status", StringType(), True),
        StructField("Delivery Status", StringType(), True),
        StructField("Shipping Mode", StringType(), True),
        StructField("Late_delivery_risk", IntegerType(), True),
        StructField("Customer Country", StringType(), True),
        StructField("Customer City", StringType(), True),
        StructField("Order Country", StringType(), True),
        StructField("Order City", StringType(), True),
        StructField("Order Region", StringType(), True),
        StructField("Category Name", StringType(), True),
        StructField("Customer Segment", StringType(), True),
        StructField("Department Name", StringType(), True),
        StructField("Type", StringType(), True)
    ])

    # 1. LECTURE DU FLUX KAFKA
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "dataco_orders") \
        .option("startingOffsets", "latest") \
        .load()

    # 2. BRONZE SINK (MINIO) - SAUVEGARDE DE LA DONNEE BRUTE COMPLETE
    bronze_query = df.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .format("text") \
        .option("path", "s3a://logistics-bronze/raw_orders") \
        .option("checkpointLocation", "/opt/bitnami/spark/checkpoint_bronze") \
        .trigger(processingTime="10 seconds") \
        .start()

    # 3. PARSING POUR LA SILVER
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # 4. TRANSFORMATION INTELLIGENCE ARTIFICIELLE (Spark MLlib Simulé)
    from pyspark.ml.feature import VectorAssembler
    from pyspark.sql.functions import rand
    
    # Étape 1 : Pipeline MLlib - VectorAssembler pour préparer les Features du modèle (Features Engineering)
    assembler = VectorAssembler(
        inputCols=["Days for shipping (real)", "Days for shipment (scheduled)", "Order Item Total"],
        outputCol="features",
        handleInvalid="skip"
    )
    
    # En environnement de prod, on chargerait le modèle: model = PipelineModel.load("hdfs://.../mon_modele_rf")
    # Et on l'appliquerait via: ml_expanded_df = model.transform(assembler.transform(parsed_df))
    ml_df = assembler.transform(parsed_df)
    
    # Étape 2 : Prédiction MLlib (Inférance stochastique pour la démo PFA)
    # On calcule le delivery_delay_risk classique, et on demande à l'I.A. (simulée) de prédire le %
    enriched_df = ml_df.withColumn("delivery_delay_risk", expr("`Days for shipping (real)` - `Days for shipment (scheduled)`")) \
        .withColumn("delay_probability", rand() * 100) \
        .withColumn("is_delayed_prediction", when(col("delay_probability") >= 85, 1).otherwise(0))
    
    # 5. SILVER SINK (POSTGRESQL) - MISE EN STRUCTURE
    silver_query = enriched_df.writeStream \
        .foreachBatch(load_to_silver_layer) \
        .outputMode("append") \
        .option("checkpointLocation", "/opt/bitnami/spark/checkpoint_silver") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("--- FLUX BRONZE ET SILVER ACTIFS ---")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
