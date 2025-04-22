from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import mysql.connector
from mysql.connector import Error

# ================================
# Initialize Spark Session
# ================================
spark = SparkSession.builder \
    .appName("RealTimeFitnessAnalysis") \
    .config("spark.jars", "mysql-connector/mysql-connector-j-8.3.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================================
# Define Schema for Fitness Data
# ================================
fitness_schema = StructType() \
    .add("user_id", StringType()) \
    .add("heart_rate", IntegerType()) \
    .add("steps", IntegerType()) \
    .add("calories", DoubleType()) \
    .add("timestamp", StringType())

# ================================
# Initialize MySQL Connection and Create Table
# ================================
def init_mysql():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Sairam@123",
            database="fitness_db"
        )
        print('Connected to MySQL')
        cursor = connection.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS fitness_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(20),
                steps INT,
                calories DOUBLE,
                heart_rate INT,
                timestamp VARCHAR(30)
            )
        """
        cursor.execute(create_table_query)
        connection.commit()
        print("✅ Table fitness_data is ready")
        
    except Error as e:
        print(f"❌ Error initializing MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Initialize table
init_mysql()

# ================================
# Store to MySQL with Error Handling
# ================================
def store_to_mysql(df, batch_id):
    try:
        print(f"\n=== Batch {batch_id} → Storing fitness data ===")
        df.show(5, truncate=False)

        mysql_properties = {
            "driver": "com.mysql.cj.jdbc.Driver",
            "url": "jdbc:mysql://localhost:3306/fitness_db",
            "user": "root",
            "password": "Sairam@123",
        }
        
        # Write to MySQL
        df.write \
            .format("jdbc") \
            .options(**mysql_properties) \
            .option("dbtable", "fitness_data") \
            .mode("append") \
            .save()

        print(f"✅ Successfully stored batch {batch_id} to fitness_data")

    except Exception as e:
        print(f"❌ Error storing batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

# ================================
# Fitness Data Processing
# ================================
def fitness_analysis(df, batch_id):
    store_to_mysql(df, batch_id)

    print("\n=== Average Stats by User ===")
    df.groupBy("user_id").agg(
        avg("steps").alias("avg_steps"),
        avg("calories").alias("avg_calories"),
        avg("heart_rate").alias("avg_heart_rate")
    ).show()

    print("\n=== Overall Health Snapshot ===")
    df.select(
        avg("heart_rate").alias("avg_heart_rate"),
        max("heart_rate").alias("max_heart_rate"),
        min("heart_rate").alias("min_heart_rate")
    ).show()

# ================================
# Read Kafka Stream
# ================================
def read_kafka(topic, schema):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

# Use your Kafka topic name
fitness_df = read_kafka("fitness-topic", fitness_schema)

# ================================
# Start Streaming Query
# ================================
print("🚀 Starting Real-Time Fitness Stream...\n")

fitness_query = fitness_df.writeStream \
    .outputMode("append") \
    .foreachBatch(fitness_analysis) \
    .start()

fitness_query.awaitTermination() 