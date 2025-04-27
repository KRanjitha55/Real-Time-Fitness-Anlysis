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
    .config("spark.jars", "mysql-connector/mysql-connector-j-8.0.33.jar") \
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

# Daily summary schema
daily_summary_schema = StructType() \
    .add("user_id", StringType()) \
    .add("total_steps", IntegerType()) \
    .add("total_calories", DoubleType()) \
    .add("avg_heart_rate", IntegerType()) \
    .add("date", StringType())

# Alert schema (example fields)
alert_schema = StructType() \
    .add("user_id", StringType()) \
    .add("alert_type", StringType()) \
    .add("alert_message", StringType()) \
    .add("timestamp", StringType())

# ================================
# Initialize MySQL Connection and Create Table
# ================================
def init_mysql():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="ranjitha",
            password="ubuntu",
            database="fitness_db"
        )
        print('Connected to MySQL')
        cursor = connection.cursor()

        # Create table for real-time fitness data
        create_fitness_table = """
            CREATE TABLE IF NOT EXISTS fitness_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(20),
                steps INT,
                calories DOUBLE,
                heart_rate INT,
                timestamp VARCHAR(30)
            )
        """
        
        # Create table for daily summaries
        create_summary_table = """
            CREATE TABLE IF NOT EXISTS daily_summaries (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(20),
                total_steps INT,
                total_calories DOUBLE,
                avg_heart_rate INT,
                date VARCHAR(30)
            )
        """
        
        # Create table for alerts
        create_alert_table = """
            CREATE TABLE IF NOT EXISTS fitness_alerts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(20),
                alert_type VARCHAR(50),
                alert_message TEXT,
                timestamp VARCHAR(30)
            )
        """
        
        cursor.execute(create_fitness_table)
        cursor.execute(create_summary_table)
        cursor.execute(create_alert_table)
        connection.commit()
        print("✅ All tables are ready")
        
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
def store_to_mysql(df, batch_id, topic):
    try:
        print(f"\n=== Batch {batch_id} → Storing fitness data for {topic} ===")
        df.show(5, truncate=False)

        mysql_properties = {
            "driver": "com.mysql.cj.jdbc.Driver",
            "url": "jdbc:mysql://localhost:3306/fitness_db",
            "user": "root",
            "password": "Sairam@123",
        }
        
        # Store data based on the topic
        if topic == "fitness-topic":
            table = "fitness_data"
        elif topic == "daily-summary-topic":
            table = "daily_summaries"
        elif topic == "alert-topic":
            table = "fitness_alerts"
        else:
            return  # Ignore unknown topics

        # Write to MySQL
        df.write \
            .format("jdbc") \
            .options(**mysql_properties) \
            .option("dbtable", table) \
            .mode("append") \
            .save()

        print(f"✅ Successfully stored batch {batch_id} to {table}")

    except Exception as e:
        print(f"❌ Error storing batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

# ================================
# Fitness Data Processing
# ================================
def fitness_analysis(df, batch_id):
    # First store the data in appropriate tables
    store_to_mysql(df, batch_id, df.select("topic").head()[0])  # Get the topic from the stream
    
    # Process real-time fitness data
    fitness_data = df.filter(col("topic") == "fitness-topic")
    if not fitness_data.isEmpty():
        print("\n=== Real-time Fitness Data Analysis ===")
        print("User-level Statistics:")
        fitness_data.groupBy("user_id").agg(
            avg("steps").alias("avg_steps"),
            avg("calories").alias("avg_calories"),
            avg("heart_rate").alias("avg_heart_rate")
        ).show()

        print("\nOverall Health Snapshot:")
        fitness_data.select(
            avg("heart_rate").alias("avg_heart_rate"),
            max("heart_rate").alias("max_heart_rate"),
            min("heart_rate").alias("min_heart_rate")
        ).show()

    # Process daily summaries
    daily_summaries = df.filter(col("topic") == "daily-summary-topic")
    if not daily_summaries.isEmpty():
        print("\n=== Daily Summary Analysis ===")
        daily_summaries.select(
            "user_id",
            "total_steps",
            "total_calories",
            "avg_heart_rate",
            "date"
        ).show()

    # Process alerts
    alerts = df.filter(col("topic") == "alert-topic")
    if not alerts.isEmpty():
        print("\n🚨 Recent Alerts:")
        alerts.select(
            "user_id", "alert_type", "alert_message", "timestamp"
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
        .selectExpr("CAST(value AS STRING)", "topic") \
        .select(from_json(col("value"), schema).alias("data"), col("topic")) \
        .select("data.*", "topic")

# Read streams with appropriate schemas
fitness_stream = read_kafka("fitness-topic", fitness_schema)
daily_summary_stream = read_kafka("daily-summary-topic", daily_summary_schema)
alert_stream = read_kafka("alert-topic", alert_schema)

# Union all streams (either is fine, but unionAll is more efficient)
combined_stream = fitness_stream.unionAll(daily_summary_stream).unionAll(alert_stream)

# ================================
# Start Streaming Query
# ================================
print("🚀 Starting Real-Time Fitness Stream...\n")

fitness_query = combined_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(fitness_analysis) \
    .start()

fitness_query.awaitTermination()
