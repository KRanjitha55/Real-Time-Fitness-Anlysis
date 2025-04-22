from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, sum, date_format, to_timestamp
import mysql.connector
from mysql.connector import Error
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ================================
# Initialize Spark Session
# ================================
spark = SparkSession.builder \
    .appName("FitnessBatchAnalysis") \
    .config("spark.jars", "mysql-connector/mysql-connector-j-8.3.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================================
# Read from MySQL
# ================================
def read_from_mysql():
    try:
        mysql_properties = {
            "driver": "com.mysql.cj.jdbc.Driver",
            "url": "jdbc:mysql://localhost:3306/fitness_db",
            "user": "root",
            "password": "Sairam@123",
        }
        
        df = spark.read \
            .format("jdbc") \
            .options(**mysql_properties) \
            .option("dbtable", "fitness_data") \
            .load()
            
        return df
    except Exception as e:
        print(f"❌ Error reading from MySQL: {e}")
        return None

# ================================
# Batch Analysis Functions
# ================================
def user_activity_analysis(df):
    print("\n=== User Activity Analysis ===")
    user_stats = df.groupBy("user_id").agg(
        count("*").alias("total_records"),
        avg("steps").alias("avg_steps"),
        avg("calories").alias("avg_calories"),
        avg("heart_rate").alias("avg_heart_rate"),
        max("steps").alias("max_steps"),
        max("calories").alias("max_calories")
    ).orderBy("avg_steps", ascending=False)
    
    user_stats.show()
    return user_stats

def time_based_analysis(df):
    print("\n=== Time-Based Analysis ===")
    # Convert timestamp to date format
    df_with_date = df.withColumn("date", date_format(to_timestamp("timestamp"), "yyyy-MM-dd"))
    
    daily_stats = df_with_date.groupBy("date").agg(
        avg("steps").alias("avg_steps"),
        avg("calories").alias("avg_calories"),
        avg("heart_rate").alias("avg_heart_rate"),
        count("*").alias("total_records")
    ).orderBy("date")
    
    daily_stats.show()
    return daily_stats

def health_metrics_analysis(df):
    print("\n=== Health Metrics Analysis ===")
    health_stats = df.select(
        avg("heart_rate").alias("avg_heart_rate"),
        max("heart_rate").alias("max_heart_rate"),
        min("heart_rate").alias("min_heart_rate"),
        avg("steps").alias("avg_steps"),
        avg("calories").alias("avg_calories")
    )
    
    health_stats.show()
    return health_stats

def generate_visualizations(df):
    # Convert Spark DataFrame to Pandas for visualization
    pdf = df.toPandas()
    
    # Set style
    plt.style.use('seaborn')
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # 1. Heart Rate Distribution
    sns.histplot(data=pdf, x="heart_rate", bins=20, ax=axes[0, 0])
    axes[0, 0].set_title("Heart Rate Distribution")
    
    # 2. Steps vs Calories
    sns.scatterplot(data=pdf, x="steps", y="calories", ax=axes[0, 1])
    axes[0, 1].set_title("Steps vs Calories")
    
    # 3. Average Steps by User
    user_avg = pdf.groupby("user_id")["steps"].mean().sort_values(ascending=False).head(10)
    sns.barplot(x=user_avg.index, y=user_avg.values, ax=axes[1, 0])
    axes[1, 0].set_title("Top 10 Users by Average Steps")
    axes[1, 0].tick_params(axis='x', rotation=45)
    
    # 4. Heart Rate vs Steps
    sns.scatterplot(data=pdf, x="steps", y="heart_rate", ax=axes[1, 1])
    axes[1, 1].set_title("Steps vs Heart Rate")
    
    plt.tight_layout()
    plt.savefig("fitness_analysis.png")
    print("\n✅ Visualizations saved as fitness_analysis.png")

# ================================
# Main Execution
# ================================
def main():
    print("🚀 Starting Batch Analysis...\n")
    
    # Read data from MySQL
    df = read_from_mysql()
    if df is None:
        return
    
    print(f"✅ Successfully loaded {df.count()} records")
    
    # Perform analyses
    user_stats = user_activity_analysis(df)
    daily_stats = time_based_analysis(df)
    health_stats = health_metrics_analysis(df)
    
    # Generate visualizations
    generate_visualizations(df)
    
    print("\n✅ Batch analysis completed successfully!")

if __name__ == "__main__":
    main() 