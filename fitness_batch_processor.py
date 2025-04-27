from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count, sum, date_format, to_timestamp, col
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
def read_from_mysql(table_name):
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
            .option("dbtable", table_name) \
            .load()
            
        return df
    except Exception as e:
        print(f"❌ Error reading from MySQL table {table_name}: {e}")
        return None

# ================================
# Batch Analysis Functions
# ================================
def real_time_data_analysis(df):
    print("\n=== Real-time Fitness Data Analysis ===")
    
    # User activity analysis
    print("\nUser Activity Statistics:")
    user_stats = df.groupBy("user_id").agg(
        count("*").alias("total_records"),
        avg("steps").alias("avg_steps"),
        avg("calories").alias("avg_calories"),
        avg("heart_rate").alias("avg_heart_rate"),
        max("steps").alias("max_steps"),
        max("calories").alias("max_calories")
    ).orderBy("avg_steps", ascending=False)
    user_stats.show()

    # Time-based analysis
    print("\nDaily Trends:")
    df_with_date = df.withColumn("date", date_format(to_timestamp("timestamp"), "yyyy-MM-dd"))
    daily_stats = df_with_date.groupBy("date").agg(
        avg("steps").alias("avg_steps"),
        avg("calories").alias("avg_calories"),
        avg("heart_rate").alias("avg_heart_rate"),
        count("*").alias("total_records")
    ).orderBy("date")
    daily_stats.show()

    return user_stats, daily_stats

def daily_summary_analysis(df):
    print("\n=== Daily Summary Analysis ===")
    
    # User performance analysis
    print("\nUser Performance Overview:")
    user_performance = df.groupBy("user_id").agg(
        avg("total_steps").alias("avg_daily_steps"),
        avg("total_calories").alias("avg_daily_calories"),
        avg("avg_heart_rate").alias("avg_heart_rate")
    ).orderBy("avg_daily_steps", ascending=False)
    user_performance.show()

    # Overall statistics
    print("\nOverall Daily Statistics:")
    df.select(
        avg("total_steps").alias("avg_total_steps"),
        max("total_steps").alias("max_total_steps"),
        avg("total_calories").alias("avg_total_calories"),
        avg("avg_heart_rate").alias("overall_avg_heart_rate")
    ).show()

def alert_analysis(df):
    print("\n=== Alert Analysis ===")
    
    # Alert type distribution
    print("\nAlert Type Distribution:")
    df.groupBy("alert_type").agg(
        count("*").alias("count")
    ).orderBy("count", ascending=False).show()
    
    # User-wise alert analysis
    print("\nUser-wise Alert Distribution:")
    df.groupBy("user_id").agg(
        count("*").alias("total_alerts"),
        count("alert_type").alias("unique_alert_types")
    ).orderBy("total_alerts", ascending=False).show()

def generate_visualizations(fitness_df, summary_df, alerts_df):
    # Convert Spark DataFrames to Pandas for visualization
    fitness_pdf = fitness_df.toPandas()
    summary_pdf = summary_df.toPandas()
    alerts_pdf = alerts_df.toPandas()
    
    # Set style
    plt.style.use('seaborn')
    
    # Create figure with subplots
    fig, axes = plt.subplots(3, 2, figsize=(15, 20))
    
    # Real-time Fitness Data Visualizations
    sns.histplot(data=fitness_pdf, x="heart_rate", bins=20, ax=axes[0, 0])
    axes[0, 0].set_title("Heart Rate Distribution (Real-time)")
    
    sns.scatterplot(data=fitness_pdf, x="steps", y="calories", ax=axes[0, 1])
    axes[0, 1].set_title("Steps vs Calories (Real-time)")
    
    # Daily Summary Visualizations
    sns.boxplot(data=summary_pdf, y="total_steps", ax=axes[1, 0])
    axes[1, 0].set_title("Daily Steps Distribution")
    
    sns.scatterplot(data=summary_pdf, x="total_steps", y="total_calories", ax=axes[1, 1])
    axes[1, 1].set_title("Daily Steps vs Calories")
    
    # Alert Visualizations
    alert_counts = alerts_pdf["alert_type"].value_counts()
    sns.barplot(x=alert_counts.index, y=alert_counts.values, ax=axes[2, 0])
    axes[2, 0].set_title("Alert Type Distribution")
    axes[2, 0].tick_params(axis='x', rotation=45)
    
    user_alerts = alerts_pdf["user_id"].value_counts()
    sns.barplot(x=user_alerts.index, y=user_alerts.values, ax=axes[2, 1])
    axes[2, 1].set_title("Alerts by User")
    axes[2, 1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig("fitness_analysis.png")
    print("\n✅ Visualizations saved as fitness_analysis.png")

# ================================
# Main Execution
# ================================
def main():
    print("🚀 Starting Batch Analysis...\n")
    
    # Read data from all tables
    fitness_df = read_from_mysql("fitness_data")
    summary_df = read_from_mysql("daily_summaries")
    alerts_df = read_from_mysql("fitness_alerts")
    
    if not all([fitness_df, summary_df, alerts_df]):
        return
    
    print(f"✅ Successfully loaded:")
    print(f"   - {fitness_df.count()} fitness records")
    print(f"   - {summary_df.count()} daily summaries")
    print(f"   - {alerts_df.count()} alerts")
    
    # Perform analyses
    user_stats, daily_stats = real_time_data_analysis(fitness_df)
    daily_summary_analysis(summary_df)
    alert_analysis(alerts_df)
    
    # Generate visualizations
    generate_visualizations(fitness_df, summary_df, alerts_df)
    
    print("\n✅ Batch analysis completed successfully!")

if __name__ == "__main__":
    main() 
