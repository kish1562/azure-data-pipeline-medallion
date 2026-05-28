# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business Aggregations
# MAGIC Creates business-ready aggregated tables and analytical views from Silver data,
# MAGIC optimized for consumption by Azure Synapse Analytics and Power BI.

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, max as _max, min as _min,
    countDistinct, date_format, current_timestamp, round as _round, rank
)
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldAggregation")

# COMMAND ----------

ADLS_STORAGE_ACCOUNT = dbutils.secrets.get(scope="azure-keyvault", key="storage-account-name")
SILVER_PATH = f"abfss://curated-data@{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net/silver"
GOLD_PATH = f"abfss://business-data@{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net/gold"

# COMMAND ----------

class GoldAggregationPipeline:
    """Builds business-ready aggregated tables for analytics and reporting."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        logger.info("Gold Aggregation Pipeline initialized")
    
    def build_customer_360(self) -> DataFrame:
        """Create a 360-degree customer view with lifetime metrics."""
        logger.info("Building customer_360 Gold table")
        
        customers = self.spark.read.format("delta").load(f"{SILVER_PATH}/customers")
        orders = self.spark.read.format("delta").load(f"{SILVER_PATH}/orders")
        
        # Aggregate order metrics per customer
        order_metrics = (
            orders
            .filter(col("order_status") == "completed")
            .groupBy("customer_id")
            .agg(
                count("order_id").alias("total_orders"),
                _round(_sum("order_amount"), 2).alias("lifetime_value"),
                _round(avg("order_amount"), 2).alias("avg_order_value"),
                _max("order_date").alias("last_order_date"),
                _min("order_date").alias("first_order_date")
            )
        )
        
        # Join with customer dimension
        customer_360 = (
            customers
            .join(order_metrics, on="customer_id", how="left")
            .withColumn("total_orders", coalesce_zero(col("total_orders")))
            .withColumn("lifetime_value", coalesce_zero(col("lifetime_value")))
            # Customer segmentation by value
            .withColumn(
                "customer_segment",
                when(col("lifetime_value") >= 10000, "VIP")
                .when(col("lifetime_value") >= 5000, "High Value")
                .when(col("lifetime_value") >= 1000, "Regular")
                .otherwise("New/Low Value")
            )
            .withColumn("_gold_processed_at", current_timestamp())
        )
        
        self._write_gold(customer_360, "customer_360")
        return customer_360
    
    def build_daily_sales_summary(self) -> DataFrame:
        """Aggregate daily sales metrics for executive dashboards."""
        logger.info("Building daily_sales_summary Gold table")
        
        orders = self.spark.read.format("delta").load(f"{SILVER_PATH}/orders")
        
        daily_summary = (
            orders
            .filter(col("order_status") == "completed")
            .withColumn("order_day", date_format(col("order_date"), "yyyy-MM-dd"))
            .groupBy("order_day")
            .agg(
                count("order_id").alias("total_orders"),
                countDistinct("customer_id").alias("unique_customers"),
                _round(_sum("order_amount"), 2).alias("total_revenue"),
                _round(avg("order_amount"), 2).alias("avg_order_value")
            )
            .orderBy(col("order_day").desc())
            .withColumn("_gold_processed_at", current_timestamp())
        )
        
        self._write_gold(daily_summary, "daily_sales_summary")
        return daily_summary
    
    def build_top_customers_ranking(self) -> DataFrame:
        """Rank customers by lifetime value for targeted campaigns."""
        logger.info("Building top_customers_ranking Gold table")
        
        customer_360 = self.spark.read.format("delta").load(f"{GOLD_PATH}/customer_360")
        
        ranking_window = Window.orderBy(col("lifetime_value").desc())
        
        top_customers = (
            customer_360
            .withColumn("value_rank", rank().over(ranking_window))
            .filter(col("value_rank") <= 100)
            .select(
                "customer_id", "customer_name", "email", "customer_segment",
                "lifetime_value", "total_orders", "value_rank"
            )
            .withColumn("_gold_processed_at", current_timestamp())
        )
        
        self._write_gold(top_customers, "top_customers_ranking")
        return top_customers
    
    def _write_gold(self, df: DataFrame, table_name: str):
        """Write Gold table optimized for downstream consumption."""
        output_path = f"{GOLD_PATH}/{table_name}"
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(output_path)
        )
        # Optimize for query performance (Synapse/Power BI consumption)
        self.spark.sql(f"OPTIMIZE delta.`{output_path}`")
        logger.info(f"Gold table {table_name} written and optimized")

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit, when

def coalesce_zero(column):
    """Replace nulls with zero for numeric aggregations."""
    return coalesce(column, lit(0))

# COMMAND ----------

if __name__ == "__main__":
    pipeline = GoldAggregationPipeline(spark)
    
    pipeline.build_customer_360()
    pipeline.build_daily_sales_summary()
    pipeline.build_top_customers_ranking()
    
    logger.info("Gold aggregation complete. Tables ready for Synapse & Power BI.")
