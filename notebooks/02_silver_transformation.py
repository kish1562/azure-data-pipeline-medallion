# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Data Cleaning & Transformation
# MAGIC Reads from Bronze Delta tables, applies data quality rules, deduplication,
# MAGIC and standardization, then writes cleaned data to Silver Delta tables.

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, trim, lower, upper, regexp_replace, to_date, to_timestamp,
    coalesce, lit, row_number, current_timestamp, sha2, concat_ws
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverTransformation")

# COMMAND ----------

ADLS_STORAGE_ACCOUNT = dbutils.secrets.get(scope="azure-keyvault", key="storage-account-name")
BRONZE_PATH = f"abfss://raw-data@{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net/bronze"
SILVER_PATH = f"abfss://curated-data@{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net/silver"

# COMMAND ----------

class SilverTransformationPipeline:
    """Cleans and standardizes Bronze data into validated Silver Delta tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.quality_metrics = {}
        logger.info("Silver Transformation Pipeline initialized")
    
    def transform_customers(self) -> DataFrame:
        """Clean and standardize customer records."""
        logger.info("Transforming customers Bronze -> Silver")
        
        bronze_df = self.spark.read.format("delta").load(f"{BRONZE_PATH}/customers")
        initial_count = bronze_df.count()
        
        cleaned = (
            bronze_df
            # Standardize text fields
            .withColumn("customer_name", trim(col("customer_name")))
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("country", upper(trim(col("country"))))
            # Clean phone numbers
            .withColumn("phone", regexp_replace(col("phone"), "[^0-9+]", ""))
            # Parse dates safely
            .withColumn("created_date", to_date(col("created_date"), "yyyy-MM-dd"))
            # Handle nulls with defaults
            .withColumn("status", coalesce(col("status"), lit("unknown")))
        )
        
        # Validate email format
        validated = cleaned.withColumn(
            "is_valid_email",
            col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        )
        
        # Deduplicate — keep latest record per customer
        dedup_window = Window.partitionBy("customer_id").orderBy(col("_ingestion_timestamp").desc())
        deduplicated = (
            validated
            .withColumn("_row_num", row_number().over(dedup_window))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )
        
        # Add Silver metadata
        final = (
            deduplicated
            .withColumn("_silver_processed_at", current_timestamp())
            .withColumn("_record_hash", sha2(concat_ws("||", "customer_id", "email", "customer_name"), 256))
        )
        
        final_count = final.count()
        self._track_quality("customers", initial_count, final_count)
        
        self._write_silver(final, "customers", merge_key="customer_id")
        return final
    
    def transform_orders(self) -> DataFrame:
        """Clean and enrich order records with referential integrity checks."""
        logger.info("Transforming orders Bronze -> Silver")
        
        bronze_df = self.spark.read.format("delta").load(f"{BRONZE_PATH}/orders")
        initial_count = bronze_df.count()
        
        cleaned = (
            bronze_df
            .withColumn("order_date", to_timestamp(col("order_date")))
            .withColumn("order_amount", col("order_amount").cast("decimal(12,2)"))
            # Flag invalid amounts
            .withColumn("is_valid_amount", col("order_amount") > 0)
            .withColumn("order_status", lower(trim(col("order_status"))))
            # Standardize status values
            .withColumn(
                "order_status",
                when(col("order_status").isin("complete", "completed", "done"), "completed")
                .when(col("order_status").isin("cancel", "cancelled", "canceled"), "cancelled")
                .when(col("order_status").isin("pending", "processing"), "pending")
                .otherwise("unknown")
            )
        )
        
        # Referential integrity — only keep orders with valid customers
        valid_customers = (
            self.spark.read.format("delta")
            .load(f"{SILVER_PATH}/customers")
            .select("customer_id")
        )
        
        validated = cleaned.join(valid_customers, on="customer_id", how="inner")
        
        final = (
            validated
            .filter(col("is_valid_amount"))
            .withColumn("_silver_processed_at", current_timestamp())
        )
        
        final_count = final.count()
        self._track_quality("orders", initial_count, final_count)
        
        self._write_silver(final, "orders", merge_key="order_id")
        return final
    
    def _write_silver(self, df: DataFrame, table_name: str, merge_key: str):
        """Write or merge data into Silver Delta table."""
        output_path = f"{SILVER_PATH}/{table_name}"
        
        if DeltaTable.isDeltaTable(self.spark, output_path):
            target = DeltaTable.forPath(self.spark, output_path)
            (
                target.alias("t")
                .merge(df.alias("s"), f"t.{merge_key} = s.{merge_key}")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info(f"Merged {table_name} into Silver")
        else:
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(output_path)
            )
            logger.info(f"Created Silver table {table_name}")
    
    def _track_quality(self, table_name: str, initial: int, final: int):
        """Track data quality metrics for monitoring."""
        dropped = initial - final
        drop_rate = round((dropped / initial * 100), 2) if initial > 0 else 0
        
        self.quality_metrics[table_name] = {
            "initial_records": initial,
            "final_records": final,
            "dropped_records": dropped,
            "drop_rate_pct": drop_rate
        }
        logger.info(f"{table_name}: {initial} -> {final} ({drop_rate}% dropped)")

# COMMAND ----------

if __name__ == "__main__":
    pipeline = SilverTransformationPipeline(spark)
    
    pipeline.transform_customers()
    pipeline.transform_orders()
    
    logger.info(f"Silver transformation complete. Quality metrics: {pipeline.quality_metrics}")
