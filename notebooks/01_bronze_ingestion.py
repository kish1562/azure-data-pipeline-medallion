# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Data Ingestion
# MAGIC Ingests raw data from SQL Server and REST APIs into Delta Lake Bronze tables
# MAGIC on Azure Data Lake Storage Gen2.

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeIngestion")

# COMMAND ----------

# Configuration
ADLS_STORAGE_ACCOUNT = dbutils.secrets.get(scope="azure-keyvault", key="storage-account-name")
ADLS_CONTAINER = "raw-data"
BRONZE_PATH = f"abfss://{ADLS_CONTAINER}@{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net/bronze"

SQL_SERVER_JDBC_URL = dbutils.secrets.get(scope="azure-keyvault", key="sql-server-jdbc-url")
SQL_SERVER_USER = dbutils.secrets.get(scope="azure-keyvault", key="sql-server-user")
SQL_SERVER_PASSWORD = dbutils.secrets.get(scope="azure-keyvault", key="sql-server-password")

# COMMAND ----------

class BronzeIngestionPipeline:
    """Handles raw data ingestion from multiple sources into Bronze Delta tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bronze_path = BRONZE_PATH
        logger.info("Bronze Ingestion Pipeline initialized")
    
    def ingest_from_sql_server(self, table_name: str, partition_column: str = None) -> int:
        """
        Ingest data from on-premises SQL Server into Bronze Delta table.
        
        Args:
            table_name: Source SQL Server table name
            partition_column: Column for parallel read partitioning
            
        Returns:
            Number of records ingested
        """
        logger.info(f"Starting SQL Server ingestion for table: {table_name}")
        
        reader = (
            self.spark.read
            .format("jdbc")
            .option("url", SQL_SERVER_JDBC_URL)
            .option("dbtable", table_name)
            .option("user", SQL_SERVER_USER)
            .option("password", SQL_SERVER_PASSWORD)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("fetchsize", "10000")
        )
        
        if partition_column:
            bounds = self._get_partition_bounds(table_name, partition_column)
            reader = (
                reader
                .option("partitionColumn", partition_column)
                .option("lowerBound", bounds["min"])
                .option("upperBound", bounds["max"])
                .option("numPartitions", 8)
            )
        
        df = reader.load()
        
        # Add metadata columns for lineage tracking
        df_with_metadata = (
            df
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_system", lit("sql_server"))
            .withColumn("_source_table", lit(table_name))
            .withColumn("_batch_id", lit(self._generate_batch_id()))
        )
        
        # Write to Bronze Delta table with merge for incremental loads
        output_path = f"{self.bronze_path}/{table_name}"
        record_count = df_with_metadata.count()
        
        if DeltaTable.isDeltaTable(self.spark, output_path):
            logger.info(f"Performing incremental merge for {table_name}")
            self._merge_to_bronze(df_with_metadata, output_path, table_name)
        else:
            logger.info(f"Initial full load for {table_name}")
            (
                df_with_metadata.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("_ingestion_timestamp")
                .save(output_path)
            )
        
        logger.info(f"Ingested {record_count} records from {table_name} to Bronze")
        return record_count
    
    def ingest_from_rest_api(self, api_url: str, entity_name: str, schema: StructType) -> int:
        """
        Ingest data from REST API endpoints into Bronze Delta table.
        
        Args:
            api_url: REST API endpoint URL
            entity_name: Name for the Bronze table
            schema: Expected PySpark schema for the API response
            
        Returns:
            Number of records ingested
        """
        logger.info(f"Starting REST API ingestion from: {api_url}")
        
        import requests
        import json
        
        headers = {
            "Authorization": f"Bearer {dbutils.secrets.get(scope='azure-keyvault', key='api-token')}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        records = data.get("results", data) if isinstance(data, dict) else data
        
        df = self.spark.createDataFrame(records, schema=schema)
        
        df_with_metadata = (
            df
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_system", lit("rest_api"))
            .withColumn("_source_endpoint", lit(api_url))
            .withColumn("_batch_id", lit(self._generate_batch_id()))
        )
        
        output_path = f"{self.bronze_path}/{entity_name}"
        record_count = df_with_metadata.count()
        
        (
            df_with_metadata.write
            .format("delta")
            .mode("append")
            .save(output_path)
        )
        
        logger.info(f"Ingested {record_count} records from API to Bronze/{entity_name}")
        return record_count
    
    def _merge_to_bronze(self, source_df, target_path: str, table_name: str):
        """Perform upsert merge into existing Bronze Delta table."""
        target_table = DeltaTable.forPath(self.spark, target_path)
        
        merge_key = self._get_merge_key(table_name)
        
        (
            target_table.alias("target")
            .merge(source_df.alias("source"), f"target.{merge_key} = source.{merge_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    def _get_partition_bounds(self, table_name: str, partition_column: str) -> dict:
        """Get min/max values for JDBC partitioning."""
        bounds_query = f"(SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {table_name}) t"
        
        bounds_df = (
            self.spark.read
            .format("jdbc")
            .option("url", SQL_SERVER_JDBC_URL)
            .option("dbtable", bounds_query)
            .option("user", SQL_SERVER_USER)
            .option("password", SQL_SERVER_PASSWORD)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
        )
        
        row = bounds_df.first()
        return {"min": row["min_val"], "max": row["max_val"]}
    
    def _get_merge_key(self, table_name: str) -> str:
        """Return primary key column for each source table."""
        merge_keys = {
            "customers": "customer_id",
            "orders": "order_id",
            "products": "product_id",
            "transactions": "transaction_id",
        }
        return merge_keys.get(table_name, "id")
    
    def _generate_batch_id(self) -> str:
        """Generate unique batch ID for lineage tracking."""
        from datetime import datetime
        return f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# COMMAND ----------

# Execute Bronze Ingestion
if __name__ == "__main__":
    pipeline = BronzeIngestionPipeline(spark)
    
    # Ingest core tables from SQL Server
    source_tables = [
        ("dbo.customers", "customer_id"),
        ("dbo.orders", "order_id"),
        ("dbo.products", "product_id"),
        ("dbo.transactions", "transaction_id"),
    ]
    
    total_records = 0
    for table, partition_col in source_tables:
        count = pipeline.ingest_from_sql_server(table, partition_col)
        total_records += count
    
    logger.info(f"Bronze ingestion complete. Total records: {total_records}")
