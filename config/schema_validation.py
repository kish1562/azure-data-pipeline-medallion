"""
Schema validation rules for the Medallion pipeline.
Defines expected schemas and validation logic for each layer.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType, DecimalType
)
from pyspark.sql import DataFrame
from typing import List, Dict
import logging

logger = logging.getLogger("SchemaValidation")


# Expected schemas for API ingestion
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_date", StringType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("order_status", StringType(), True),
])


class SchemaValidator:
    """Validates DataFrames against expected schemas and business rules."""

    @staticmethod
    def validate_columns(df: DataFrame, required_columns: List[str]) -> Dict[str, bool]:
        """Check that all required columns are present."""
        actual_columns = set(df.columns)
        results = {}
        for col_name in required_columns:
            results[col_name] = col_name in actual_columns
            if not results[col_name]:
                logger.error(f"Missing required column: {col_name}")
        return results

    @staticmethod
    def validate_not_null(df: DataFrame, columns: List[str]) -> Dict[str, int]:
        """Count null values in critical columns."""
        from pyspark.sql.functions import col, count, when
        null_counts = {}
        for column in columns:
            null_count = df.filter(col(column).isNull()).count()
            null_counts[column] = null_count
            if null_count > 0:
                logger.warning(f"Column '{column}' has {null_count} null values")
        return null_counts

    @staticmethod
    def validate_uniqueness(df: DataFrame, key_column: str) -> bool:
        """Verify a key column contains unique values."""
        total = df.count()
        distinct = df.select(key_column).distinct().count()
        is_unique = total == distinct
        if not is_unique:
            logger.error(f"Duplicate keys found in '{key_column}': {total - distinct} duplicates")
        return is_unique

    @staticmethod
    def validate_range(df: DataFrame, column: str, min_val: float, max_val: float) -> int:
        """Count records outside the expected value range."""
        from pyspark.sql.functions import col
        out_of_range = df.filter((col(column) < min_val) | (col(column) > max_val)).count()
        if out_of_range > 0:
            logger.warning(f"Column '{column}' has {out_of_range} out-of-range values")
        return out_of_range
