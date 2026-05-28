"""Shared helper functions for Medallion pipeline notebooks."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan
import logging

logger = logging.getLogger("PipelineUtils")


def profile_dataframe(df: DataFrame, name: str = "DataFrame") -> dict:
    """Generate a data quality profile for a DataFrame."""
    total_rows = df.count()
    total_cols = len(df.columns)

    null_counts = {}
    for c in df.columns:
        null_count = df.filter(col(c).isNull()).count()
        null_counts[c] = {
            "nulls": null_count,
            "null_pct": round(null_count / total_rows * 100, 2) if total_rows > 0 else 0
        }

    profile = {
        "name": name,
        "total_rows": total_rows,
        "total_columns": total_cols,
        "null_summary": null_counts
    }
    logger.info(f"Profile for {name}: {total_rows} rows, {total_cols} columns")
    return profile


def optimize_delta_table(spark, path: str, zorder_columns: list = None):
    """Optimize a Delta table with optional Z-ordering for query performance."""
    if zorder_columns:
        cols = ", ".join(zorder_columns)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
        logger.info(f"Optimized {path} with Z-ordering on {cols}")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")
        logger.info(f"Optimized {path}")


def vacuum_delta_table(spark, path: str, retention_hours: int = 168):
    """Remove old files from Delta table (default 7 day retention)."""
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    logger.info(f"Vacuumed {path} (retention: {retention_hours}h)")
