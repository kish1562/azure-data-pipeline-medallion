"""
Unit tests for the Medallion pipeline transformations.
Run with: pytest tests/test_transformations.py
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)


@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("MedallionPipelineTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_customers(spark):
    """Sample customer data with quality issues for testing."""
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
    ])
    data = [
        (1, "  John Doe  ", "JOHN@EXAMPLE.COM", "usa"),
        (2, "Jane Smith", "invalid-email", "uk"),
        (1, "John Doe", "john@example.com", "USA"),  # duplicate
        (3, "Bob Wilson", "bob@test.com", "  canada  "),
    ]
    return spark.createDataFrame(data, schema)


def test_email_standardization(spark, sample_customers):
    """Emails should be lowercased and trimmed."""
    from pyspark.sql.functions import lower, trim, col
    result = sample_customers.withColumn("email", lower(trim(col("email"))))
    emails = [row["email"] for row in result.collect()]
    assert "john@example.com" in emails
    assert all(e == e.lower() for e in emails)


def test_email_validation(spark, sample_customers):
    """Invalid emails should be flagged."""
    from pyspark.sql.functions import col, lower, trim
    result = (
        sample_customers
        .withColumn("email", lower(trim(col("email"))))
        .withColumn(
            "is_valid_email",
            col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        )
    )
    invalid = result.filter(~col("is_valid_email")).count()
    assert invalid == 1  # only "invalid-email"


def test_name_trimming(spark, sample_customers):
    """Customer names should have whitespace trimmed."""
    from pyspark.sql.functions import trim, col
    result = sample_customers.withColumn("customer_name", trim(col("customer_name")))
    names = [row["customer_name"] for row in result.collect()]
    assert "John Doe" in names
    assert "  John Doe  " not in names


def test_uniqueness_validation(spark, sample_customers):
    """Should detect duplicate customer IDs."""
    total = sample_customers.count()
    distinct = sample_customers.select("customer_id").distinct().count()
    assert total == 4
    assert distinct == 3  # customer_id 1 appears twice


def test_country_standardization(spark, sample_customers):
    """Countries should be uppercased and trimmed."""
    from pyspark.sql.functions import upper, trim, col
    result = sample_customers.withColumn("country", upper(trim(col("country"))))
    countries = [row["country"] for row in result.collect()]
    assert "USA" in countries
    assert "CANADA" in countries
    assert "  canada  " not in countries


def test_order_amount_validation(spark):
    """Orders with non-positive amounts should be flagged invalid."""
    from pyspark.sql.functions import col
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("order_amount", DoubleType(), True),
    ])
    data = [(1, 100.0), (2, -50.0), (3, 0.0), (4, 250.5)]
    df = spark.createDataFrame(data, schema)
    result = df.withColumn("is_valid_amount", col("order_amount") > 0)
    valid = result.filter(col("is_valid_amount")).count()
    assert valid == 2  # only 100.0 and 250.5
