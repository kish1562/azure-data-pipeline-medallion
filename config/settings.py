"""
Configuration settings for the Azure Medallion Data Pipeline.
Loads environment-specific settings and Azure resource configurations.
"""
import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class AzureConfig:
    """Azure resource configuration."""
    storage_account: str = os.getenv("ADLS_STORAGE_ACCOUNT", "stmedalliondata")
    bronze_container: str = "raw-data"
    silver_container: str = "curated-data"
    gold_container: str = "business-data"
    key_vault_scope: str = "azure-keyvault"

    @property
    def bronze_path(self) -> str:
        return f"abfss://{self.bronze_container}@{self.storage_account}.dfs.core.windows.net/bronze"

    @property
    def silver_path(self) -> str:
        return f"abfss://{self.silver_container}@{self.storage_account}.dfs.core.windows.net/silver"

    @property
    def gold_path(self) -> str:
        return f"abfss://{self.gold_container}@{self.storage_account}.dfs.core.windows.net/gold"


@dataclass
class PipelineConfig:
    """Pipeline execution configuration."""
    environment: str = os.getenv("ENVIRONMENT", "dev")
    num_partitions: int = 8
    fetch_size: int = 10000
    enable_optimization: bool = True
    source_tables: List[str] = field(default_factory=lambda: [
        "customers", "orders", "products", "transactions"
    ])

    # Data quality thresholds
    max_acceptable_drop_rate: float = 15.0  # percent
    min_email_validity_rate: float = 90.0   # percent


# Singleton instances
azure_config = AzureConfig()
pipeline_config = PipelineConfig()
