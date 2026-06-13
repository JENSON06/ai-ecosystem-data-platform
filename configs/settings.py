"""
Central settings loader.

Why: Centralizing config avoids scattered os.getenv() calls across files.
      Pydantic validates types at startup — you find bad config immediately,
      not halfway through a 2-hour Spark job.
"""

import os
from pathlib import Path
import yaml
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


class PostgresSettings(BaseModel):
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "ai_platform")
    user: str = os.getenv("POSTGRES_USER", "platform")
    password: str = os.getenv("POSTGRES_PASSWORD", "platform123")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class DataLakeSettings(BaseModel):
    base_path: str = os.getenv("DATA_LAKE_PATH", "/opt/data_lake")

    @property
    def raw(self) -> str:
        return f"{self.base_path}/raw"

    @property
    def silver(self) -> str:
        return f"{self.base_path}/silver"

    @property
    def gold(self) -> str:
        return f"{self.base_path}/gold"


class SparkSettings(BaseModel):
    master: str = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    app_name: str = "AIEcosystemPlatform"
    executor_memory: str = "1g"
    driver_memory: str = "1g"


# Singleton instances — import these directly in other modules
postgres = PostgresSettings()
data_lake = DataLakeSettings()
spark_cfg = SparkSettings()
