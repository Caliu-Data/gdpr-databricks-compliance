"""
Configuration management for GDPR compliance framework
"""
import os
from pathlib import Path
from typing import Optional
try:
    from pydantic_settings import BaseSettings
    from pydantic import Field
except ImportError:
    # Fallback for older pydantic versions
    from pydantic import BaseSettings, Field


class GDPRConfig(BaseSettings):
    """Configuration for GDPR compliance framework"""
    
    # Databricks Configuration
    databricks_host: str = Field(..., env="DATABRICKS_HOST")
    databricks_token: str = Field(..., env="DATABRICKS_TOKEN")
    databricks_cluster_id: Optional[str] = Field(None, env="DATABRICKS_CLUSTER_ID")
    
    # Database Configuration
    databricks_catalog: str = Field("gdpr_compliance", env="DATABRICKS_CATALOG")
    databricks_schema: str = Field("governance", env="DATABRICKS_SCHEMA")
    
    # Encryption Keys
    pseudonymization_key: str = Field(..., env="PSEUDONYMIZATION_KEY")
    encryption_key: str = Field(..., env="ENCRYPTION_KEY")
    
    # Retention Policy
    retention_years: int = Field(7, env="RETENTION_YEARS")
    
    # Audit Configuration
    audit_log_table: str = Field(
        "gdpr_compliance.governance.audit_logs",
        env="AUDIT_LOG_TABLE"
    )
    lineage_table: str = Field(
        "gdpr_compliance.governance.data_lineage",
        env="LINEAGE_TABLE"
    )
    pii_registry_table: str = Field(
        "gdpr_compliance.governance.pii_registry",
        env="PII_REGISTRY_TABLE"
    )
    
    # K-Anonymity Configuration
    k_anonymity_threshold: int = Field(5, env="K_ANONYMITY_THRESHOLD")
    
    # Data Quality Thresholds
    min_data_quality_score: float = Field(0.8, env="MIN_DATA_QUALITY_SCORE")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"


def load_config() -> GDPRConfig:
    """Load configuration from environment variables"""
    # Try to find .env file in project root
    project_root = Path(__file__).parent.parent
    env_file = project_root / ".env"
    
    if env_file.exists():
        return GDPRConfig(_env_file=str(env_file))
    # Try loading from environment variables directly
    return GDPRConfig()

