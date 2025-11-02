"""
Utility script to initialize Databricks tables for GDPR compliance
Run this once to set up the governance tables
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from pyspark.sql import SparkSession


def main():
    """Initialize Databricks tables"""
    print("Initializing GDPR Compliance Tables in Databricks...")
    
    config = load_config()
    
    # Get or create Spark session
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("GDPR_Init").getOrCreate()
    
    # Create catalog if it doesn't exist
    catalog = config.databricks_catalog
    schema = config.databricks_schema
    full_schema = f"{catalog}.{schema}"
    
    print(f"\n1. Creating catalog: {catalog}")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"USE CATALOG {catalog}")
    
    print(f"2. Creating schema: {full_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    spark.sql(f"USE SCHEMA {full_schema}")
    
    # Initialize components to create tables
    print("\n3. Initializing governance components...")
    from src.audit import AuditLogger
    from src.lineage import LineageTracker
    
    audit_logger = AuditLogger(
        catalog=catalog,
        schema=schema,
        spark_session=spark
    )
    audit_logger._initialize_table()
    print("   ✓ Audit logs table created")
    
    lineage_tracker = LineageTracker(
        catalog=catalog,
        schema=schema,
        spark_session=spark
    )
    lineage_tracker._initialize_table()
    print("   ✓ Data lineage table created")
    
    # Create PII registry table
    print("\n4. Creating PII registry table...")
    pii_registry_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_schema}.pii_registry (
        registry_id STRING,
        table_name STRING,
        column_name STRING,
        pii_type STRING,
        detected_at TIMESTAMP,
        confidence DOUBLE,
        row_count BIGINT,
        user STRING
    ) USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    spark.sql(pii_registry_sql)
    print("   ✓ PII registry table created")
    
    # Grant permissions (adjust as needed)
    print("\n5. Setting up permissions...")
    try:
        spark.sql(f"GRANT SELECT ON SCHEMA {full_schema} TO `account users`")
        print("   ✓ Permissions configured")
    except Exception as e:
        print(f"   ⚠️  Permission setup skipped: {str(e)}")
    
    print("\n" + "=" * 60)
    print("✓ GDPR Compliance tables initialized successfully!")
    print("=" * 60)
    print(f"\nTables created in: {full_schema}")
    print("  - audit_logs")
    print("  - data_lineage")
    print("  - pii_registry")
    print("\nYou can now use the GDPR compliance framework!")


if __name__ == "__main__":
    main()

