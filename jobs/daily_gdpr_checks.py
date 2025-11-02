"""
Daily GDPR Compliance Checks Job
Runs automated compliance checks and applies retention policies
"""
import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from src.governance import GDPRGovernanceFramework
from src.audit import AuditEventType


def main():
    """Main job function"""
    print("Starting Daily GDPR Compliance Checks...")
    
    # Load configuration
    config = load_config()
    
    # Initialize framework (will get Spark session from context)
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    
    if spark is None:
        spark = SparkSession.builder.appName("GDPR_Daily_Checks").getOrCreate()
    
    framework = GDPRGovernanceFramework(config=config, spark_session=spark)
    
    print("\n1. Applying Retention Policies...")
    retention_results = framework.apply_retention_policies()
    
    for table, result in retention_results.items():
        status = result.get("status", "unknown")
        rows_deleted = result.get("rows_deleted", 0)
        print(f"  {table}: {status}, {rows_deleted} rows deleted")
    
    print("\n2. Scanning Registered Tables for PII...")
    # You can configure which tables to scan
    tables_to_scan = [
        # Add your tables here
        # "catalog.schema.table1",
        # "catalog.schema.table2",
    ]
    
    for table in tables_to_scan:
        try:
            pii_result = framework.scan_for_pii(table, user="system")
            if pii_result["pii_detected"]:
                print(f"  ⚠️  PII detected in {table}")
                for col, detections in pii_result["detections"].items():
                    print(f"     - {col}: {len(detections)} types")
            else:
                print(f"  ✓ No PII detected in {table}")
        except Exception as e:
            print(f"  ✗ Error scanning {table}: {str(e)}")
    
    print("\n3. Validating Data Quality...")
    # Add your quality checks here
    
    print("\nDaily GDPR Compliance Checks Completed!")
    
    # Log completion
    framework.audit_logger.log(
        event_type=AuditEventType.USER_ACTION,
        table_name="system",
        user="system",
        details={
            "action": "daily_gdpr_checks",
            "status": "completed",
            "retention_results": retention_results
        },
        job_id=os.environ.get("DATABRICKS_JOB_ID", "unknown")
    )
    framework.audit_logger.flush()


if __name__ == "__main__":
    main()

