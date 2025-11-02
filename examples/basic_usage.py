"""
Example: Basic Usage of GDPR Compliance Framework
Demonstrates common compliance operations
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from src.governance import GDPRGovernanceFramework
from pyspark.sql import SparkSession


def example_1_scan_for_pii():
    """Example: Scan a table for PII"""
    print("=" * 60)
    print("Example 1: Scanning for PII")
    print("=" * 60)
    
    spark = SparkSession.getActiveSession() or SparkSession.builder.appName("GDPR_Example").getOrCreate()
    config = load_config()
    framework = GDPRGovernanceFramework(config=config, spark_session=spark)
    
    # Scan a table (replace with your table name)
    table_name = "your_catalog.your_schema.your_table"
    
    try:
        result = framework.scan_for_pii(
            table_name=table_name,
            user="data_engineer",
            sample_size=1000
        )
        
        print(f"\nScan Results for {table_name}:")
        print(result["summary"])
        
        if result["pii_detected"]:
            print("\n⚠️  PII detected! Consider pseudonymization.")
        else:
            print("\n✓ No PII detected.")
    
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Make sure the table exists and is accessible.")


def example_2_process_with_compliance():
    """Example: Process data with full GDPR compliance"""
    print("\n" + "=" * 60)
    print("Example 2: Processing Data with GDPR Compliance")
    print("=" * 60)
    
    spark = SparkSession.getActiveSession() or SparkSession.builder.appName("GDPR_Example").getOrCreate()
    config = load_config()
    framework = GDPRGovernanceFramework(config=config, spark_session=spark)
    
    source_table = "your_catalog.your_schema.raw_customers"
    target_table = "your_catalog.your_schema.processed_customers"
    
    try:
        result = framework.process_with_compliance(
            source_table=source_table,
            target_table=target_table,
            user="data_engineer",
            pii_columns=None,  # Auto-detect
            validate_quality=True,
            ensure_k_anonymity=True,
            quasi_identifiers=["age", "zip_code", "gender"]
        )
        
        print(f"\n✓ Processing completed!")
        print(f"Steps: {', '.join(result['steps_completed'])}")
        
        if result.get("pii_detected"):
            print(f"PII columns processed: {', '.join(result.get('pii_columns', []))}")
    
    except Exception as e:
        print(f"Error: {str(e)}")


def example_3_retention_policy():
    """Example: Register and apply retention policies"""
    print("\n" + "=" * 60)
    print("Example 3: Retention Policy Management")
    print("=" * 60)
    
    spark = SparkSession.getActiveSession() or SparkSession.builder.appName("GDPR_Example").getOrCreate()
    config = load_config()
    framework = GDPRGovernanceFramework(config=config, spark_session=spark)
    
    # Register a retention policy
    table_name = "your_catalog.your_schema.transactions"
    
    framework.retention_manager.register_policy(
        table_name=table_name,
        date_column="transaction_date",
        retention_years=7,
        enabled=True,
        soft_delete=False  # Hard delete
    )
    
    print(f"✓ Retention policy registered for {table_name}")
    print("\nPolicy Summary:")
    print(framework.retention_manager.get_policy_summary())
    
    # Apply retention (usually done automatically via scheduled job)
    print("\nApplying retention policies...")
    results = framework.apply_retention_policies()
    
    for table, result in results.items():
        status = result.get("status")
        rows_deleted = result.get("rows_deleted", 0)
        print(f"  {table}: {status}, {rows_deleted} rows deleted")


def example_4_compliance_report():
    """Example: Generate compliance report"""
    print("\n" + "=" * 60)
    print("Example 4: Compliance Report Generation")
    print("=" * 60)
    
    spark = SparkSession.getActiveSession() or SparkSession.builder.appName("GDPR_Example").getOrCreate()
    config = load_config()
    framework = GDPRGovernanceFramework(config=config, spark_session=spark)
    
    table_name = "your_catalog.your_schema.customer_data"
    
    try:
        report = framework.get_compliance_report(table_name=table_name)
        
        print(f"\nCompliance Report for {table_name}:")
        print(f"Timestamp: {report['timestamp']}")
        print(f"\nPII Status: {'Detected' if report['pii_status'].get('pii_detected') else 'None'}")
        print(f"Lineage - Upstream: {report['lineage'].get('upstream_count', 0)}")
        print(f"Lineage - Downstream: {report['lineage'].get('downstream_count', 0)}")
        print(f"Recent Audit Events: {report['audit_trail'].get('recent_events_count', 0)}")
        
        retention = report['retention_policy']
        if retention.get('registered'):
            print(f"Retention Policy: {retention.get('retention_years')} years")
        else:
            print("Retention Policy: Not registered")
    
    except Exception as e:
        print(f"Error: {str(e)}")


def example_5_create_aggregate_view():
    """Example: Create aggregate-only view (no raw PII)"""
    print("\n" + "=" * 60)
    print("Example 5: Creating Aggregate-Only View")
    print("=" * 60)
    
    spark = SparkSession.getActiveSession() or SparkSession.builder.appName("GDPR_Example").getOrCreate()
    config = load_config()
    framework = GDPRGovernanceFramework(config=config, spark_session=spark)
    
    source_table = "your_catalog.your_schema.customer_data"
    target_view = "your_catalog.your_schema.customer_analytics"
    
    try:
        framework.create_aggregate_view(
            source_table=source_table,
            target_view=target_view,
            group_by_columns=["region", "age_group"],
            aggregate_columns={
                "revenue": "sum",
                "transaction_count": "count",
                "avg_order_value": "mean"
            },
            user="data_engineer"
        )
        
        print(f"✓ Aggregate view created: {target_view}")
        print("This view contains only aggregations, no raw PII.")
    
    except Exception as e:
        print(f"Error: {str(e)}")


def main():
    """Run all examples"""
    print("\n" + "=" * 60)
    print("GDPR Compliance Framework - Usage Examples")
    print("=" * 60)
    print("\nNote: Update table names in the examples before running.")
    print("These examples demonstrate the framework capabilities.")
    
    # Uncomment to run specific examples:
    # example_1_scan_for_pii()
    # example_2_process_with_compliance()
    # example_3_retention_policy()
    # example_4_compliance_report()
    # example_5_create_aggregate_view()
    
    print("\n" + "=" * 60)
    print("Examples are ready to use!")
    print("Uncomment the example functions in main() to run them.")
    print("=" * 60)


if __name__ == "__main__":
    main()

