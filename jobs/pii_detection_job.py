"""
PII Detection Job
Scans specified tables for PII and registers findings
"""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from src.governance import GDPRGovernanceFramework


def main():
    """Main job function for PII detection"""
    import argparse
    
    parser = argparse.ArgumentParser(description="PII Detection Job")
    parser.add_argument(
        "--tables",
        nargs="+",
        required=True,
        help="Tables to scan for PII"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Sample size for PII detection"
    )
    
    args = parser.parse_args()
    
    print(f"Starting PII Detection for {len(args.tables)} table(s)...")
    
    # Load configuration
    config = load_config()
    
    # Initialize framework
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    
    if spark is None:
        spark = SparkSession.builder.appName("GDPR_PII_Detection").getOrCreate()
    
    framework = GDPRGovernanceFramework(config=config, spark_session=spark)
    
    results = {}
    
    for table in args.tables:
        print(f"\nScanning {table}...")
        try:
            result = framework.scan_for_pii(
                table_name=table,
                user="system",
                sample_size=args.sample_size
            )
            results[table] = result
            
            if result["pii_detected"]:
                print(f"  ⚠️  PII DETECTED in {table}")
                print(f"  {result['summary']}")
            else:
                print(f"  ✓ No PII detected in {table}")
        
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
            results[table] = {"error": str(e)}
    
    print(f"\nPII Detection completed for {len(args.tables)} table(s)")
    return results


if __name__ == "__main__":
    main()

