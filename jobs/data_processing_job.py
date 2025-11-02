"""
Data Processing Job with GDPR Compliance
Processes data with automatic compliance checks
"""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from src.governance import GDPRGovernanceFramework


def main():
    """Main job function for compliant data processing"""
    import argparse
    
    parser = argparse.ArgumentParser(description="GDPR-Compliant Data Processing")
    parser.add_argument("--source-table", required=True, help="Source table name")
    parser.add_argument("--target-table", required=True, help="Target table name")
    parser.add_argument("--user", default="system", help="User performing operation")
    parser.add_argument(
        "--pii-columns",
        nargs="+",
        help="Columns to pseudonymize (auto-detect if not provided)"
    )
    parser.add_argument(
        "--quasi-identifiers",
        nargs="+",
        help="Quasi-identifiers for k-anonymity check"
    )
    parser.add_argument(
        "--ensure-k-anonymity",
        action="store_true",
        help="Ensure k-anonymity compliance"
    )
    parser.add_argument(
        "--skip-quality-check",
        action="store_true",
        help="Skip data quality validation"
    )
    
    args = parser.parse_args()
    
    print(f"Processing {args.source_table} -> {args.target_table} with GDPR compliance...")
    
    # Load configuration
    config = load_config()
    
    # Initialize framework
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    
    if spark is None:
        spark = SparkSession.builder.appName("GDPR_Data_Processing").getOrCreate()
    
    framework = GDPRGovernanceFramework(config=config, spark_session=spark)
    
    try:
        result = framework.process_with_compliance(
            source_table=args.source_table,
            target_table=args.target_table,
            user=args.user,
            pii_columns=args.pii_columns,
            validate_quality=not args.skip_quality_check,
            ensure_k_anonymity=args.ensure_k_anonymity,
            quasi_identifiers=args.quasi_identifiers
        )
        
        print("\n✓ Processing completed successfully!")
        print(f"  Steps completed: {', '.join(result['steps_completed'])}")
        
        if "pii_detected" in result:
            print(f"  PII detected: {result['pii_detected']}")
            if result.get("pii_columns"):
                print(f"  PII columns: {', '.join(result['pii_columns'])}")
        
        if "k_anonymity" in result:
            k_result = result["k_anonymity"]
            print(f"  K-anonymity: k={k_result['k_value']}, compliant={k_result['compliant']}")
        
        if "quality_validation" in result:
            quality = result["quality_validation"]
            print(f"  Quality score: {quality['quality_score']:.2%}")
        
        return result
    
    except Exception as e:
        print(f"\n✗ Processing failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()

