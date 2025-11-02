"""
Compliance Report Generation Job
Generates comprehensive compliance reports for specified tables
"""
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from src.governance import GDPRGovernanceFramework


def main():
    """Main job function for compliance reporting"""
    import argparse
    
    parser = argparse.ArgumentParser(description="GDPR Compliance Reporting")
    parser.add_argument(
        "--tables",
        nargs="+",
        required=True,
        help="Tables to generate reports for"
    )
    parser.add_argument(
        "--output-path",
        help="Path to save report JSON (optional)"
    )
    parser.add_argument(
        "--user",
        help="Filter audit trail by user"
    )
    
    args = parser.parse_args()
    
    print(f"Generating compliance reports for {len(args.tables)} table(s)...")
    
    # Load configuration
    config = load_config()
    
    # Initialize framework
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    
    if spark is None:
        spark = SparkSession.builder.appName("GDPR_Compliance_Report").getOrCreate()
    
    framework = GDPRGovernanceFramework(config=config, spark_session=spark)
    
    reports = {}
    
    for table in args.tables:
        print(f"\nGenerating report for {table}...")
        try:
            report = framework.get_compliance_report(table, user=args.user)
            reports[table] = report
            
            # Print summary
            print(f"  PII Status: {'Detected' if report['pii_status'].get('pii_detected') else 'None'}")
            print(f"  Upstream Sources: {report['lineage'].get('upstream_count', 0)}")
            print(f"  Downstream Targets: {report['lineage'].get('downstream_count', 0)}")
            print(f"  Recent Audit Events: {report['audit_trail'].get('recent_events_count', 0)}")
            print(f"  Retention Policy: {'Registered' if report['retention_policy'].get('registered', True) else 'Not Registered'}")
        
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
            reports[table] = {"error": str(e)}
    
    # Save to file if requested
    if args.output_path:
        with open(args.output_path, 'w') as f:
            json.dump(reports, f, indent=2, default=str)
        print(f"\nReports saved to {args.output_path}")
    
    return reports


if __name__ == "__main__":
    main()

