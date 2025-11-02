"""
Main GDPR Governance Orchestrator
Coordinates all compliance modules
"""
from typing import Optional, Dict, Any, List
from datetime import datetime

from .config import GDPRConfig, load_config
from .pii_detection import PIIDetector, PIIType
from .lineage import LineageTracker, OperationType
from .audit import AuditLogger, AuditEventType
from .retention import RetentionManager
from .pseudonymization import Pseudonymizer
from .k_anonymity import KAnonymityChecker
from .data_quality import DataQualityValidator


class GDPRGovernanceFramework:
    """
    Main GDPR Compliance Framework
    Orchestrates all compliance features
    """
    
    def __init__(
        self,
        config: Optional[GDPRConfig] = None,
        spark_session=None
    ):
        """
        Initialize GDPR Governance Framework
        
        Args:
            config: Configuration object (loads from env if None)
            spark_session: Spark session (optional)
        """
        self.config = config or load_config()
        self.spark = spark_session
        
        # Initialize all components
        self.pii_detector = PIIDetector()
        self.lineage_tracker = LineageTracker(
            catalog=self.config.databricks_catalog,
            schema=self.config.databricks_schema,
            spark_session=self.spark
        )
        self.audit_logger = AuditLogger(
            catalog=self.config.databricks_catalog,
            schema=self.config.databricks_schema,
            spark_session=self.spark
        )
        self.retention_manager = RetentionManager(
            retention_years=self.config.retention_years,
            catalog=self.config.databricks_catalog,
            schema=self.config.databricks_schema,
            spark_session=self.spark
        )
        self.pseudonymizer = Pseudonymizer(self.config.pseudonymization_key)
        self.k_anonymity_checker = KAnonymityChecker(
            k_threshold=self.config.k_anonymity_threshold
        )
        self.quality_validator = DataQualityValidator(
            min_quality_score=self.config.min_data_quality_score
        )
    
    def scan_for_pii(
        self,
        table_name: str,
        user: str = "system",
        sample_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Scan a table for PII
        
        Args:
            table_name: Table to scan
            user: User performing the scan
            sample_size: Number of rows to sample
        
        Returns:
            Dictionary with PII detection results
        """
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            raise ValueError("Spark session required")
        
        # Read sample data
        df_pandas = self.spark.sql(f"SELECT * FROM {table_name}").limit(sample_size).toPandas()
        
        # Detect PII
        detections = self.pii_detector.scan_dataframe(df_pandas, sample_size)
        
        # Extract PII types and columns
        pii_types = set()
        pii_columns = set()
        for column, pii_list in detections.items():
            for pii in pii_list:
                pii_types.add(pii.pii_type.value)
                pii_columns.add(column)
        
        # Log to audit
        if pii_types:
            self.audit_logger.log_pii_detection(
                table_name=table_name,
                user=user,
                pii_types=list(pii_types),
                columns=list(pii_columns)
            )
            self.audit_logger.flush()
        
        return {
            "table_name": table_name,
            "pii_detected": len(detections) > 0,
            "detections": {
                col: [
                    {
                        "type": pii.pii_type.value,
                        "confidence": pii.confidence,
                        "row_count": pii.row_count
                    }
                    for pii in pii_list
                ]
                for col, pii_list in detections.items()
            },
            "summary": self.pii_detector.get_pii_summary(detections)
        }
    
    def process_with_compliance(
        self,
        source_table: str,
        target_table: str,
        user: str,
        transformations: Optional[List[str]] = None,
        pii_columns: Optional[List[str]] = None,
        validate_quality: bool = True,
        ensure_k_anonymity: bool = False,
        quasi_identifiers: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Process data with full GDPR compliance
        
        Args:
            source_table: Source table name
            target_table: Target table name
            user: User performing the operation
            transformations: List of transformation descriptions
            pii_columns: Columns to pseudonymize (if None, auto-detect)
            validate_quality: Whether to validate data quality
            ensure_k_anonymity: Whether to ensure k-anonymity
            quasi_identifiers: Columns to use for k-anonymity check
        
        Returns:
            Dictionary with processing results
        """
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            raise ValueError("Spark session required")
        
        results = {
            "source_table": source_table,
            "target_table": target_table,
            "steps_completed": []
        }
        
        # Track lineage
        lineage_id = self.lineage_tracker.track_transform(
            source_table=source_table,
            target_table=target_table,
            user=user,
            transformation=", ".join(transformations or ["Data processing"]),
            additional_details={"pii_columns": pii_columns}
        )
        results["lineage_id"] = lineage_id
        results["steps_completed"].append("lineage_tracking")
        
        # Read source data
        source_df = self.spark.sql(f"SELECT * FROM {source_table}")
        
        # Scan for PII if columns not specified
        if pii_columns is None:
            pii_scan = self.scan_for_pii(source_table, user)
            if pii_scan["pii_detected"]:
                # Extract columns with detected PII
                pii_columns = [
                    col for col in pii_scan["detections"].keys()
                ]
                results["pii_detected"] = True
                results["pii_columns"] = pii_columns
        
        # Convert to pandas for processing (for smaller datasets)
        # For larger datasets, use Spark operations
        try:
            df_pandas = source_df.toPandas()
            
            # Data quality validation
            if validate_quality:
                validation_result = self.quality_validator.validate(df_pandas, fail_on_error=True)
                results["quality_validation"] = validation_result
                results["steps_completed"].append("quality_validation")
            
            # Pseudonymize PII columns
            if pii_columns:
                df_pandas = self.pseudonymizer.pseudonymize_multiple_columns(
                    df_pandas,
                    pii_columns,
                    deterministic=True
                )
                
                # Log pseudonymization
                self.audit_logger.log_pseudonymization(
                    table_name=target_table,
                    user=user,
                    columns_pseudonymized=pii_columns,
                    row_count=len(df_pandas)
                )
                results["steps_completed"].append("pseudonymization")
            
            # K-anonymity check and enforcement
            if ensure_k_anonymity and quasi_identifiers:
                df_pandas, k_result = self.k_anonymity_checker.ensure_k_anonymity(
                    df_pandas,
                    quasi_identifiers,
                    apply_suppression=True
                )
                results["k_anonymity"] = {
                    "k_value": k_result.k_value,
                    "compliant": k_result.is_compliant
                }
                results["steps_completed"].append("k_anonymity")
            
            # Write to target table
            target_spark_df = self.spark.createDataFrame(df_pandas)
            target_spark_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(target_table)
            
            results["steps_completed"].append("write")
            
        except Exception as e:
            results["error"] = str(e)
            raise
        
        # Flush lineage and audit
        self.lineage_tracker.flush()
        self.audit_logger.flush()
        
        return results
    
    def apply_retention_policies(self) -> Dict[str, Dict[str, Any]]:
        """Apply all registered retention policies"""
        return self.retention_manager.apply_all_policies(self.audit_logger)
    
    def create_aggregate_view(
        self,
        source_table: str,
        target_view: str,
        group_by_columns: List[str],
        aggregate_columns: Dict[str, str],
        user: str = "system"
    ):
        """
        Create an aggregate-only view (no raw PII)
        
        Args:
            source_table: Source table
            target_view: Target view name
            group_by_columns: Columns to group by
            aggregate_columns: Dict of {column: aggregation_function}
            user: User creating the view
        """
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            raise ValueError("Spark session required")
        
        # Read source
        df_pandas = self.spark.sql(f"SELECT * FROM {source_table}").toPandas()
        
        # Create aggregated view
        aggregated_df = self.k_anonymity_checker.create_aggregate_only_view(
            df_pandas,
            group_by_columns,
            aggregate_columns
        )
        
        # Write as view
        aggregated_spark_df = self.spark.createDataFrame(aggregated_df)
        aggregated_spark_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_view)
        
        # Track lineage
        self.lineage_tracker.track_transform(
            source_table=source_table,
            target_table=target_view,
            user=user,
            transformation="Aggregation only (no raw PII)",
            additional_details={
                "group_by": group_by_columns,
                "aggregations": aggregate_columns
            }
        )
        self.lineage_tracker.flush()
        
        # Log to audit
        self.audit_logger.log(
            event_type=AuditEventType.DATA_MODIFICATION,
            table_name=target_view,
            user=user,
            details={
                "operation": "create_aggregate_view",
                "source_table": source_table,
                "group_by": group_by_columns
            }
        )
        self.audit_logger.flush()
    
    def get_compliance_report(
        self,
        table_name: str,
        user: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a comprehensive compliance report
        
        Args:
            table_name: Table to report on
            user: Optional user filter
        
        Returns:
            Dictionary with compliance information
        """
        report = {
            "table_name": table_name,
            "timestamp": datetime.utcnow().isoformat(),
            "pii_status": {},
            "lineage": {},
            "audit_trail": {},
            "retention_policy": {}
        }
        
        # PII scan
        try:
            pii_scan = self.scan_for_pii(table_name)
            report["pii_status"] = pii_scan
        except Exception as e:
            report["pii_status"] = {"error": str(e)}
        
        # Lineage
        try:
            lineage = self.lineage_tracker.get_complete_lineage(table_name)
            report["lineage"] = {
                "upstream_count": len(lineage.get("upstream", [])),
                "downstream_count": len(lineage.get("downstream", []))
            }
        except Exception as e:
            report["lineage"] = {"error": str(e)}
        
        # Audit trail (recent)
        try:
            from datetime import timedelta
            start_date = datetime.utcnow() - timedelta(days=30)
            audit_trail = self.audit_logger.get_audit_trail(
                table_name=table_name,
                user=user,
                start_date=start_date
            )
            report["audit_trail"] = {
                "recent_events_count": len(audit_trail),
                "last_30_days": True
            }
        except Exception as e:
            report["audit_trail"] = {"error": str(e)}
        
        # Retention policy
        if table_name in self.retention_manager.policies:
            policy = self.retention_manager.policies[table_name]
            report["retention_policy"] = {
                "enabled": policy.enabled,
                "retention_years": policy.retention_years,
                "date_column": policy.date_column
            }
        else:
            report["retention_policy"] = {"registered": False}
        
        return report

