"""
Complete Audit Trail System
Records all data access and modifications for compliance
"""
from datetime import datetime
from typing import Dict, Optional, Any, List
from dataclasses import dataclass, asdict
from enum import Enum
import json


class AuditEventType(Enum):
    """Types of audit events"""
    DATA_ACCESS = "data_access"
    DATA_MODIFICATION = "data_modification"
    DATA_DELETION = "data_deletion"
    SCHEMA_CHANGE = "schema_change"
    PII_DETECTED = "pii_detected"
    PSEUDONYMIZATION = "pseudonymization"
    ANONYMIZATION = "anonymization"
    RETENTION_APPLIED = "retention_applied"
    QUALITY_CHECK = "quality_check"
    EXPORT = "export"
    USER_ACTION = "user_action"


@dataclass
class AuditLog:
    """Represents an audit log entry"""
    event_type: AuditEventType
    table_name: str
    user: str
    timestamp: datetime
    details: Dict[str, Any]
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    job_id: Optional[str] = None
    notebook_path: Optional[str] = None
    audit_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage"""
        data = asdict(self)
        data['event_type'] = self.event_type.value
        data['timestamp'] = self.timestamp.isoformat()
        data['details'] = json.dumps(data['details'])
        return data


class AuditLogger:
    """Comprehensive audit logging system"""
    
    def __init__(
        self,
        catalog: str,
        schema: str,
        table_name: str = "audit_logs",
        spark_session=None
    ):
        """
        Initialize audit logger
        
        Args:
            catalog: Databricks catalog name
            schema: Schema name
            table_name: Name of audit log table
            spark_session: Spark session (optional)
        """
        self.catalog = catalog
        self.schema = schema
        self.table_name = table_name
        self.full_table_name = f"{catalog}.{schema}.{table_name}"
        self.spark = spark_session
        self.log_buffer: List[AuditLog] = []
    
    def _initialize_table(self):
        """Initialize the audit log table if it doesn't exist"""
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            raise ValueError("Spark session required for table initialization")
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.full_table_name} (
            audit_id STRING,
            event_type STRING,
            table_name STRING,
            user STRING,
            timestamp TIMESTAMP,
            details STRING,
            ip_address STRING,
            user_agent STRING,
            job_id STRING,
            notebook_path STRING
        ) USING DELTA
        PARTITIONED BY (event_type, DATE(timestamp))
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        
        self.spark.sql(create_table_sql)
    
    def log(
        self,
        event_type: AuditEventType,
        table_name: str,
        user: str,
        details: Dict[str, Any],
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        job_id: Optional[str] = None,
        notebook_path: Optional[str] = None
    ):
        """
        Log an audit event
        
        Args:
            event_type: Type of event
            table_name: Table affected
            user: User who performed the action
            details: Additional event details
            ip_address: IP address of user
            user_agent: User agent string
            job_id: Optional job ID
            notebook_path: Optional notebook path
        """
        import uuid
        
        audit_log = AuditLog(
            audit_id=str(uuid.uuid4()),
            event_type=event_type,
            table_name=table_name,
            user=user,
            timestamp=datetime.utcnow(),
            details=details,
            ip_address=ip_address,
            user_agent=user_agent,
            job_id=job_id,
            notebook_path=notebook_path
        )
        
        self.log_buffer.append(audit_log)
    
    def log_data_access(
        self,
        table_name: str,
        user: str,
        columns_accessed: List[str],
        row_count: Optional[int] = None,
        filters: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        """Log data access event"""
        self.log(
            event_type=AuditEventType.DATA_ACCESS,
            table_name=table_name,
            user=user,
            details={
                "columns_accessed": columns_accessed,
                "row_count": row_count,
                "filters": filters or {}
            },
            **kwargs
        )
    
    def log_pii_detection(
        self,
        table_name: str,
        user: str,
        pii_types: List[str],
        columns: List[str],
        **kwargs
    ):
        """Log PII detection event"""
        self.log(
            event_type=AuditEventType.PII_DETECTED,
            table_name=table_name,
            user=user,
            details={
                "pii_types": pii_types,
                "columns": columns
            },
            **kwargs
        )
    
    def log_pseudonymization(
        self,
        table_name: str,
        user: str,
        columns_pseudonymized: List[str],
        row_count: int,
        **kwargs
    ):
        """Log pseudonymization event"""
        self.log(
            event_type=AuditEventType.PSEUDONYMIZATION,
            table_name=table_name,
            user=user,
            details={
                "columns_pseudonymized": columns_pseudonymized,
                "row_count": row_count
            },
            **kwargs
        )
    
    def log_data_deletion(
        self,
        table_name: str,
        user: str,
        row_count: int,
        reason: str,
        **kwargs
    ):
        """Log data deletion event"""
        self.log(
            event_type=AuditEventType.DATA_DELETION,
            table_name=table_name,
            user=user,
            details={
                "row_count": row_count,
                "reason": reason
            },
            **kwargs
        )
    
    def log_retention_applied(
        self,
        table_name: str,
        rows_deleted: int,
        retention_years: int,
        **kwargs
    ):
        """Log retention policy application"""
        self.log(
            event_type=AuditEventType.RETENTION_APPLIED,
            table_name=table_name,
            user="system",
            details={
                "rows_deleted": rows_deleted,
                "retention_years": retention_years
            },
            **kwargs
        )
    
    def flush(self):
        """Flush audit log buffer to the audit table"""
        if not self.log_buffer:
            return
        
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            raise ValueError("Spark session required to flush audit logs")
        
        # Initialize table if needed
        try:
            self._initialize_table()
        except Exception:
            pass  # Table might already exist
        
        # Convert audit logs to DataFrame
        import pandas as pd
        
        records = []
        for log_entry in self.log_buffer:
            records.append(log_entry.to_dict())
        
        if records:
            df = pd.DataFrame(records)
            spark_df = self.spark.createDataFrame(df)
            
            # Write to Delta table
            spark_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(self.full_table_name)
        
        # Clear buffer
        self.log_buffer = []
    
    def get_audit_trail(
        self,
        table_name: Optional[str] = None,
        user: Optional[str] = None,
        event_type: Optional[AuditEventType] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Retrieve audit trail with filters
        
        Args:
            table_name: Filter by table name
            user: Filter by user
            event_type: Filter by event type
            start_date: Start date for filtering
            end_date: End date for filtering
        
        Returns:
            List of audit log entries
        """
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            return []
        
        try:
            conditions = []
            
            if table_name:
                conditions.append(f"table_name = '{table_name}'")
            if user:
                conditions.append(f"user = '{user}'")
            if event_type:
                conditions.append(f"event_type = '{event_type.value}'")
            if start_date:
                conditions.append(f"timestamp >= '{start_date.isoformat()}'")
            if end_date:
                conditions.append(f"timestamp <= '{end_date.isoformat()}'")
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            query = f"""
            SELECT * FROM {self.full_table_name}
            WHERE {where_clause}
            ORDER BY timestamp DESC
            """
            
            df = self.spark.sql(query)
            return df.collect()
        except Exception:
            return []

