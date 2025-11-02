"""
7-Year Retention Policy Enforcement
Automatically deletes data older than retention period
"""
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class RetentionPolicy:
    """Retention policy configuration"""
    table_name: str
    date_column: str
    retention_years: int
    enabled: bool = True
    soft_delete: bool = False  # If True, marks as deleted instead of hard delete


class RetentionManager:
    """Manages data retention policies"""
    
    def __init__(
        self,
        retention_years: int = 7,
        catalog: str = "gdpr_compliance",
        schema: str = "governance",
        spark_session=None
    ):
        """
        Initialize retention manager
        
        Args:
            retention_years: Default retention period in years
            catalog: Databricks catalog name
            schema: Schema name
            spark_session: Spark session (optional)
        """
        self.retention_years = retention_years
        self.catalog = catalog
        self.schema = schema
        self.spark = spark_session
        self.policies: Dict[str, RetentionPolicy] = {}
    
    def register_policy(
        self,
        table_name: str,
        date_column: str,
        retention_years: Optional[int] = None,
        enabled: bool = True,
        soft_delete: bool = False
    ):
        """
        Register a retention policy for a table
        
        Args:
            table_name: Full table name (catalog.schema.table)
            date_column: Column containing the date to check
            retention_years: Retention period (uses default if not provided)
            enabled: Whether policy is enabled
            soft_delete: If True, mark as deleted instead of hard delete
        """
        policy = RetentionPolicy(
            table_name=table_name,
            date_column=date_column,
            retention_years=retention_years or self.retention_years,
            enabled=enabled,
            soft_delete=soft_delete
        )
        self.policies[table_name] = policy
    
    def apply_retention(
        self,
        table_name: str,
        audit_logger=None
    ) -> Dict[str, Any]:
        """
        Apply retention policy to a table
        
        Args:
            table_name: Table to apply retention to
            audit_logger: Optional audit logger instance
        
        Returns:
            Dictionary with results (rows_deleted, etc.)
        """
        if table_name not in self.policies:
            return {
                "status": "error",
                "message": f"No retention policy registered for {table_name}"
            }
        
        policy = self.policies[table_name]
        
        if not policy.enabled:
            return {
                "status": "skipped",
                "message": f"Retention policy disabled for {table_name}"
            }
        
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            return {
                "status": "error",
                "message": "Spark session required"
            }
        
        try:
            # Calculate cutoff date
            cutoff_date = datetime.utcnow() - timedelta(days=policy.retention_years * 365)
            cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")
            
            # Count rows to be deleted
            count_query = f"""
            SELECT COUNT(*) as count
            FROM {policy.table_name}
            WHERE {policy.date_column} < '{cutoff_date_str}'
            """
            count_result = self.spark.sql(count_query).collect()
            rows_to_delete = count_result[0]['count'] if count_result else 0
            
            if rows_to_delete == 0:
                return {
                    "status": "success",
                    "rows_deleted": 0,
                    "message": "No rows to delete"
                }
            
            # Apply deletion
            if policy.soft_delete:
                # Soft delete: add a deleted_at column if it doesn't exist
                try:
                    update_query = f"""
                    UPDATE {policy.table_name}
                    SET deleted_at = CURRENT_TIMESTAMP()
                    WHERE {policy.date_column} < '{cutoff_date_str}'
                      AND (deleted_at IS NULL OR deleted_at = '1970-01-01')
                    """
                    self.spark.sql(update_query)
                    rows_deleted = rows_to_delete
                except Exception as e:
                    # If update fails, try adding column first
                    try:
                        alter_query = f"""
                        ALTER TABLE {policy.table_name}
                        ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP
                        """
                        self.spark.sql(alter_query)
                        update_query = f"""
                        UPDATE {policy.table_name}
                        SET deleted_at = CURRENT_TIMESTAMP()
                        WHERE {policy.date_column} < '{cutoff_date_str}'
                          AND deleted_at IS NULL
                        """
                        self.spark.sql(update_query)
                        rows_deleted = rows_to_delete
                    except Exception:
                        # Fallback to hard delete if soft delete fails
                        delete_query = f"""
                        DELETE FROM {policy.table_name}
                        WHERE {policy.date_column} < '{cutoff_date_str}'
                        """
                        self.spark.sql(delete_query)
                        rows_deleted = rows_to_delete
            else:
                # Hard delete
                delete_query = f"""
                DELETE FROM {policy.table_name}
                WHERE {policy.date_column} < '{cutoff_date_str}'
                """
                self.spark.sql(delete_query)
                rows_deleted = rows_to_delete
            
            # Log to audit
            if audit_logger:
                audit_logger.log_retention_applied(
                    table_name=table_name,
                    rows_deleted=rows_deleted,
                    retention_years=policy.retention_years
                )
                audit_logger.flush()
            
            return {
                "status": "success",
                "rows_deleted": rows_deleted,
                "cutoff_date": cutoff_date_str,
                "retention_years": policy.retention_years
            }
        
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }
    
    def apply_all_policies(self, audit_logger=None) -> Dict[str, Dict[str, Any]]:
        """
        Apply retention policies to all registered tables
        
        Args:
            audit_logger: Optional audit logger instance
        
        Returns:
            Dictionary mapping table names to results
        """
        results = {}
        for table_name in self.policies.keys():
            results[table_name] = self.apply_retention(table_name, audit_logger)
        return results
    
    def get_policy_summary(self) -> str:
        """Get summary of all retention policies"""
        if not self.policies:
            return "No retention policies registered."
        
        summary = ["Retention Policies:"]
        summary.append("=" * 60)
        
        for table_name, policy in self.policies.items():
            status = "Enabled" if policy.enabled else "Disabled"
            delete_type = "Soft Delete" if policy.soft_delete else "Hard Delete"
            summary.append(
                f"\nTable: {table_name}\n"
                f"  Date Column: {policy.date_column}\n"
                f"  Retention: {policy.retention_years} years\n"
                f"  Status: {status}\n"
                f"  Delete Type: {delete_type}"
            )
        
        return "\n".join(summary)

