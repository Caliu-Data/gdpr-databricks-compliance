"""
Automatic Lineage Tracking
Tracks data flow from source to destination with complete provenance
"""
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import json


class OperationType(Enum):
    """Types of data operations"""
    READ = "read"
    WRITE = "write"
    TRANSFORM = "transform"
    COPY = "copy"
    JOIN = "join"
    FILTER = "filter"
    AGGREGATE = "aggregate"


@dataclass
class DataLineage:
    """Represents a data lineage entry"""
    source_table: str
    target_table: str
    operation_type: OperationType
    operation_details: Dict[str, Any]
    timestamp: datetime
    user: str
    job_id: Optional[str] = None
    notebook_path: Optional[str] = None
    lineage_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage"""
        data = asdict(self)
        data['operation_type'] = self.operation_type.value
        data['timestamp'] = self.timestamp.isoformat()
        return data


class LineageTracker:
    """Tracks data lineage automatically"""
    
    def __init__(
        self,
        catalog: str,
        schema: str,
        table_name: str = "data_lineage",
        spark_session=None
    ):
        """
        Initialize lineage tracker
        
        Args:
            catalog: Databricks catalog name
            schema: Schema name
            table_name: Name of lineage tracking table
            spark_session: Spark session (optional)
        """
        self.catalog = catalog
        self.schema = schema
        self.table_name = table_name
        self.full_table_name = f"{catalog}.{schema}.{table_name}"
        self.spark = spark_session
        self.lineage_history: List[DataLineage] = []
    
    def _initialize_table(self):
        """Initialize the lineage tracking table if it doesn't exist"""
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            raise ValueError("Spark session required for table initialization")
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.full_table_name} (
            lineage_id STRING,
            source_table STRING,
            target_table STRING,
            operation_type STRING,
            operation_details STRING,
            timestamp TIMESTAMP,
            user STRING,
            job_id STRING,
            notebook_path STRING
        ) USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        
        self.spark.sql(create_table_sql)
    
    def track_read(
        self,
        source_table: str,
        user: str,
        job_id: Optional[str] = None,
        notebook_path: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Track a read operation
        
        Args:
            source_table: Source table name
            user: User performing the operation
            job_id: Optional job ID
            notebook_path: Optional notebook path
            filters: Optional filter conditions applied
        
        Returns:
            Lineage ID
        """
        import uuid
        lineage_id = str(uuid.uuid4())
        
        lineage = DataLineage(
            lineage_id=lineage_id,
            source_table=source_table,
            target_table="",  # No target for read operations
            operation_type=OperationType.READ,
            operation_details={"filters": filters or {}},
            timestamp=datetime.utcnow(),
            user=user,
            job_id=job_id,
            notebook_path=notebook_path
        )
        
        self.lineage_history.append(lineage)
        return lineage_id
    
    def track_write(
        self,
        source_table: str,
        target_table: str,
        user: str,
        operation_details: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None,
        notebook_path: Optional[str] = None
    ) -> str:
        """
        Track a write operation
        
        Args:
            source_table: Source table name
            target_table: Target table name
            user: User performing the operation
            operation_details: Details about the operation
            job_id: Optional job ID
            notebook_path: Optional notebook path
        
        Returns:
            Lineage ID
        """
        import uuid
        lineage_id = str(uuid.uuid4())
        
        lineage = DataLineage(
            lineage_id=lineage_id,
            source_table=source_table,
            target_table=target_table,
            operation_type=OperationType.WRITE,
            operation_details=operation_details or {},
            timestamp=datetime.utcnow(),
            user=user,
            job_id=job_id,
            notebook_path=notebook_path
        )
        
        self.lineage_history.append(lineage)
        return lineage_id
    
    def track_transform(
        self,
        source_table: str,
        target_table: str,
        user: str,
        transformation: str,
        job_id: Optional[str] = None,
        notebook_path: Optional[str] = None,
        additional_details: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Track a transformation operation
        
        Args:
            source_table: Source table name
            target_table: Target table name
            user: User performing the operation
            transformation: Description of transformation
            job_id: Optional job ID
            notebook_path: Optional notebook path
            additional_details: Additional operation details
        
        Returns:
            Lineage ID
        """
        import uuid
        lineage_id = str(uuid.uuid4())
        
        details = {
            "transformation": transformation,
            **(additional_details or {})
        }
        
        lineage = DataLineage(
            lineage_id=lineage_id,
            source_table=source_table,
            target_table=target_table,
            operation_type=OperationType.TRANSFORM,
            operation_details=details,
            timestamp=datetime.utcnow(),
            user=user,
            job_id=job_id,
            notebook_path=notebook_path
        )
        
        self.lineage_history.append(lineage)
        return lineage_id
    
    def flush(self):
        """Flush lineage history to the tracking table"""
        if not self.lineage_history:
            return
        
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            raise ValueError("Spark session required to flush lineage")
        
        # Initialize table if needed
        try:
            self._initialize_table()
        except Exception:
            pass  # Table might already exist
        
        # Convert lineage entries to DataFrame
        import pandas as pd
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        
        records = []
        for lineage in self.lineage_history:
            record = lineage.to_dict()
            record['operation_details'] = json.dumps(record['operation_details'])
            records.append(record)
        
        if records:
            df = pd.DataFrame(records)
            spark_df = self.spark.createDataFrame(df)
            
            # Write to Delta table
            spark_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(self.full_table_name)
        
        # Clear history
        self.lineage_history = []
    
    def get_lineage_graph(self, table_name: str, direction: str = "downstream") -> List[Dict]:
        """
        Get lineage graph for a table
        
        Args:
            table_name: Table to get lineage for
            direction: "downstream" (where data goes) or "upstream" (where data comes from)
        
        Returns:
            List of lineage entries
        """
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            return []
        
        try:
            if direction == "downstream":
                query = f"""
                SELECT * FROM {self.full_table_name}
                WHERE source_table = '{table_name}'
                ORDER BY timestamp DESC
                """
            else:
                query = f"""
                SELECT * FROM {self.full_table_name}
                WHERE target_table = '{table_name}'
                ORDER BY timestamp DESC
                """
            
            df = self.spark.sql(query)
            return df.collect()
        except Exception:
            return []
    
    def get_complete_lineage(self, table_name: str) -> Dict[str, List]:
        """
        Get complete lineage (both upstream and downstream) for a table
        
        Args:
            table_name: Table to get lineage for
        
        Returns:
            Dictionary with 'upstream' and 'downstream' lineage
        """
        return {
            "upstream": self.get_lineage_graph(table_name, "upstream"),
            "downstream": self.get_lineage_graph(table_name, "downstream")
        }

