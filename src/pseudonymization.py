"""
Reversible Pseudonymization
Securely pseudonymize PII while maintaining ability to reverse
"""
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
from cryptography.hazmat.backends import default_backend
import base64
import hashlib
from typing import Dict, Optional, List, Any
import json


class Pseudonymizer:
    """Reversible pseudonymization using encryption"""
    
    def __init__(self, key: str):
        """
        Initialize pseudonymizer
        
        Args:
            key: Base64-encoded encryption key or hex string
                (will be converted to proper format)
        """
        self.key = self._prepare_key(key)
        self.cipher = Fernet(self.key)
        self.mapping_cache: Dict[str, str] = {}
    
    def _prepare_key(self, key: str) -> bytes:
        """
        Prepare encryption key from various formats
        
        Args:
            key: Key in hex, base64, or raw format
        
        Returns:
            Base64-encoded key suitable for Fernet
        """
        # If already base64, try to decode to check
        try:
            decoded = base64.urlsafe_b64decode(key)
            if len(decoded) == 32:
                # Already in correct format
                return key.encode() if isinstance(key, str) else key
        except Exception:
            pass
        
        # If hex string, convert to bytes then base64
        try:
            if len(key) == 64:  # 32 bytes in hex
                key_bytes = bytes.fromhex(key)
                return base64.urlsafe_b64encode(key_bytes)
        except Exception:
            pass
        
        # If it's a raw string, derive key using PBKDF2
        if isinstance(key, str):
            kdf = PBKDF2(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b'gdpr_compliance_salt',  # Fixed salt for consistency
                iterations=100000,
                backend=default_backend()
            )
            key_bytes = kdf.derive(key.encode())
            return base64.urlsafe_b64encode(key_bytes)
        
        raise ValueError("Invalid key format")
    
    def pseudonymize(self, value: str, deterministic: bool = True) -> str:
        """
        Pseudonymize a value
        
        Args:
            value: Value to pseudonymize
            deterministic: If True, same input always produces same output
        
        Returns:
            Pseudonymized value
        """
        if not value or not isinstance(value, str):
            return value
        
        # Check cache for deterministic pseudonymization
        if deterministic and value in self.mapping_cache:
            return self.mapping_cache[value]
        
        # Encrypt the value
        encrypted = self.cipher.encrypt(value.encode())
        
        # For deterministic pseudonymization, use hash of value as seed
        if deterministic:
            # Use first 16 bytes of hash for consistent mapping
            hash_value = hashlib.sha256(value.encode()).digest()[:16]
            # Create deterministic cipher from hash
            kdf = PBKDF2(
                algorithm=hashes.SHA256(),
                length=32,
                salt=hash_value,
                iterations=1000,
                backend=default_backend()
            )
            det_key = base64.urlsafe_b64encode(kdf.derive(value.encode()))
            det_cipher = Fernet(det_key)
            pseudonymized = base64.urlsafe_b64encode(
                det_cipher.encrypt(value.encode())
            ).decode()
        else:
            pseudonymized = encrypted.decode()
        
        # Cache for deterministic mode
        if deterministic:
            self.mapping_cache[value] = pseudonymized
        
        return pseudonymized
    
    def depseudonymize(self, pseudonymized_value: str) -> str:
        """
        Reverse pseudonymization
        
        Args:
            pseudonymized_value: Pseudonymized value to reverse
        
        Returns:
            Original value
        """
        if not pseudonymized_value:
            return pseudonymized_value
        
        try:
            # Try to decrypt
            decrypted = self.cipher.decrypt(pseudonymized_value.encode())
            return decrypted.decode()
        except Exception:
            # If direct decryption fails, might be deterministic
            # Try brute force through cache (limited use case)
            for original, pseudonym in self.mapping_cache.items():
                if pseudonym == pseudonymized_value:
                    return original
            
            raise ValueError(f"Cannot depseudonymize value: {pseudonymized_value}")
    
    def pseudonymize_column(
        self,
        df,
        column_name: str,
        deterministic: bool = True,
        spark_session=None
    ):
        """
        Pseudonymize a column in a DataFrame
        
        Args:
            df: Spark DataFrame or pandas DataFrame
            column_name: Name of column to pseudonymize
            deterministic: If True, same input always produces same output
        
        Returns:
            DataFrame with pseudonymized column
        """
        if spark_session is None:
            from pyspark.sql import SparkSession
            spark_session = SparkSession.getActiveSession()
        
        # Check if it's a Spark DataFrame
        if hasattr(df, 'sparkSession') or spark_session:
            return self._pseudonymize_spark_column(df, column_name, deterministic, spark_session)
        else:
            # Assume pandas DataFrame
            return self._pseudonymize_pandas_column(df, column_name, deterministic)
    
    def _pseudonymize_pandas_column(self, df, column_name: str, deterministic: bool):
        """Pseudonymize column in pandas DataFrame"""
        import pandas as pd
        
        df_copy = df.copy()
        df_copy[column_name] = df_copy[column_name].apply(
            lambda x: self.pseudonymize(str(x), deterministic) if pd.notna(x) else x
        )
        return df_copy
    
    def _pseudonymize_spark_column(self, df, column_name: str, deterministic: bool, spark):
        """Pseudonymize column in Spark DataFrame"""
        from pyspark.sql.functions import udf, col
        from pyspark.sql.types import StringType
        
        # Create UDF for pseudonymization
        def pseudonymize_udf(value):
            if value is None:
                return None
            return self.pseudonymize(str(value), deterministic)
        
        pseudonymize_func = udf(pseudonymize_udf, StringType())
        
        return df.withColumn(column_name, pseudonymize_func(col(column_name)))
    
    def pseudonymize_multiple_columns(
        self,
        df,
        column_names: List[str],
        deterministic: bool = True,
        spark_session=None
    ):
        """
        Pseudonymize multiple columns
        
        Args:
            df: DataFrame
            column_names: List of column names to pseudonymize
            deterministic: If True, same input always produces same output
            spark_session: Spark session (for Spark DataFrames)
        
        Returns:
            DataFrame with pseudonymized columns
        """
        result_df = df
        for column_name in column_names:
            result_df = self.pseudonymize_column(
                result_df,
                column_name,
                deterministic,
                spark_session
            )
        return result_df
    
    def save_mapping(self, filepath: str):
        """Save pseudonymization mapping to file (for recovery)"""
        with open(filepath, 'w') as f:
            json.dump(self.mapping_cache, f)
    
    def load_mapping(self, filepath: str):
        """Load pseudonymization mapping from file"""
        with open(filepath, 'r') as f:
            self.mapping_cache = json.load(f)

