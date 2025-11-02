"""
Automatic PII Detection Module
Detects emails, phone numbers, SSNs, and other sensitive data
"""
import re
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from enum import Enum


class PIIType(Enum):
    """Types of Personally Identifiable Information"""
    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    IP_ADDRESS = "ip_address"
    PASSPORT = "passport"
    DRIVER_LICENSE = "driver_license"
    BANK_ACCOUNT = "bank_account"
    DATE_OF_BIRTH = "date_of_birth"
    NAME = "name"


@dataclass
class PIIDetection:
    """Result of PII detection"""
    pii_type: PIIType
    column_name: str
    row_count: int
    sample_values: List[str]
    confidence: float


class PIIDetector:
    """Detects PII in data columns"""
    
    # Regex patterns for PII detection
    PATTERNS = {
        PIIType.EMAIL: re.compile(
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            re.IGNORECASE
        ),
        PIIType.PHONE: re.compile(
            r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
            r'|\+\d{1,3}\s?\d{9,14}'
        ),
        PIIType.SSN: re.compile(
            r'\b\d{3}-\d{2}-\d{4}\b|\b\d{9}\b'
        ),
        PIIType.CREDIT_CARD: re.compile(
            r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b'
        ),
        PIIType.IP_ADDRESS: re.compile(
            r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
        ),
        PIIType.DATE_OF_BIRTH: re.compile(
            r'\b(0?[1-9]|1[0-2])[/-](0?[1-9]|[12]\d|3[01])[/-](\d{4}|\d{2})\b'
            r'|\b(19|20)\d{2}[/-](0?[1-9]|1[0-2])[/-](0?[1-9]|[12]\d|3[01])\b'
        ),
    }
    
    # Common name patterns (basic detection)
    COMMON_NAMES = {
        "john", "jane", "smith", "johnson", "williams", "brown", "jones",
        "garcia", "miller", "davis", "rodriguez", "martinez", "hernandez"
    }
    
    def __init__(self, min_confidence: float = 0.7):
        """
        Initialize PII detector
        
        Args:
            min_confidence: Minimum confidence threshold for PII detection (0-1)
        """
        self.min_confidence = min_confidence
    
    def detect_pii_in_column(
        self,
        column_name: str,
        sample_values: List[str],
        full_data_size: Optional[int] = None
    ) -> List[PIIDetection]:
        """
        Detect PII in a column based on sample values
        
        Args:
            column_name: Name of the column
            sample_values: Sample values from the column
            full_data_size: Total number of rows (for accurate counts)
        
        Returns:
            List of PII detections found
        """
        detections = []
        sample_size = len(sample_values)
        
        if sample_size == 0:
            return detections
        
        # Check each PII type
        for pii_type, pattern in self.PATTERNS.items():
            matches = []
            for value in sample_values:
                if value and isinstance(value, str):
                    if pattern.search(str(value)):
                        matches.append(value)
            
            if matches:
                match_ratio = len(matches) / sample_size
                # Higher match ratio = higher confidence
                confidence = min(match_ratio * 1.2, 1.0)
                
                if confidence >= self.min_confidence:
                    row_count = full_data_size if full_data_size else sample_size
                    detections.append(PIIDetection(
                        pii_type=pii_type,
                        column_name=column_name,
                        row_count=row_count,
                        sample_values=matches[:5],  # Keep only 5 samples
                        confidence=confidence
                    ))
        
        # Check for names (less precise)
        name_matches = self._detect_names(column_name, sample_values)
        if name_matches:
            detections.extend(name_matches)
        
        return detections
    
    def _detect_names(
        self,
        column_name: str,
        sample_values: List[str]
    ) -> List[PIIDetection]:
        """Detect potential name columns"""
        detections = []
        
        # Check if column name suggests it's a name field
        name_indicators = ['name', 'firstname', 'lastname', 'fullname', 'customer_name']
        if any(indicator in column_name.lower() for indicator in name_indicators):
            name_count = 0
            for value in sample_values:
                if value and isinstance(value, str):
                    words = value.lower().split()
                    # Check if contains common name patterns
                    if any(word in self.COMMON_NAMES for word in words):
                        name_count += 1
            
            if name_count > 0:
                confidence = min(name_count / len(sample_values) * 1.5, 0.9)
                if confidence >= self.min_confidence:
                    detections.append(PIIDetection(
                        pii_type=PIIType.NAME,
                        column_name=column_name,
                        row_count=len(sample_values),
                        sample_values=[v for v in sample_values[:5] if v],
                        confidence=confidence
                    ))
        
        return detections
    
    def scan_dataframe(self, df, sample_size: int = 1000) -> Dict[str, List[PIIDetection]]:
        """
        Scan a pandas DataFrame for PII
        
        Args:
            df: pandas DataFrame to scan
            sample_size: Number of rows to sample for detection
        
        Returns:
            Dictionary mapping column names to their PII detections
        """
        results = {}
        
        # Sample data if too large
        if len(df) > sample_size:
            sample_df = df.sample(min(sample_size, len(df)))
        else:
            sample_df = df
        
        for column in df.columns:
            sample_values = sample_df[column].dropna().astype(str).tolist()
            detections = self.detect_pii_in_column(
                column_name=column,
                sample_values=sample_values,
                full_data_size=len(df)
            )
            
            if detections:
                results[column] = detections
        
        return results
    
    def get_pii_summary(self, detections: Dict[str, List[PIIDetection]]) -> str:
        """
        Generate a human-readable summary of PII detections
        
        Args:
            detections: Dictionary of detections from scan_dataframe
        
        Returns:
            Formatted summary string
        """
        if not detections:
            return "No PII detected."
        
        summary = ["PII Detection Summary:"]
        summary.append("=" * 50)
        
        for column, pii_list in detections.items():
            summary.append(f"\nColumn: {column}")
            for detection in pii_list:
                summary.append(
                    f"  - {detection.pii_type.value.upper()}: "
                    f"{detection.row_count} occurrences "
                    f"(confidence: {detection.confidence:.2%})"
                )
        
        return "\n".join(summary)

