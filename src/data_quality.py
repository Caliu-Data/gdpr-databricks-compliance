"""
Data Quality Validation Framework
Validates data quality before processing to reject bad data early
"""
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from enum import Enum
import pandas as pd


class ValidationSeverity(Enum):
    """Severity levels for validation failures"""
    ERROR = "error"  # Reject data
    WARNING = "warning"  # Flag but allow
    INFO = "info"  # Informational only


@dataclass
class ValidationRule:
    """A data quality validation rule"""
    name: str
    description: str
    severity: ValidationSeverity
    check_function: Callable
    column: Optional[str] = None  # None for table-level checks


@dataclass
class ValidationResult:
    """Result of a validation check"""
    rule_name: str
    passed: bool
    severity: ValidationSeverity
    message: str
    affected_rows: int = 0
    details: Optional[Dict[str, Any]] = None


class DataQualityValidator:
    """Validates data quality according to configured rules"""
    
    def __init__(self, min_quality_score: float = 0.8):
        """
        Initialize validator
        
        Args:
            min_quality_score: Minimum quality score to accept (0-1)
        """
        self.min_quality_score = min_quality_score
        self.rules: List[ValidationRule] = []
    
    def add_rule(
        self,
        name: str,
        description: str,
        check_function: Callable,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
        column: Optional[str] = None
    ):
        """
        Add a validation rule
        
        Args:
            name: Rule name
            description: Rule description
            check_function: Function that returns (bool, message, affected_rows)
            severity: Severity level
            column: Column to check (None for table-level)
        """
        rule = ValidationRule(
            name=name,
            description=description,
            severity=severity,
            check_function=check_function,
            column=column
        )
        self.rules.append(rule)
    
    def validate_not_null(self, column: str, severity: ValidationSeverity = ValidationSeverity.ERROR):
        """Add a not-null validation rule"""
        def check(df):
            null_count = df[column].isna().sum()
            passed = null_count == 0
            message = f"Found {null_count} null values in {column}" if not passed else f"All values in {column} are non-null"
            return passed, message, null_count
        
        self.add_rule(
            name=f"{column}_not_null",
            description=f"Check that {column} contains no null values",
            check_function=check,
            severity=severity,
            column=column
        )
    
    def validate_uniqueness(self, column: str, severity: ValidationSeverity = ValidationSeverity.ERROR):
        """Add a uniqueness validation rule"""
        def check(df):
            duplicate_count = df[column].duplicated().sum()
            passed = duplicate_count == 0
            message = f"Found {duplicate_count} duplicate values in {column}" if not passed else f"All values in {column} are unique"
            return passed, message, duplicate_count
        
        self.add_rule(
            name=f"{column}_unique",
            description=f"Check that {column} contains unique values",
            check_function=check,
            severity=severity,
            column=column
        )
    
    def validate_range(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ):
        """Add a range validation rule"""
        def check(df):
            errors = []
            if min_value is not None:
                below_min = (df[column] < min_value).sum()
                if below_min > 0:
                    errors.append(f"{below_min} values below minimum {min_value}")
            if max_value is not None:
                above_max = (df[column] > max_value).sum()
                if above_max > 0:
                    errors.append(f"{above_max} values above maximum {max_value}")
            
            total_errors = sum([int(e.split()[0]) for e in errors])
            passed = len(errors) == 0
            message = "; ".join(errors) if errors else f"All values in {column} are within range"
            return passed, message, total_errors
        
        range_desc = []
        if min_value is not None:
            range_desc.append(f"min={min_value}")
        if max_value is not None:
            range_desc.append(f"max={max_value}")
        
        self.add_rule(
            name=f"{column}_range",
            description=f"Check that {column} values are within range [{', '.join(range_desc)}]",
            check_function=check,
            severity=severity,
            column=column
        )
    
    def validate_format(
        self,
        column: str,
        pattern: str,
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ):
        """Add a format validation rule (regex pattern)"""
        import re
        
        compiled_pattern = re.compile(pattern)
        
        def check(df):
            invalid_count = df[column].astype(str).apply(
                lambda x: x and not compiled_pattern.match(str(x))
            ).sum()
            passed = invalid_count == 0
            message = f"Found {invalid_count} values not matching pattern in {column}" if not passed else f"All values in {column} match pattern"
            return passed, message, invalid_count
        
        self.add_rule(
            name=f"{column}_format",
            description=f"Check that {column} values match pattern {pattern}",
            check_function=check,
            severity=severity,
            column=column
        )
    
    def validate_referential_integrity(
        self,
        column: str,
        reference_df: pd.DataFrame,
        reference_column: str,
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ):
        """Add a referential integrity validation rule"""
        def check(df):
            valid_values = set(reference_df[reference_column].dropna().unique())
            invalid_count = ~df[column].isin(valid_values).sum()
            passed = invalid_count == 0
            message = f"Found {invalid_count} values not in reference table" if not passed else "All values satisfy referential integrity"
            return passed, message, invalid_count
        
        self.add_rule(
            name=f"{column}_referential_integrity",
            description=f"Check that {column} values exist in reference table",
            check_function=check,
            severity=severity,
            column=column
        )
    
    def validate(self, df: pd.DataFrame, fail_on_error: bool = True) -> Dict[str, Any]:
        """
        Validate DataFrame against all rules
        
        Args:
            df: DataFrame to validate
            fail_on_error: If True, raise exception on error-level failures
        
        Returns:
            Dictionary with validation results
        """
        results = []
        error_count = 0
        warning_count = 0
        
        for rule in self.rules:
            try:
                if rule.column and rule.column not in df.columns:
                    result = ValidationResult(
                        rule_name=rule.name,
                        passed=False,
                        severity=ValidationSeverity.ERROR,
                        message=f"Column {rule.column} not found"
                    )
                    results.append(result)
                    error_count += 1
                    continue
                
                # Execute check
                if rule.column:
                    check_df = df[[rule.column]]
                else:
                    check_df = df
                
                passed, message, affected_rows = rule.check_function(check_df)
                
                result = ValidationResult(
                    rule_name=rule.name,
                    passed=passed,
                    severity=rule.severity,
                    message=message,
                    affected_rows=affected_rows
                )
                results.append(result)
                
                if not passed:
                    if rule.severity == ValidationSeverity.ERROR:
                        error_count += 1
                    elif rule.severity == ValidationSeverity.WARNING:
                        warning_count += 1
            
            except Exception as e:
                result = ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"Validation error: {str(e)}"
                )
                results.append(result)
                error_count += 1
        
        # Calculate quality score
        total_rules = len(self.rules)
        passed_rules = sum(1 for r in results if r.passed)
        quality_score = passed_rules / total_rules if total_rules > 0 else 0.0
        
        # Determine overall status
        overall_passed = quality_score >= self.min_quality_score and error_count == 0
        
        validation_result = {
            "overall_passed": overall_passed,
            "quality_score": quality_score,
            "total_rules": total_rules,
            "passed_rules": passed_rules,
            "error_count": error_count,
            "warning_count": warning_count,
            "results": [result.__dict__ for result in results],
            "min_quality_score": self.min_quality_score
        }
        
        if fail_on_error and not overall_passed and error_count > 0:
            error_messages = [
                f"{r.rule_name}: {r.message}"
                for r in results
                if not r.passed and r.severity == ValidationSeverity.ERROR
            ]
            raise ValueError(
                f"Data quality validation failed. Quality score: {quality_score:.2%}, "
                f"Errors: {error_count}\n" + "\n".join(error_messages)
            )
        
        return validation_result
    
    def get_validation_summary(self, validation_result: Dict[str, Any]) -> str:
        """Get human-readable validation summary"""
        lines = ["Data Quality Validation Summary"]
        lines.append("=" * 60)
        lines.append(f"Overall Status: {'PASSED' if validation_result['overall_passed'] else 'FAILED'}")
        lines.append(f"Quality Score: {validation_result['quality_score']:.2%}")
        lines.append(f"Rules Passed: {validation_result['passed_rules']}/{validation_result['total_rules']}")
        lines.append(f"Errors: {validation_result['error_count']}")
        lines.append(f"Warnings: {validation_result['warning_count']}")
        lines.append("\nDetailed Results:")
        
        for result in validation_result['results']:
            status = "✓" if result['passed'] else "✗"
            severity = result['severity'].upper()
            lines.append(
                f"  {status} [{severity}] {result['rule_name']}: {result['message']}"
            )
        
        return "\n".join(lines)

