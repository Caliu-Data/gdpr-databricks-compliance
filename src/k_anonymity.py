"""
K-Anonymity Protection for Analytics
Prevents re-identification of individuals in datasets
"""
from typing import List, Dict, Set, Optional, Tuple
from collections import Counter
from dataclasses import dataclass
import math


@dataclass
class KAnonymityResult:
    """Result of k-anonymity check"""
    k_value: int
    is_compliant: bool
    vulnerable_combinations: List[Dict[str, Any]]
    recommendations: List[str]


class KAnonymityChecker:
    """Checks and enforces k-anonymity on datasets"""
    
    def __init__(self, k_threshold: int = 5):
        """
        Initialize k-anonymity checker
        
        Args:
            k_threshold: Minimum k value required (default: 5)
        """
        self.k_threshold = k_threshold
    
    def check_k_anonymity(
        self,
        df,
        quasi_identifiers: List[str],
        sensitive_attributes: Optional[List[str]] = None
    ) -> KAnonymityResult:
        """
        Check k-anonymity of a dataset
        
        Args:
            df: pandas DataFrame to check
            quasi_identifiers: Columns that can be used to identify individuals
            sensitive_attributes: Columns containing sensitive information
        
        Returns:
            KAnonymityResult with compliance status
        """
        import pandas as pd
        
        if not isinstance(df, pd.DataFrame):
            raise ValueError("DataFrame must be a pandas DataFrame")
        
        # Check if all quasi-identifiers exist
        missing_cols = [col for col in quasi_identifiers if col not in df.columns]
        if missing_cols:
            return KAnonymityResult(
                k_value=0,
                is_compliant=False,
                vulnerable_combinations=[],
                recommendations=[f"Missing columns: {missing_cols}"]
            )
        
        # Count combinations of quasi-identifiers
        combination_counts = df[quasi_identifiers].value_counts()
        
        # Find minimum k (smallest group size)
        min_k = int(combination_counts.min()) if len(combination_counts) > 0 else 0
        
        # Find vulnerable combinations (groups smaller than threshold)
        vulnerable = combination_counts[combination_counts < self.k_threshold]
        vulnerable_combinations = []
        
        if len(vulnerable) > 0:
            for combo, count in vulnerable.items():
                combo_dict = dict(zip(quasi_identifiers, combo))
                combo_dict['count'] = int(count)
                vulnerable_combinations.append(combo_dict)
        
        # Generate recommendations
        recommendations = []
        if min_k < self.k_threshold:
            recommendations.append(
                f"Dataset has k={min_k}, but threshold is {self.k_threshold}"
            )
            recommendations.append(
                f"Found {len(vulnerable_combinations)} vulnerable combinations"
            )
            recommendations.append(
                "Consider: Generalization, Suppression, or Sampling"
            )
        
        return KAnonymityResult(
            k_value=min_k,
            is_compliant=min_k >= self.k_threshold,
            vulnerable_combinations=vulnerable_combinations,
            recommendations=recommendations
        )
    
    def generalize(
        self,
        df,
        column: str,
        generalization_level: str = "medium"
    ):
        """
        Generalize a column to improve k-anonymity
        
        Args:
            df: pandas DataFrame
            column: Column to generalize
            generalization_level: "low", "medium", or "high"
        
        Returns:
            DataFrame with generalized column
        """
        import pandas as pd
        
        if column not in df.columns:
            raise ValueError(f"Column {column} not found")
        
        df_copy = df.copy()
        series = df_copy[column]
        
        if pd.api.types.is_numeric_dtype(series):
            # Generalize numeric values
            if generalization_level == "low":
                df_copy[column] = (series // 10) * 10
            elif generalization_level == "medium":
                df_copy[column] = (series // 100) * 100
            else:  # high
                df_copy[column] = (series // 1000) * 1000
        elif pd.api.types.is_datetime64_any_dtype(series):
            # Generalize dates
            if generalization_level == "low":
                df_copy[column] = pd.to_datetime(series).dt.to_period('M')
            elif generalization_level == "medium":
                df_copy[column] = pd.to_datetime(series).dt.to_period('Y')
            else:  # high
                df_copy[column] = pd.to_datetime(series).dt.to_period('Y')
        else:
            # For string columns, truncate or generalize
            if generalization_level == "low":
                df_copy[column] = series.astype(str).str[:5]
            elif generalization_level == "medium":
                df_copy[column] = series.astype(str).str[:3]
            else:  # high
                df_copy[column] = "***"
        
        return df_copy
    
    def suppress_rows(
        self,
        df,
        quasi_identifiers: List[str],
        min_group_size: int = None
    ):
        """
        Suppress rows that are in groups smaller than min_group_size
        
        Args:
            df: pandas DataFrame
            quasi_identifiers: Columns used for grouping
            min_group_size: Minimum group size (uses k_threshold if not provided)
        
        Returns:
            DataFrame with suppressed rows removed
        """
        import pandas as pd
        
        min_group_size = min_group_size or self.k_threshold
        
        # Count combinations
        combination_counts = df[quasi_identifiers].value_counts()
        
        # Filter to keep only combinations with sufficient size
        valid_combinations = combination_counts[
            combination_counts >= min_group_size
        ].index
        
        # Create mask for valid rows
        if len(valid_combinations) == 0:
            return pd.DataFrame(columns=df.columns)
        
        mask = pd.Series([False] * len(df))
        for combo in valid_combinations:
            if isinstance(combo, tuple):
                combo_dict = dict(zip(quasi_identifiers, combo))
            else:
                combo_dict = {quasi_identifiers[0]: combo}
            
            row_mask = pd.Series([True] * len(df))
            for col, val in combo_dict.items():
                row_mask = row_mask & (df[col] == val)
            mask = mask | row_mask
        
        return df[mask].copy()
    
    def ensure_k_anonymity(
        self,
        df,
        quasi_identifiers: List[str],
        generalization_strategy: Optional[Dict[str, str]] = None,
        apply_suppression: bool = True
    ):
        """
        Ensure dataset meets k-anonymity requirement
        
        Args:
            df: pandas DataFrame
            quasi_identifiers: Columns that can identify individuals
            generalization_strategy: Dict mapping columns to generalization levels
            apply_suppression: Whether to suppress small groups
        
        Returns:
            DataFrame that meets k-anonymity requirement
        """
        import pandas as pd
        
        result_df = df.copy()
        
        # Apply generalization if strategy provided
        if generalization_strategy:
            for column, level in generalization_strategy.items():
                if column in result_df.columns:
                    result_df = self.generalize(result_df, column, level)
        
        # Check k-anonymity
        check_result = self.check_k_anonymity(result_df, quasi_identifiers)
        
        # Apply suppression if needed and enabled
        if not check_result.is_compliant and apply_suppression:
            result_df = self.suppress_rows(result_df, quasi_identifiers)
            # Re-check after suppression
            check_result = self.check_k_anonymity(result_df, quasi_identifiers)
        
        return result_df, check_result
    
    def create_aggregate_only_view(
        self,
        df,
        group_by_columns: List[str],
        aggregate_columns: Dict[str, str],
        min_group_size: int = None
    ):
        """
        Create a view with only aggregations (no raw PII)
        
        Args:
            df: pandas DataFrame
            group_by_columns: Columns to group by
            aggregate_columns: Dict of {column: aggregation_function}
            min_group_size: Minimum group size (filters out small groups)
        
        Returns:
            Aggregated DataFrame
        """
        import pandas as pd
        
        min_group_size = min_group_size or self.k_threshold
        
        # Group and aggregate
        grouped = df.groupby(group_by_columns)
        
        # Apply aggregations
        agg_dict = {}
        for col, agg_func in aggregate_columns.items():
            if col in df.columns:
                if agg_func == "count":
                    agg_dict[col] = "count"
                elif agg_func == "sum":
                    agg_dict[col] = "sum"
                elif agg_func == "mean":
                    agg_dict[col] = "mean"
                elif agg_func == "min":
                    agg_dict[col] = "min"
                elif agg_func == "max":
                    agg_dict[col] = "max"
                else:
                    agg_dict[col] = agg_func
        
        if not agg_dict:
            aggregated = grouped.size().reset_index(name='count')
        else:
            aggregated = grouped.agg(agg_dict).reset_index()
            # Add count column
            aggregated['_group_count'] = grouped.size().values
        
        # Filter out small groups
        if min_group_size:
            aggregated = aggregated[aggregated['_group_count'] >= min_group_size]
            aggregated = aggregated.drop(columns=['_group_count'])
        
        return aggregated

