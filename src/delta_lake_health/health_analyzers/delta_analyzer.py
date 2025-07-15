from typing import Optional
from delta_lake_health.health_analyzers.base_analyzer import HealthResult, DeltaAnalyzerMetrics, HealthStatus
from delta_lake_health.health_analyzers.delta_python_analyzer import DeltaPythonAnalyzer
from delta_lake_health.health_analyzers.delta_spark_analyzer import DeltaSparkAnalyzer
from enum import Enum


class Environment(str, Enum):
    PYTHON = "python"
    DATABRICKS = "databricks"


class DeltaAnalyzer:
    """
    Factory class for Delta Lake table analyzers.
    
    This class provides backward compatibility and acts as a factory to create
    appropriate analyzer instances based on the environment.
    
    Supported environments:
    - Environment.PYTHON: Uses deltalake Python library (no Spark required)
    - Environment.DATABRICKS: Uses Databricks Spark (requires Spark session)
    """
    
    def __init__(self, environment: str = "python", spark=None) -> None:
        """
        Initialize the Delta Lake analyzer factory.
        
        Args:
            environment: The execution environment - either "python" for local analysis 
                        using deltalake library, or "databricks" for Spark-based analysis
            spark: Spark session instance, required when environment is "databricks"
        """
        if isinstance(environment, str):
            self.environment = Environment(environment.lower())
        else:
            self.environment = environment
        self.spark = spark

    def create_health_result(self, metrics: DeltaAnalyzerMetrics) -> HealthResult:
        """
        Create a HealthResult from DeltaAnalyzerMetrics.
        """
        metrics.calculate_health_score()  # Modifies metrics in place
        return HealthResult(
            health_metric=int(metrics.health_score or 0), 
            health_status=metrics.health_status or HealthStatus.UNKNOWN, 
            metrics=metrics
        )

    def analyze(
        self,
        table_name: Optional[str] = None,
        table_path: Optional[str] = None,
        skew_threshold: float = 0.1,
        vacuum_size_ratio_threshold: float = 0.7,
        orphan_file_ratio_threshold: float = 0.8,
        small_file_size_mb: float = 10.0,
        small_file_ratio_threshold: float = 0.3
    ) -> DeltaAnalyzerMetrics:
        """
        Analyze the Delta Lake table and return health metrics.
        """
        if self.environment == Environment.PYTHON:
            if table_path is None:
                raise ValueError("table_path must be provided for Python environment")
            analyzer = DeltaPythonAnalyzer()
            metrics = analyzer.analyze_table(
                table_path=table_path,
                skew_threshold=skew_threshold,
                vacuum_size_ratio_threshold=vacuum_size_ratio_threshold,
                orphan_file_ratio_threshold=orphan_file_ratio_threshold,
                small_file_size_mb=small_file_size_mb,
                small_file_ratio_threshold=small_file_ratio_threshold
            )
        elif self.environment == Environment.DATABRICKS:
            if self.spark is None:
                raise ValueError("Spark session must be provided for Databricks environment")
            analyzer = DeltaSparkAnalyzer(self.spark)
            metrics = analyzer.analyze_table(
                table_name=table_name,
                table_path=table_path,
                skew_threshold=skew_threshold,
                vacuum_size_ratio_threshold=vacuum_size_ratio_threshold,
                orphan_file_ratio_threshold=orphan_file_ratio_threshold,
                small_file_size_mb=small_file_size_mb,
                small_file_ratio_threshold=small_file_ratio_threshold
            )
        else:
            raise ValueError(f"Unsupported environment: {self.environment}")
        
        metrics.calculate_health_score()
        return metrics

    def analyze_with_health_result(
        self,
        table_name: Optional[str] = None,
        table_path: Optional[str] = None,
        skew_threshold: float = 0.1,
        vacuum_size_ratio_threshold: float = 0.7,
        orphan_file_ratio_threshold: float = 0.8,
        small_file_size_mb: float = 10.0,
        small_file_ratio_threshold: float = 0.3
    ) -> HealthResult:
        """
        Analyze the Delta Lake table and return HealthResult for backward compatibility.
        For visualization and detailed analysis, use analyze() method instead.
        """
        metrics = self.analyze(
            table_name=table_name,
            table_path=table_path,
            skew_threshold=skew_threshold,
            vacuum_size_ratio_threshold=vacuum_size_ratio_threshold,
            orphan_file_ratio_threshold=orphan_file_ratio_threshold,
            small_file_size_mb=small_file_size_mb,
            small_file_ratio_threshold=small_file_ratio_threshold
        )
        return self.create_health_result(metrics)


# Export the specific analyzer classes for direct use
__all__ = ['DeltaAnalyzer', 'DeltaPythonAnalyzer', 'DeltaSparkAnalyzer', 'Environment']
