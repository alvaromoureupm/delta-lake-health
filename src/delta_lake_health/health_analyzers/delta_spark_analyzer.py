from typing import Optional
from delta_lake_health.health_analyzers.base_analyzer import BaseAnalyzer, DeltaAnalyzerMetrics
from delta.tables import DeltaTable as SparkDeltaTable


class DeltaSparkAnalyzer(BaseAnalyzer):
    """
    Analyzer for Delta Lake tables using Spark (for Databricks environments).
    """
    data: Optional[SparkDeltaTable]

    def __init__(self, spark) -> None:
        super().__init__()
        self.spark = spark
        if self.spark is None:
            raise ValueError("Spark session must be provided for Spark environment.")

    def load_data(self, table_name: Optional[str] = None, table_path: Optional[str] = None) -> SparkDeltaTable:
        if table_path is not None:
            self.data = SparkDeltaTable.forPath(self.spark, table_path)
        elif table_name is not None:
            self.data = SparkDeltaTable.forName(self.spark, table_name)
        else:
            raise ValueError("Either table_name or table_path must be provided.")
        return self.data

    def _load_data(self, data: str) -> None:
        """Load data from the provided table path or name."""
        # For Spark, data could be a table name or path
        if data.startswith("/") or data.startswith("s3://") or data.startswith("abfss://"):
            # Treat as path
            self.load_data(table_path=data)
        else:
            # Treat as table name
            self.load_data(table_name=data)

    def analyze_skewness(self, table_name: str, partition_cols: list, threshold: float, metrics: DeltaAnalyzerMetrics):
        partition_cols_str = ", ".join(partition_cols)
        query = f"""
        SELECT {partition_cols_str}, COUNT(*) as record_count
        FROM {table_name}
        GROUP BY {partition_cols_str}
        """
        
        partition_counts = self.spark.sql(query).collect()
        
        counts = [row['record_count'] for row in partition_counts]
        
        records_per_partition_dict = {}
        for row in partition_counts:
            if len(partition_cols) == 1:
                key = str(row[partition_cols[0]])
            else:
                key = str(tuple(row[col] for col in partition_cols))
            records_per_partition_dict[key] = row['record_count']
        
        if not counts:
            skewness_max = 0.0
            skewness_average = 0.0
        else:
            max_count = max(counts)
            min_count = min(counts)
            mean_count = sum(counts) / len(counts)
            
            skewness_max = (max_count - min_count) / max_count if max_count > 0 else 0.0
            
            avg_abs_dev = sum(abs(c - mean_count) for c in counts) / len(counts)
            skewness_average = avg_abs_dev / mean_count if mean_count > 0 else 0.0
        
        metrics.skewness_max = skewness_max
        metrics.skewness_average = skewness_average
        metrics.is_skewed = skewness_max > threshold
        metrics.partition_skewness = skewness_max
        
        metrics.skew_metrics = {
            'partition_columns': partition_cols,
            'skewness_max': skewness_max,
            'skewness_average': skewness_average,
            'is_skewed': skewness_max > threshold,
            'records_per_partition_dict': records_per_partition_dict
        }

    def analyze(self, data: str) -> DeltaAnalyzerMetrics:
        """
        Analyze the Delta Lake table and return metrics.
        For backward compatibility, this method accepts data as table_name or table_path.
        """
        if data.startswith("/") or data.startswith("s3://") or data.startswith("abfss://"):
            return self.analyze_table(table_path=data)
        else:
            return self.analyze_table(table_name=data)

    def analyze_table(
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
        self.load_data(table_name, table_path)
        
        if not isinstance(self.data, SparkDeltaTable):
            raise TypeError("For Spark environment, self.data must be a SparkDeltaTable instance")
        
        if table_name:
            full_table_name = table_name
        else:
            full_table_name = f"delta.`{table_path}`"
        
        table_details = self.spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0]
        
        history = self.spark.sql(f"DESCRIBE HISTORY {full_table_name}").collect()
        
        metrics = DeltaAnalyzerMetrics()
        
        metrics.table_size_bytes = table_details.sizeInBytes
        metrics.data_file_count = table_details.numFiles
        metrics.total_file_count = metrics.data_file_count
        
        ops_count = {"WRITE": 0, "DELETE": 0, "OPTIMIZE": 0, "MERGE": 0}
        
        for entry in history:
            op = entry.operation
            if op in ops_count:
                ops_count[op] += 1
        
        metrics.number_of_writes = ops_count["WRITE"]
        metrics.number_of_deletes = ops_count["DELETE"]
        metrics.number_of_optimizes = ops_count["OPTIMIZE"]
        metrics.version_count = max([entry.version for entry in history]) if history else 0
        
        metrics.folder_size_bytes = metrics.table_size_bytes
        
        partition_cols = table_details.partitionColumns
        if partition_cols:
            try:
                self.analyze_skewness(full_table_name, partition_cols, skew_threshold, metrics)
            except Exception as e:
                metrics.skew_metrics = {
                    'partition_columns': partition_cols,
                    'skewness_max': 0.0,
                    'skewness_average': 0.0,
                    'is_skewed': False,
                    'error': f'Failed to calculate skewness: {str(e)}'
                }
                metrics.skewness_max = 0.0
                metrics.skewness_average = 0.0
                metrics.is_skewed = False
        
        try:
            metrics.record_count = self.data.toDF().count()
        except Exception:
            metrics.record_count = 0
            
        if metrics.number_of_writes > 10 * metrics.number_of_optimizes and metrics.number_of_writes > 0:
            metrics.needs_vacuum = True
            metrics.files_needing_vacuum = int(metrics.data_file_count * 0.1)
        else:
            metrics.needs_vacuum = False
            metrics.files_needing_vacuum = 0
        
        if metrics.data_file_count > 0 and metrics.table_size_bytes > 0:
            avg_file_size = metrics.table_size_bytes / metrics.data_file_count
            metrics.avg_file_size_bytes = int(avg_file_size)
            
            small_file_threshold = small_file_size_mb * 1024 * 1024
            if avg_file_size < small_file_threshold:
                metrics.small_files_count = int(metrics.data_file_count * 0.8)
                metrics.needs_optimize = True
            elif avg_file_size < small_file_threshold * 2:
                metrics.small_files_count = int(metrics.data_file_count * 0.3)
                metrics.needs_optimize = metrics.small_files_count > metrics.data_file_count * small_file_ratio_threshold
            else:
                metrics.small_files_count = int(metrics.data_file_count * 0.1)
                metrics.needs_optimize = False
        
        metrics.has_orphan_files = metrics.needs_vacuum
        metrics.orphan_files_count = metrics.files_needing_vacuum
        
        metrics.file_size_efficiency = min(1.0, metrics.avg_file_size_bytes / (128 * 1024 * 1024)) if metrics.avg_file_size_bytes else 0
        metrics.storage_efficiency = 0.95
        
        metrics.table_path = table_details.location
        
        return metrics
