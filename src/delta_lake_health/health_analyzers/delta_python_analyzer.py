from typing import Optional
from delta_lake_health.health_analyzers.base_analyzer import BaseAnalyzer, DeltaAnalyzerMetrics, DeltaMetrics
from deltalake import DeltaTable as RustDeltaTable
import polars as pl
import os


class DeltaPythonAnalyzer(BaseAnalyzer):
    """
    Analyzer for Delta Lake tables using Python libraries (no Spark required).
    """
    data: Optional[RustDeltaTable]

    def __init__(self) -> None:
        super().__init__()

    def _load_data(self, table_path: str) -> RustDeltaTable:
        if table_path is not None:
            dt = RustDeltaTable(table_path)
            partition_cols = dt.metadata().partition_columns if hasattr(dt.metadata(), "partition_columns") else []
            if partition_cols:
                df = pl.read_delta(table_path)
                self.partition_df = df
                self.partition_cols = partition_cols
            else:
                self.partition_df = None
                self.partition_cols = []
            self.data = dt
            return dt
        else:
            raise ValueError("table_path must be provided for Python environment.")
        
    def analyze_skewness(self, threshold: float = 0.1, method: str = "max") -> float:
        """
        Analyze partition skewness using Polars from a Python DeltaTable.
        Returns a normalized skewness value between 0 (perfectly balanced) and 1 (max skew).
        method: 'max' uses (max_count - min_count) / max_count,
                'average' uses average absolute deviation from mean divided by mean.
        """
        if not isinstance(self.data, RustDeltaTable):
            raise TypeError("Partition data not loaded. Make sure to load a partitioned Delta table.")
        meta = self.data.metadata()
        partition_cols = getattr(meta, 'partition_columns', [])
        if not partition_cols:
            raise ValueError("No partition columns found in Delta table metadata.")
        
        df = pl.from_pandas(self.data.to_pandas())
        
        counts_df = df.group_by(partition_cols).count()
        
        records_per_partition_dict = {}
        for row in counts_df.iter_rows():
            if len(partition_cols) == 1:
                partition_key = str(row[0])
            else:
                partition_key = str(tuple(row[:-1]))
            records_per_partition_dict[partition_key] = row[-1]
        
        counts = counts_df.select('count').to_series().to_list()
        if len(counts) <= 1:
            return 0.0
        
        max_count = max(counts)
        min_count = min(counts)
        if max_count == 0:
            return 0.0
        
        if method == "max":
            normalized_skew = (max_count - min_count) / max_count
        elif method == "average":
            mean_count = sum(counts) / len(counts)
            avg_abs_dev = sum(abs(c - mean_count) for c in counts) / len(counts)
            normalized_skew = avg_abs_dev / mean_count if mean_count != 0 else 0.0
        else:
            raise ValueError(f"Unknown skewness method: {method}")
        
        if not hasattr(self, 'skew_metrics'):
            self.skew_metrics = {}
        
        self.skew_metrics = {
            'partition_columns': partition_cols,
            'skewness_max': normalized_skew if method == "max" else (max_count - min_count) / max_count if max_count > 0 else 0,
            'skewness_average': normalized_skew if method == "average" else (sum(abs(c - (sum(counts) / len(counts))) for c in counts) / len(counts)) / (sum(counts) / len(counts)) if sum(counts) > 0 else 0,
            'is_skewed': normalized_skew > threshold,
            'records_per_partition_dict': records_per_partition_dict
        }
        
        return normalized_skew

    def get_table_folder_size_bytes(self, table_path: Optional[str] = None) -> int:
        if table_path is None:
            if isinstance(self.data, RustDeltaTable):
                table_path = self.data.table_uri
            else:
                raise ValueError("table_path must be provided or DeltaTable must be loaded.")
        total_size = 0
        all_file_paths = []
        for dirpath, dirnames, filenames in os.walk(table_path):
            for f in filenames:
                if not f.endswith('.parquet'):
                    continue
                fp = os.path.join(dirpath, f)
                all_file_paths.append(fp)
                if os.path.isfile(fp):
                    total_size += os.path.getsize(fp)
        self._all_folder_file_paths = all_file_paths
        return total_size

    def check_delta_file_usage(self) -> dict:
        """
        Calculates the total size of Delta table files and returns file lists for orphan file detection.
        """
        if not isinstance(self.data, RustDeltaTable):
            raise TypeError("Partition data not loaded. Make sure to load a partitioned Delta table.")
        file_paths = self.data.file_uris()
        file_sizes = {}
        errors = {}
        for fp in file_paths:
            try:
                file_sizes[fp] = os.path.getsize(fp)
            except Exception as e:
                file_sizes[fp] = None
                errors[fp] = str(e)
        total_delta_file_size = sum(sz for sz in file_sizes.values() if sz is not None)
        delta_file_names = [os.path.basename(fp) for fp in file_paths]
        all_folder_file_names = [os.path.basename(fp) for fp in getattr(self, '_all_folder_file_paths', [])]
        return {
            'delta_file_sizes': file_sizes,
            'total_delta_file_size': total_delta_file_size,
            'delta_file_names': delta_file_names,
            'all_folder_file_names': all_folder_file_names,
            'errors': errors
        }


    def analyze(self, data: str) -> DeltaAnalyzerMetrics:
        """
        Analyze the Delta Lake table and return metrics.
        For backward compatibility, this method accepts only table_path as data.
        """
        return self.analyze_table(
            table_path=data,
            skew_threshold=0.1,
            vacuum_size_ratio_threshold=0.7,
            orphan_file_ratio_threshold=0.8,
            small_file_size_mb=10.0,
            small_file_ratio_threshold=0.3
        )

    def analyze_table(
        self,
        table_path: str,
        skew_threshold: float = 0.1,
        vacuum_size_ratio_threshold: float = 0.7,
        orphan_file_ratio_threshold: float = 0.8,
        small_file_size_mb: float = 10.0,
        small_file_ratio_threshold: float = 0.3
    ) -> DeltaAnalyzerMetrics:
        """
        Analyze the Delta Lake table and return health metrics.
        """
        self._load_data(table_path)
        
        if not isinstance(self.data, RustDeltaTable):
            raise TypeError("For Python environment, self.data must be a DeltaTable instance.")
        
        history = self.data.history()
        ops_summary = {"WRITE": [], "MERGE": [], "DELETE": [], "OPTIMIZE": []}
        metrics = DeltaAnalyzerMetrics()
        
        for entry in history or []:
            op = entry.get("operation", "")
            if op in ops_summary:
                ops_summary[op].append(
                    DeltaMetrics(
                        operation_type=op,
                        timestamp=str(entry.get("timestamp")),
                        version=entry.get("version", 0),
                        operation_metrics=entry.get("operationMetrics", {}),
                        parameters=entry.get("operationParameters", {})
                ))
            if op == "WRITE":
                metrics.number_of_writes += 1
            elif op == "DELETE":
                metrics.number_of_deletes += 1
            elif op == "OPTIMIZE":
                metrics.number_of_optimizes += 1
        
        metrics.version_count = max((entry.get("version", 0) for entry in history), default=0)

        try:
            self.analyze_skewness(threshold=skew_threshold, method="max")
            self.analyze_skewness(threshold=skew_threshold, method="average")
            
            if hasattr(self, 'skew_metrics'):
                metrics.skew_metrics = self.skew_metrics
                metrics.skewness_max = metrics.skew_metrics.get('skewness_max', 0.0)
                metrics.skewness_average = metrics.skew_metrics.get('skewness_average', 0.0)
                metrics.is_skewed = metrics.skew_metrics.get('is_skewed', False)
                metrics.partition_skewness = metrics.skew_metrics.get('skewness_max', 0.0)
        except Exception:
            metrics.skew_metrics = {
                'skewness_max': 0.0,
                'skewness_average': 0.0,
                'is_skewed': False,
                'error': 'Failed to calculate skewness'
            }
            metrics.skewness_max = 0.0
            metrics.skewness_average = 0.0
            metrics.is_skewed = False
            metrics.partition_skewness = None

        try:
            metrics.folder_size_bytes = self.get_table_folder_size_bytes()
        except Exception:
            metrics.folder_size_bytes = 0
            
        try:
            df = pl.read_delta(self.data.table_uri)
            metrics.record_count = len(df)
        except Exception:
            metrics.record_count = 0

        try:
            file_usage = self.check_delta_file_usage()
            metrics.table_size_bytes = file_usage['total_delta_file_size']
            total_folder_size = metrics.folder_size_bytes
            total_delta_size = metrics.table_size_bytes
            delta_file_count = len(file_usage['delta_file_names'])
            all_file_count = len(file_usage['all_folder_file_names'])
            size_ratio = total_delta_size / total_folder_size if total_folder_size > 0 else 1.0
            metrics.needs_vacuum = size_ratio < vacuum_size_ratio_threshold
            file_ratio = delta_file_count / all_file_count if all_file_count > 0 else 1.0
            metrics.has_orphan_files = file_ratio < orphan_file_ratio_threshold
            delta_file_sizes = [size for size in file_usage['delta_file_sizes'].values() if size is not None]
            if delta_file_sizes:
                avg_file_size = sum(delta_file_sizes) / len(delta_file_sizes)
                if small_file_size_mb is None:
                    small_file_threshold = avg_file_size
                else:
                    small_file_threshold = small_file_size_mb * 1024 * 1024
                small_files_count = sum(1 for size in delta_file_sizes if size < small_file_threshold)
                metrics.needs_optimize = small_files_count > len(delta_file_sizes) * small_file_ratio_threshold
                metrics.small_files_count = small_files_count
                metrics.avg_file_size_bytes = int(avg_file_size)
            else:
                metrics.needs_optimize = False
                metrics.small_files_count = 0
                metrics.avg_file_size_bytes = 0
        except Exception:
            metrics.table_size_bytes = 0
            metrics.needs_vacuum = False
            metrics.has_orphan_files = False
            metrics.needs_optimize = False
            metrics.small_files_count = 0
            metrics.avg_file_size_bytes = 0
        
        metrics.table_path = self.data.table_uri
        
        file_usage = self.check_delta_file_usage()
        metrics.data_file_count = len(file_usage.get('delta_file_names', []))
        metrics.total_file_count = len(file_usage.get('all_folder_file_names', []))
        
        all_files = set(file_usage.get('all_folder_file_names', []))
        delta_files = set(file_usage.get('delta_file_names', []))
        metrics.orphan_files_count = len(all_files - delta_files)
        
        metrics.files_needing_vacuum = 0 if metrics.needs_vacuum is False else metrics.orphan_files_count
        
        if metrics.folder_size_bytes > 0:
            metrics.storage_efficiency = metrics.table_size_bytes / metrics.folder_size_bytes
        
        if metrics.data_file_count > 0 and metrics.avg_file_size_bytes > 0:
            ideal_file_size = 128 * 1024 * 1024
            metrics.file_size_efficiency = min(1.0, metrics.avg_file_size_bytes / ideal_file_size)
        
        if hasattr(self, 'partition_cols') and self.partition_cols:
            metrics.partition_skewness = metrics.skewness_max

        return metrics
