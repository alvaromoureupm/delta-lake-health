from enum import Enum
from pydantic import BaseModel, Field
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any


class HealthStatus(str, Enum):
    """
    Enum for health status of a Delta Lake table.
    """
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    VERY_UNHEALTHY = "very_unhealthy"
    UNKNOWN = "unknown"


class DeltaAnalyzerMetrics(BaseModel):
    """
    Base model for Delta Lake health metrics.
    This can be extended to include specific metrics as needed.
    """
    version_count: int = 0
    partition_count: int = 0
    record_count: int = 0
    is_skewed: bool = False
    skewness_max: float = 0.0
    skewness_average: float = 0.0
    is_compacted: bool = False
    has_orfan_files: bool = False
    number_of_writes: int = 0
    number_of_deletes: int = 0
    number_of_optimizes: int = 0
    table_size_bytes: int = 0
    folder_size_bytes: int = 0
    needs_vacuum: bool = False
    has_orphan_files: bool = False
    needs_optimize: bool = False
    small_files_count: int = 0
    avg_file_size_bytes: int = 0
    partition_skewness: Optional[float] = None
    data_file_count: int = 0
    orphan_files_count: int = 0
    files_needing_vacuum: int = 0
    file_size_efficiency: Optional[float] = None
    storage_efficiency: Optional[float] = None
    table_path: str = ""
    total_file_count: int = 0
    skew_metrics: Dict[str, Any] = Field(default_factory=dict)
    health_score: Optional[float] = None
    health_status: Optional[HealthStatus] = None
    
    def calculate_health_score(self) -> tuple[float, 'HealthStatus']:
        """
        Calculate a comprehensive health score based on multiple metrics.
        Returns:
            - float: Overall health score (0-100)
            - HealthStatus: Categorical health status
        """
        component_scores = {}
        
        max_freshness_score = 25.0
        freshness_threshold = 10
        freshness_score = min(self.number_of_writes / freshness_threshold * max_freshness_score, max_freshness_score)
        component_scores['data_freshness'] = freshness_score
        
        max_maintenance_score = 25.0
        optimize_ratio = self.number_of_optimizes / max(self.number_of_writes, 1)
        optimize_score = min(optimize_ratio * 10 * (max_maintenance_score/2), max_maintenance_score/2)
        
        vacuum_score = 0 if self.needs_vacuum else max_maintenance_score/2
        
        maintenance_score = optimize_score + vacuum_score
        component_scores['maintenance'] = maintenance_score
        
        max_balance_score = 25.0
        skewness = self.skew_metrics.get('skewness_max', self.skewness_max) if self.skew_metrics else self.skewness_max
        skew_penalty = min(skewness * 100, max_balance_score)
        balance_score = max_balance_score - skew_penalty
        component_scores['data_balance'] = balance_score
        
        max_storage_score = 25.0
        small_files_ratio = self.small_files_count / max(self.number_of_writes * 2, 1)
        small_files_penalty = min(small_files_ratio * (max_storage_score/2), max_storage_score/2)
        
        orphan_penalty = max_storage_score/2 if self.has_orphan_files else 0
        
        storage_score = max_storage_score - small_files_penalty - orphan_penalty
        component_scores['storage_efficiency'] = storage_score
        
        overall_score = freshness_score + maintenance_score + balance_score + storage_score
        
        if overall_score >= 80:
            status = HealthStatus.HEALTHY
        elif overall_score >= 50:
            status = HealthStatus.UNHEALTHY
        else:
            status = HealthStatus.VERY_UNHEALTHY

        self.health_score = overall_score
        self.health_status = status
        return overall_score, status
    
    def print_results(self) -> None:
        """
        Print a formatted summary of Delta table health metrics.
        This includes health score, version count, record count, and all other key metrics.
        """
        health_score, health_status = self.calculate_health_score()
        
        print("\nDelta Table Analysis Results:")
        print("----------------------------")
        print(f"Health Score: {health_score:.1f}/100 ({health_status.value})")
        print(f"Version Count: {self.version_count}")
        print(f"Record Count: {self.record_count:,}")
        print(f"Operations: {self.number_of_writes} writes, {self.number_of_deletes} deletes, {self.number_of_optimizes} optimizes")
        print(f"Skewness: {self.skewness_max:.2f} (Max), {self.skewness_average:.2f} (Avg)")
        
        # Print partition skew metrics if available
        if self.skew_metrics:
            print("\nPartition Skew Metrics:")
            if 'partition_columns' in self.skew_metrics:
                print(f"Partition Columns: {', '.join(self.skew_metrics['partition_columns'])}")
            if 'records_per_partition_dict' in self.skew_metrics:
                records = self.skew_metrics['records_per_partition_dict']
                print(f"Partition Count: {len(records)}")
                if records:
                    max_partition = max(records.items(), key=lambda x: x[1])
                    min_partition = min(records.items(), key=lambda x: x[1])
                    print(f"Max Records: {max_partition[1]} (Partition: {max_partition[0]})")
                    print(f"Min Records: {min_partition[1]} (Partition: {min_partition[0]})")
        
        print(f"Table Size: {self.table_size_bytes / (1024*1024):.2f} MB")
        print(f"Folder Size: {self.folder_size_bytes / (1024*1024):.2f} MB")
        print(f"Total Files: {self.total_file_count} files")
        print(f"Data Files: {self.data_file_count} files")
        print(f"Small Files: {self.small_files_count} files")
        print(f"Orphan Files: {self.orphan_files_count} files")
        print(f"Needs Vacuum: {self.needs_vacuum}")
        print(f"Has Orphan Files: {self.has_orphan_files}")
        print(f"Needs Optimize: {self.needs_optimize}")
        
        return None

class DeltaMetrics(BaseModel):
    operation_type: str
    timestamp: str
    version: int
    operation_metrics: dict = {}
    parameters: dict = {}

class HealthResult(BaseModel):
    health_metric: int
    health_status: HealthStatus
    metrics: Optional[DeltaAnalyzerMetrics] = None


class BaseAnalyzer(ABC):
    """
    Abstract base class for health analyzers.
    All analyzers should inherit from this class and implement the `analyze` method.
    """
    data = None
    health_result = None

    @abstractmethod
    def _load_data(self, data: str) -> None:
        """
        Load data from the provided source and return a BaseModel.
        """
        pass
    
    @abstractmethod
    def analyze(self, data: str) -> DeltaAnalyzerMetrics:
        """
        Analyze the provided data and return DeltaAnalyzerMetrics.
        """
        pass
