from delta_lake_health.health_analyzers.delta_analyzer import DeltaAnalyzer, Environment
from delta_lake_health.health_analyzers.base_analyzer import DeltaAnalyzerMetrics

TABLE_PATH = "./data/tables/complex_delta"

def test_analyze_python_by_path():
    analyzer = DeltaAnalyzer(environment=Environment.PYTHON)
    metrics = analyzer.analyze(table_path=TABLE_PATH)
    assert isinstance(metrics, DeltaAnalyzerMetrics)
    assert "complex_delta" in metrics.table_path
    assert metrics.version_count > 0
