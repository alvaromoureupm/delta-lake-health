import pytest
import tempfile
import shutil
import polars as pl
import numpy as np
from deltalake import write_deltalake
from delta_lake_health.health_analyzers.delta_analyzer import DeltaAnalyzer, Environment

@pytest.fixture(scope="module")
def tmp_delta_table():
    tmpdir = tempfile.mkdtemp()
    table_path = f"{tmpdir}/tips"
    n_rows = 100
    days = ["Mon"] * 80 + ["Tue"] * 10 + ["Wed"] * 10
    times = ["Dinner"] * 80 + ["Lunch"] * 10 + ["Lunch"] * 10
    df = pl.DataFrame({
        "total_bill": np.random.uniform(10, 50, n_rows),
        "tip": np.random.uniform(1, 10, n_rows),
        "day": days,
        "time": times,
        "size": np.random.randint(1, 5, n_rows)
    })
    write_deltalake(table_path, df, mode="overwrite", partition_by=["day", "time"])
    for _ in range(3):
        write_deltalake(table_path, df.sample(10), mode="append", partition_by=["day", "time"])
    from deltalake import DeltaTable
    dt = DeltaTable(table_path)
    dt.delete("total_bill > 40")
    dt.optimize.compact()
    yield table_path
    shutil.rmtree(tmpdir)

def test_delta_analyzer_metrics_and_skewness(tmp_delta_table):
    analyzer = DeltaAnalyzer(environment=Environment.PYTHON)
    metrics = analyzer.analyze(table_path=tmp_delta_table)
    assert metrics is not None
    assert metrics.number_of_writes >= 4
    assert metrics.number_of_deletes >= 1
    assert metrics.number_of_optimizes >= 0
    assert metrics.skewness_max > 0.7
    assert metrics.skewness_average > 0.2

def test_delta_analyzer_no_skewness():
    tmpdir = tempfile.mkdtemp()
    table_path = f"{tmpdir}/tips_noskew"
    n_rows = 120
    days = ["Mon"] * 60 + ["Tue"] * 60
    times = ["Dinner"] * 30 + ["Lunch"] * 30 + ["Dinner"] * 30 + ["Lunch"] * 30
    df = pl.DataFrame({
        "total_bill": np.random.uniform(10, 50, n_rows),
        "tip": np.random.uniform(1, 10, n_rows),
        "day": days,
        "time": times,
        "size": np.random.randint(1, 5, n_rows)
    })
    write_deltalake(table_path, df, mode="overwrite", partition_by=["day", "time"])
    analyzer = DeltaAnalyzer(environment=Environment.PYTHON)
    metrics = analyzer.analyze(table_path=table_path)
    assert metrics.skewness_max < 0.01
    assert metrics.skewness_average < 0.01
    shutil.rmtree(tmpdir)

def test_table_size_metrics_no_orphan_files():
    tmpdir = tempfile.mkdtemp()
    table_path = f"{tmpdir}/tips_no_orphan"
    n_rows = 100
    days = ["Mon"] * 50 + ["Tue"] * 50
    times = ["Dinner"] * 50 + ["Lunch"] * 50
    df = pl.DataFrame({
        "total_bill": np.random.uniform(10, 50, n_rows),
        "tip": np.random.uniform(1, 10, n_rows),
        "day": days,
        "time": times,
        "size": np.random.randint(1, 5, n_rows)
    })
    write_deltalake(table_path, df, mode="overwrite", partition_by=["day", "time"])
    analyzer = DeltaAnalyzer(environment="python")
    # Use a lower threshold to avoid false positives for orphan files
    metrics = analyzer.analyze(table_path=table_path, orphan_file_ratio_threshold=0.5)
    assert metrics is not None
    assert metrics.has_orphan_files is False
    shutil.rmtree(tmpdir)

def test_table_size_metrics_many_orphan_files():
    tmpdir = tempfile.mkdtemp()
    table_path = f"{tmpdir}/tips_orphan"
    n_rows = 100
    days = ["Mon"] * 50 + ["Tue"] * 50
    times = ["Dinner"] * 50 + ["Lunch"] * 50
    df = pl.DataFrame({
        "total_bill": np.random.uniform(10, 50, n_rows),
        "tip": np.random.uniform(1, 10, n_rows),
        "day": days,
        "time": times,
        "size": np.random.randint(1, 5, n_rows)
    })
    write_deltalake(table_path, df, mode="overwrite", partition_by=["day", "time"])
    for _ in range(10):
        write_deltalake(table_path, df.sample(10), mode="append", partition_by=["day", "time"])
    from deltalake import DeltaTable
    dt = DeltaTable(table_path)
    dt.delete("total_bill > 40")
    dt.optimize.compact()
    analyzer = DeltaAnalyzer(environment="python")
    metrics = analyzer.analyze(table_path=table_path, orphan_file_ratio_threshold=0.8)
    assert metrics is not None
    assert metrics.has_orphan_files is True
    shutil.rmtree(tmpdir)

def test_skew_metrics_dictionary():
    tmpdir = tempfile.mkdtemp()
    table_path = f"{tmpdir}/tips_skew"
    n_rows = 100
    days = ["Mon"] * 80 + ["Tue"] * 10 + ["Wed"] * 10
    times = ["Dinner"] * 80 + ["Lunch"] * 10 + ["Lunch"] * 10
    df = pl.DataFrame({
        "total_bill": np.random.uniform(10, 50, n_rows),
        "tip": np.random.uniform(1, 10, n_rows),
        "day": days,
        "time": times,
        "size": np.random.randint(1, 5, n_rows)
    })
    
    write_deltalake(table_path, df, mode="overwrite", partition_by=["day", "time"])
    
    analyzer = DeltaAnalyzer(environment="python")
    metrics = analyzer.analyze(table_path=table_path)
    
    assert 'skew_metrics' in metrics.__dict__
    assert 'partition_columns' in metrics.skew_metrics
    assert 'skewness_max' in metrics.skew_metrics 
    assert 'skewness_average' in metrics.skew_metrics
    assert 'is_skewed' in metrics.skew_metrics
    assert 'records_per_partition_dict' in metrics.skew_metrics
    
    assert metrics.skew_metrics['partition_columns'] == ['day', 'time']
    
    assert metrics.skewness_max == metrics.skew_metrics['skewness_max']
    assert metrics.skewness_average == metrics.skew_metrics['skewness_average']
    assert metrics.is_skewed == metrics.skew_metrics['is_skewed']
    assert metrics.partition_skewness == metrics.skew_metrics['skewness_max']
    
    records_per_partition = metrics.skew_metrics['records_per_partition_dict']
    assert len(records_per_partition) > 0
    
    max_partition = max(records_per_partition.items(), key=lambda x: x[1])
    assert max_partition[1] >= 70
    assert 'Mon' in max_partition[0] or ('Mon', 'Dinner') == eval(max_partition[0])
    
    shutil.rmtree(tmpdir)
