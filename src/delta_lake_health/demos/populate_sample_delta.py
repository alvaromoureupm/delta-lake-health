import polars as pl
import numpy as np
import random
import os
from datetime import datetime, timedelta
from deltalake import write_deltalake, DeltaTable

SAMPLE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../data/tables")
SIMPLE_TABLE_PATH = os.path.join(SAMPLE_DIR, "simple_delta")
SKEWED_TABLE_PATH = os.path.join(SAMPLE_DIR, "skewed_delta")
COMPLEX_TABLE_PATH = os.path.join(SAMPLE_DIR, "complex_delta")

os.makedirs(SIMPLE_TABLE_PATH, exist_ok=True)
os.makedirs(SKEWED_TABLE_PATH, exist_ok=True)
os.makedirs(COMPLEX_TABLE_PATH, exist_ok=True)

OUTPUT_PATH = COMPLEX_TABLE_PATH


def create_simple_delta_table():
    print(f"Creating simple Delta table at: {SIMPLE_TABLE_PATH}")
    
    n_rows = 1000
    days = ["Mon"] * 200 + ["Tue"] * 200 + ["Wed"] * 200 + ["Thu"] * 200 + ["Fri"] * 200
    categories = ["A"] * 333 + ["B"] * 333 + ["C"] * 334
    
    df = pl.DataFrame({
        "id": list(range(1, n_rows + 1)),
        "value": np.random.normal(100, 30, n_rows),
        "day": days,
        "category": categories,
        "timestamp": [(datetime.now() - timedelta(days=1)).timestamp()] * n_rows
    })
    
    write_deltalake(SIMPLE_TABLE_PATH, df, mode="overwrite", partition_by=["day"])
    print(f"Initial write: {n_rows} rows with even distribution")
    
    append_data = pl.DataFrame({
        "id": list(range(n_rows + 1, n_rows + 501)),
        "value": np.random.normal(100, 30, 500),
        "day": ["Mon"] * 100 + ["Tue"] * 100 + ["Wed"] * 100 + ["Thu"] * 100 + ["Fri"] * 100,
        "category": ["A"] * 167 + ["B"] * 167 + ["C"] * 166,
        "timestamp": [datetime.now().timestamp()] * 500
    })
    write_deltalake(SIMPLE_TABLE_PATH, append_data, mode="append", partition_by=["day"])
    print("Single append: 500 rows with even distribution")
    
    # Final status
    dt = DeltaTable(SIMPLE_TABLE_PATH)
    print(f"\nSimple table has {len(dt.history())} versions")
    final_df = pl.read_delta(SIMPLE_TABLE_PATH)
    print(f"Simple table contains {final_df.shape[0]} records")
    
    return SIMPLE_TABLE_PATH


def create_skewed_delta_table():
    
    print(f"Creating skewed Delta table at: {SKEWED_TABLE_PATH}")
    
    n_rows = 1000
    days = ["Mon"] * 600 + ["Tue"] * 200 + ["Wed"] * 100 + ["Thu"] * 50 + ["Fri"] * 50
    categories = ["A"] * 500 + ["B"] * 300 + ["C"] * 200
    
    df = pl.DataFrame({
        "id": list(range(1, n_rows + 1)),
        "value": np.random.normal(100, 30, n_rows),
        "day": days,
        "category": categories,
        "timestamp": [(datetime.now() - timedelta(days=5)).timestamp()] * n_rows
    })
    
    write_deltalake(SKEWED_TABLE_PATH, df, mode="overwrite", partition_by=["day"])
    print(f"Initial write: {n_rows} rows with skewed distribution")
    
    append1_data = pl.DataFrame({
        "id": list(range(n_rows + 1, n_rows + 501)),
        "value": np.random.normal(110, 35, 500),
        "day": ["Mon"] * 350 + ["Tue"] * 100 + ["Wed"] * 30 + ["Thu"] * 10 + ["Fri"] * 10,
        "category": ["A"] * 250 + ["B"] * 150 + ["C"] * 100,
        "timestamp": [(datetime.now() - timedelta(days=4)).timestamp()] * 500
    })
    write_deltalake(SKEWED_TABLE_PATH, append1_data, mode="append", partition_by=["day"])
    print("Append 1: 500 rows with skewed distribution")
    
    append2_data = pl.DataFrame({
        "id": list(range(n_rows + 501, n_rows + 1001)),
        "value": np.random.normal(105, 25, 500),
        "day": ["Mon"] * 400 + ["Tue"] * 50 + ["Wed"] * 30 + ["Thu"] * 10 + ["Fri"] * 10,
        "category": ["A"] * 300 + ["B"] * 150 + ["C"] * 50,
        "timestamp": [(datetime.now() - timedelta(days=3)).timestamp()] * 500
    })
    write_deltalake(SKEWED_TABLE_PATH, append2_data, mode="append", partition_by=["day"])
    print("Append 2: 500 rows with even more skewed distribution")
    
    append3_data = pl.DataFrame({
        "id": list(range(n_rows + 1001, n_rows + 1501)),
        "value": np.random.normal(102, 18, 500),
        "day": ["Mon"] * 450 + ["Tue"] * 20 + ["Wed"] * 15 + ["Thu"] * 10 + ["Fri"] * 5,
        "category": ["A"] * 350 + ["B"] * 100 + ["C"] * 50,
        "timestamp": [(datetime.now() - timedelta(days=1)).timestamp()] * 500
    })
    write_deltalake(SKEWED_TABLE_PATH, append3_data, mode="append", partition_by=["day"])
    print("Append 3: 500 rows with extreme skew")
    
    dt = DeltaTable(SKEWED_TABLE_PATH)
    print(f"\nSkewed table has {len(dt.history())} versions")
    final_df = pl.read_delta(SKEWED_TABLE_PATH)
    print(f"Skewed table contains {final_df.shape[0]} records")
    print(f"Distribution by day: {final_df.group_by('day').count().sort('count', descending=True).to_dict()}")
    
    return SKEWED_TABLE_PATH


def create_complex_delta_table():
    
    n_rows = 1000
    days = ["Mon"] * 400 + ["Tue"] * 200 + ["Wed"] * 200 + ["Thu"] * 100 + ["Fri"] * 100
    categories = ["A"] * 500 + ["B"] * 300 + ["C"] * 200
    
    df = pl.DataFrame({
        "id": list(range(1, n_rows + 1)),
        "value": np.random.normal(100, 30, n_rows),
        "day": days,
        "category": categories,
        "timestamp": [(datetime.now() - timedelta(days=5)).timestamp()] * n_rows
    })
    
    print(f"Creating complex Delta table at: {COMPLEX_TABLE_PATH}")
    
    write_deltalake(COMPLEX_TABLE_PATH, df, mode="overwrite", partition_by=["day"])
    print(f"Initial write: {n_rows} rows")
    
    day2_data = pl.DataFrame({
        "id": list(range(n_rows + 1, n_rows + 301)),
        "value": np.random.normal(110, 35, 300),
        "day": ["Mon"] * 100 + ["Tue"] * 100 + ["Wed"] * 100,
        "category": ["A"] * 100 + ["B"] * 100 + ["C"] * 100,
        "timestamp": [(datetime.now() - timedelta(days=4)).timestamp()] * 300
    })
    write_deltalake(COMPLEX_TABLE_PATH, day2_data, mode="append", partition_by=["day"])
    print("Day 2 append: 300 rows")
    
    day3_data = pl.DataFrame({
        "id": list(range(n_rows + 301, n_rows + 501)),
        "value": np.random.normal(105, 25, 200),
        "day": ["Thu"] * 100 + ["Fri"] * 100,
        "category": ["A"] * 100 + ["B"] * 100,
        "timestamp": [(datetime.now() - timedelta(days=3)).timestamp()] * 200
    })
    write_deltalake(COMPLEX_TABLE_PATH, day3_data, mode="append", partition_by=["day"])
    print("Day 3 append: 200 rows")
    
    dt = DeltaTable(COMPLEX_TABLE_PATH)
    dt.delete("value > 150")
    print("Deletion operation: Removed rows where value > 150")
    
    day4_data = pl.DataFrame({
        "id": list(range(n_rows + 501, n_rows + 701)),
        "value": np.random.normal(95, 20, 200),
        "day": ["Mon"] * 50 + ["Tue"] * 50 + ["Wed"] * 50 + ["Thu"] * 25 + ["Fri"] * 25,
        "category": ["A"] * 80 + ["B"] * 70 + ["C"] * 50,
        "timestamp": [(datetime.now() - timedelta(days=2)).timestamp()] * 200
    })
    write_deltalake(COMPLEX_TABLE_PATH, day4_data, mode="append", partition_by=["day"])
    print("Day 4 append: 200 rows")
    
    dt.optimize.compact()
    print("Performed table optimization")
    
    day5_data = pl.DataFrame({
        "id": list(range(n_rows + 701, n_rows + 901)),
        "value": np.random.normal(102, 18, 200),
        "day": ["Mon"] * 80 + ["Tue"] * 40 + ["Wed"] * 40 + ["Thu"] * 20 + ["Fri"] * 20,
        "category": ["A"] * 100 + ["B"] * 60 + ["C"] * 40,
        "timestamp": [(datetime.now() - timedelta(days=1)).timestamp()] * 200
    })
    write_deltalake(COMPLEX_TABLE_PATH, day5_data, mode="append", partition_by=["day"])
    print("Day 5 append: 200 rows")
    
    dt = DeltaTable(COMPLEX_TABLE_PATH)
    dt.delete("value < 50")
    print("Second deletion operation: Removed rows where value < 50")
    
    for i in range(10):
        small_data = pl.DataFrame({
            "id": [i + 10000],
            "value": [random.normalvariate(100, 20)],
            "day": ["Mon"],
            "category": ["A"],
            "timestamp": [datetime.now().timestamp()]
        })
        write_deltalake(COMPLEX_TABLE_PATH, small_data, mode="append", partition_by=["day"])
    print("Created 10 small files for demonstration")
    
    dt = DeltaTable(COMPLEX_TABLE_PATH)
    print(f"\nFinal complex table history has {len(dt.history())} versions")
    final_df = pl.read_delta(COMPLEX_TABLE_PATH)
    print(f"Final complex table contains {final_df.shape[0]} records")
    print(f"Distribution by day: {final_df.group_by('day').count().sort('count', descending=True).to_dict()}")
    
    return COMPLEX_TABLE_PATH


def create_sample_delta_table():
    
    create_simple_delta_table()
    create_skewed_delta_table()
    create_complex_delta_table()
    
    print("\nAll three sample Delta tables created successfully:")
    print(f"1. Simple table (balanced, single append): {SIMPLE_TABLE_PATH}")
    print(f"2. Skewed table (multiple appends, skewed): {SKEWED_TABLE_PATH}")
    print(f"3. Complex table (writes, deletes, optimize, skewed): {COMPLEX_TABLE_PATH}")
    
    return COMPLEX_TABLE_PATH


def get_table_path(table_type="complex"):
    
    if table_type.lower() == "simple":
        return SIMPLE_TABLE_PATH
    elif table_type.lower() == "skewed":
        return SKEWED_TABLE_PATH
    else:  # Default to complex
        return COMPLEX_TABLE_PATH


if __name__ == "__main__":
    create_sample_delta_table()
