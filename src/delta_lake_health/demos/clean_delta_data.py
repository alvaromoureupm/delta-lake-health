
import shutil
import os
from delta_lake_health.demos.populate_sample_delta import SIMPLE_TABLE_PATH, SKEWED_TABLE_PATH, COMPLEX_TABLE_PATH

TABLE_PATH = "./data/tables/tips"
DATA_DIR = "./data/tables"

def clean_delta_data():
    # Remove the Delta table folder if it exists
    for path in [TABLE_PATH, SIMPLE_TABLE_PATH, SKEWED_TABLE_PATH, COMPLEX_TABLE_PATH]:
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"Removed Delta table folder: {path}")
    # Remove the parent data directory if it exists and is empty
    if os.path.exists(DATA_DIR) and not os.listdir(DATA_DIR):
        os.rmdir(DATA_DIR)
        print(f"Removed empty data directory: {DATA_DIR}")
    # Remove the top-level data folder if it is empty
    if os.path.exists("./data") and not os.listdir("./data"):
        os.rmdir("./data")
        print("Removed empty top-level data directory: ./data")

if __name__ == "__main__":
    clean_delta_data()
