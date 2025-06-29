import os
import polars as pl
from package_supply_chain.constants import folder_path_output
from package_supply_chain.items import Items


def test_items():
    last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0]
    items = pl.read_parquet(os.path.join(folder_path_output, last_folder, "items.parquet"))
    print(items.columns)
