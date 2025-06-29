import os
import polars as pl
from package_supply_chain.api.api_stock import get_state_stocks
from package_supply_chain.miscellaneous_functions import get_execution_time
from package_supply_chain.constants import folder_path_output


@get_execution_time
def test_api_stock():
    last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0]
    stock = get_state_stocks()
    stock.write_parquet(os.path.join(folder_path_output, last_folder, "stock_final.parquet"))
    stock.write_excel("stock.xlsx")
    


