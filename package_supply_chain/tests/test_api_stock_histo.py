import os
import datetime as dt

from package_supply_chain.api.api_stock_histo import get_stock_histo
from package_supply_chain.constants import folder_path_output


def test_api_stock_histo():
    today = dt.datetime.today().strftime("%Y%m%d")

    stock_histo = get_stock_histo()
    stock_histo.write_parquet(os.path.join(folder_path_output, today, "stock_histo_compil_final.parquet"))
