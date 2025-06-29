import os
import datetime as dt
import polars as pl


from package_supply_chain.constants import folder_path_input, folder_path_output, file_name_500
from package_supply_chain.stock import StockHisto


def test_stock_histo():
    today = dt.datetime.today().strftime("%Y%m%d")

    stock_histo_compil = pl.DataFrame()
    for folder in os.listdir(os.path.join(folder_path_input, "MENSUEL")):
        stock_histo = StockHisto(os.path.join(folder_path_input, "MENSUEL", folder), file_name_500)
        stock_histo_compil = pl.concat([stock_histo_compil, stock_histo.df])

    stock_histo_compil.write_parquet(os.path.join(folder_path_output, today, "stock_histo.parquet"))
