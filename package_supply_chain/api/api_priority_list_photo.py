
import os
import datetime as dt
import polars as pl
import re

from package_supply_chain.constants import folder_path_output, folder_input_photo
from package_supply_chain.miscellaneous_functions import get_execution_time
from package_supply_chain.my_loguru import logger

def get_list_items_photo():
    pattern = r"[A-Z]{3}\d{4}(?:\d{2})?"
    list_photos = []
    for path, folder, elements in os.walk(folder_input_photo):
        for element in elements:
            if element.upper()[-3:] in ("JPG", "JPEG", "PNG"):
                list_photos.append(element.upper())
    list_items = [re.findall(pattern, photo)[0].upper() for photo in list_photos if re.findall(pattern, photo)]
    list_items = list(set(list_items))
    list_items.sort()
    return list_items, list_photos

@get_execution_time
def get_priority_list_photo():
    logger.info("Appel de l'api get_priority_list_photo")
    last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0]
    items_son_building = pl.read_parquet(os.path.join(folder_path_output, last_folder, "items_son_buildings.parquet"))
    stock = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stock_final.parquet"))
    mvt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))
    mvt_mplc_ol = mvt.filter((pl.col("magasin") == "MPLC") 
                             & (pl.col("lib_motif_mvt") == "ENVOI ORDRE DE LIVRAISON") 
                             & (pl.col("sens_mvt") == "-"))
    mvt_mplc_ol = mvt_mplc_ol.group_by(pl.col("code_article")).agg(pl.col("qte_mvt").sum()).sort(by="qte_mvt", descending=True)

    # today = dt.datetime.today().date()
    # mvt_mplc_ol_5_years = mvt.filter(pl.col("date_mvt") >= (today - dt.timedelta(days=1825)))
    # mvt_mplc_ol_5_years = mvt_mplc_ol_5_years.filter((pl.col("magasin") == "MPLC") 
    #                          & (pl.col("lib_motif_mvt") == "ENVOI ORDRE DE LIVRAISON") 
    #                          & (pl.col("sens_mvt") == "-"))
    # mvt_mplc_ol_5_years = mvt_mplc_ol_5_years.group_by(pl.col("code_article")).agg(pl.col("qte_mvt").sum()).sort(by="qte_mvt", descending=True)
    # mvt_mplc_ol_5_years = mvt_mplc_ol_5_years.rename({"qte_mvt": "qte_mvt_5_ans"})

    list_items_photo, list_photos = get_list_items_photo()
    stock_mplc = stock.filter((pl.col("code_magasin") == "MPLC") & (pl.col("transit").is_null()))
    stock_mplc = stock_mplc.group_by(pl.col("code_article", "libelle_court_article", "feuille_du_catalogue")).agg(pl.col("qte_stock").sum())
    stock_mplc = stock_mplc.join(mvt_mplc_ol, how="left", on="code_article")
    # stock_mplc = stock_mplc.join(mvt_mplc_ol_5_years, how="left", on="code_article")
    stock_mplc = stock_mplc.join(items_son_building, how="left", left_on="code_article", right_on="code_article_fils")

    stock_mplc = stock_mplc.with_columns(
        pl.when(pl.col("qte_mvt").is_null())
        .then(0)
        .otherwise(pl.col("qte_mvt"))
        .alias("qte_mvt")
        )
    stock_mplc = stock_mplc.sort(by="qte_mvt", descending=True)
    stock_mplc = stock_mplc.with_columns(
        pl.when(pl.col("code_article").is_in(list_items_photo))
        .then(True)
        .otherwise(False)
        .alias("dans_phototheque")
        )
    
    #stock_mplc = stock_mplc.filter(~pl.col("feuille_du_catalogue").is_in(["MIMO", "Free5G", "SIGFOX"]))
    
    logger.info("Fin de l'appel de l'api get_priority_list_photo")

    return stock_mplc
