import os
import polars as pl

from package_supply_chain.excel_csv_to_dataframe import read_excel
from package_supply_chain.miscellaneous_functions import get_execution_time
from package_supply_chain.my_loguru import logger

class Helios():
    @get_execution_time
    def __init__(self, folder_path: str, file_name: str) -> None:
        self.df = read_excel(folder_path, file_name)
        self._making_dictionnary()
        self.preserve_active_ig()

    
    def _making_dictionnary(self):
        self.dictionnary = {row["code_ig"]: row for row in self.df.iter_rows(named=True)}


    def control_latitude_and_longitude(self):
        pass


    def preserve_active_ig(self):
        # Conserver les IG actifs
        self.df_active_ig = self.df.clone()
        # On retire les codes IG Point Relais de la table
        self.df_active_ig = self.df_active_ig.filter(pl.col("categorie") != "Point Relais")
        # On retire les codes IG obsolètes:
        self.df_active_ig = self.df_active_ig.filter(pl.col("etat_ig") != "Oboslète")
        # On retire les IG fictif
        self.df_active_ig = self.df_active_ig.filter(pl.col("adresse") != "IG fictif")
        # On retire les IG test
        self.df_active_ig = self.df_active_ig.filter(pl.col("commune") != "COMMUNE TEST")

    def _write_parquet(self, folder_path):
        self.df.write_parquet(os.path.join(folder_path, "helios.parquet"))


class HeliosEquip():

    @get_execution_time
    def __init__(self, folder_path: str, file_name: str, sheet_name: str) -> None:
        self.df = read_excel(folder_path, file_name, sheet_name)
        columns = ["nb_sites_active", "nb_sites_demontes", "nb_sites_desactivites", 
                   "nb_sites_en_cours_desinstallation", "nb_sites_en_cours_installation", "nb_sites_en_cours_modification"]
        self.df = self.df.with_columns(pl.col(columns).fill_null(0))
        self.df = self.df.filter(pl.col("code_article").is_not_null())
        self.df = self.df.filter(pl.col("code_article").is_not_null())
        self._add_active_column()


    def _add_active_column(self):
        self.df = self.df.with_columns(
                pl.fold(
                    acc=pl.lit(False), 
                    function=lambda acc, x: acc | (x > 0), 
                    exprs=[pl.col("nb_sites_active"), pl.col("nb_sites_en_cours_installation"), pl.col("nb_sites_en_cours_modification")]
                    )
                    .alias("actif")
        )

    def _write_parquet(self, folder_path):
        self.df.write_parquet(os.path.join(folder_path, "helios_equipement.parquet"))
