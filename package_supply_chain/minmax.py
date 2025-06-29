from package_supply_chain.miscellaneous_functions import get_execution_time
import polars as pl
from package_supply_chain.excel_csv_to_dataframe import read_excel


class MinMax():

    @get_execution_time
    def __init__(self, folder_path: str, file_name: str):
        self.df = read_excel(folder_path, file_name)
        self._rename_columns()
        self._cleaning_string()
        self._remove_item_without_min()

    def _realisation_dico(self):
        self.dico = {}
        for i, row in self.table.iterrows():
            if row["code_magasin"] not in self.dico:
                self.dico[row["code_magasin"]] = {}
            elif row["code_article"] not in self.dico[row["code_magasin"]]:
                self.dico[row["code_magasin"]][row["code_article"]] = {}
                self.dico[row["code_magasin"]][row["code_article"]]["min"] = row["qte_min"]
                self.dico[row["code_magasin"]][row["code_article"]]["max"] = row["qte_max"]

    def _rename_columns(self):
        transco_nom_colonne = {"code_magasin_ou_site": "code_magasin", 
                               "tie_nom": "libelle_magasin"}
        self.df = self.df.rename(transco_nom_colonne)


    def _cleaning_string(self):
        self.df = self.df.with_columns(pl.col("code_article").str.strip_chars())


    def _remove_item_without_min(self):
        self.df = self.df.filter((pl.col("qte_min") != 0) & (pl.col("qte_max") != 0)) 

