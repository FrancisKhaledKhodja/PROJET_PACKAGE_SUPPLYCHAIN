import os
import polars as pl

from package_supply_chain.miscellaneous_functions import get_execution_time
from package_supply_chain.excel_csv_to_dataframe import read_excel
from package_supply_chain.constants import (STORES_CLOSED, 
                                            UPSTREAM_STORE_TYPES, 
                                            UPSTREAM_STORES)

class ReferentialStores():
    
    @get_execution_time
    def __init__(self, folder_path, file_name, sheet_name):
        self.df = read_excel(folder_path, file_name, sheet_name)
        self._rename_columns_name()
        self._correct_store_status()
        self._sort_dataframe()
        self._remove_duplicate_rows()
        self._cleaning_latitude_and_longitude()
        self._add_rma_store()
        self._identify_upstream_and_downstream_stores()
        self._identify_cluster_18DE_95MG_18BB_44AA_MBRO()
        self._identify_cluster_MMHS_MVEO_MTRA_MPER()

    def _rename_columns_name(self):
        transco_colonnes = {"code_mag_speed": "code_magasin", 
                            "nom_mag_speed": "libelle_magasin", 
                            "code_mag_oracle": "code_tiers_daher", 
                            "categorie_mag_speed": "type_de_depot"}
        self.df = self.df.rename(transco_colonnes)


    def _correct_store_status(self):
        self.df = self.df.with_columns(
            pl.when(
                pl.col("code_magasin").is_in(STORES_CLOSED)
            )
            .then(1)
            .otherwise(pl.col("statut"))
            .alias("statut")
        )


    def _add_rma_store(self):
        df_mplc = self.df.filter(pl.col("code_magasin") == "MPLC")
        df_rma = df_mplc.with_columns(pl.col("code_magasin").replace("MPLC", "RMA"))
        self.df = pl.concat([self.df, df_rma])


    def _cleaning_latitude_and_longitude(self):
        self.df = self.df.with_columns(pl.col("latitude").str.strip_chars().str.replace(",", ".").cast(pl.Float64))
        self.df = self.df.with_columns(pl.col("longitude").str.strip_chars().str.replace(",", ".").cast(pl.Float64))

    def _identify_upstream_and_downstream_stores(self):
        
        self.df = self.df.with_columns(
            pl.when(
                pl.col("code_magasin").is_in(UPSTREAM_STORES))
                .then(pl.lit("AMONT"))
                .otherwise(
                    pl.when(
                        pl.col("type_de_depot").is_in(UPSTREAM_STORE_TYPES)
                        )
                        .then(pl.lit("AMONT"))
                        .otherwise(pl.lit("AVAL"))
                        )
                        .alias("amont_aval")
                        )


    def _identify_cluster_18DE_95MG_18BB_44AA_MBRO(self):
        CLUSTER_STORES = ("18DE", "95MG", "18BB", "44AA", "MBRO")

        self.df = self.df.with_columns(
            pl.when(
                pl.col("code_magasin").is_in(CLUSTER_STORES)
                )
                .then(True)
                .otherwise(False)
                .alias("mag_18DE_95MG_18BB_44AA_MBRO")
                )


    def _identify_cluster_MMHS_MVEO_MTRA_MPER(self):
        CLUSTER_STORES = ("MMHS", "MVEO", "MVEO", "MTRA")
        self.df = self.df.with_columns(
            pl.when(
                pl.col("code_magasin").is_in(CLUSTER_STORES))
                .then(True)
                .otherwise(False)
                .alias("mag_MMHS_MVEO_MTRA_MPER")
                )
    
    def _sort_dataframe(self):
        self.df = self.df.sort(["code_magasin", "date_creation"], descending=[False, False])


    def _remove_duplicate_rows(self):
        self.df = self.df.unique(subset=["code_magasin"], keep="last")

    def _write_parquet(self, folder_path):
        self.df.write_parquet(os.path.join(folder_path, "stores.parquet"))
