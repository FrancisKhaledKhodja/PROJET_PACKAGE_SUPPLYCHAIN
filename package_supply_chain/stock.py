import os
import polars as pl

from package_supply_chain.excel_csv_to_dataframe import read_excel, read_csv
from package_supply_chain.miscellaneous_functions import get_execution_time

@get_execution_time
class LoadStock():

    @get_execution_time
    def __init__(self, folder_path, file_name):
        self.df = read_excel(folder_path, file_name)
        self._rename_columns()
        self._cleaning_string("type_depot")
        self._transform_column_n_cde_dpm_dpi()
        self._remove_rows_without_date()
        #self._correct_store_code_missing()


    def _remove_rows_without_date(self):
        self.df = self.df.filter(pl.col("date_stock").is_not_null())

    def _rename_columns(self):
        transco_nom_colonne = {"code_magasin_tiers": "code_magasin", 
                               "type_de_depot___a3": "type_depot", 
                               "type_de_depot": "type_depot",
                               "n_bt_stock": "n_bt", 
                               "pump_date_du_stock": "pump", 
                               "libelle_court_article": "description_article", 
                               "feuille_du_catalogue": "feuille_catalogue", 
                               "lot1___lot_fabricant": "n_lot", 
                               "lot2___n_serie": "n_serie", 
                               "lot3___n_version": "n_version",
                               'lot_1_n_lot': "n_lot", 
                               'lot_2_n_serie': "n_serie",
                                'lot_3_n_version': "n_version",
                                "code_qualite": "qualite",
                                "dt_reception_emplacement": "date_reception",
                                "delai_anciennete_du_stock_jours": "delai_anciennete",
                                "stocksecu_inv_oui___non": "stock_secu", 
                               "reference_et_indice_cde": "n_cde_dpm_dpi", 
                               "dt_stock": "date_stock", 
                               "libelle_magasin_tiers": "libelle_magasin", 
                               "type_stock_d_m": "flag_stock_d_m",
                               "commentaire_operateur_tdf_-_a13": "commentaires_operateurs_tdf", 
                               "n_ums___a5": "n_ums", 
                               "a21_bt_stock": "numero_bt_2", 
                               "code_dossier": "code_projet", 
                               "n_cde_ref_dpm_pdi_stk": "n_cde_dpm_dpi", 
                               "numero_bt": "n_bt"}
        
        self.df = self.df.rename(transco_nom_colonne, strict=False)

    def _cleaning_string(self, column_name):
        self.df = self.df.with_columns(pl.col(column_name).str.strip_chars())

    def _correct_store_code_missing(self):
        self.df = self.df.with_columns(pl.when(pl.col("code_magasin").is_null()).then(pl.col("emplacement").str.split("-").implode().list.get(0)).alias("code_magasin"))

    def _write_parquet(self, folder_path):
        self.df.write_parquet(os.path.join(folder_path, "stock.parquet"))

    def _transform_column_n_cde_dpm_dpi(self):
        length_expr = pl.col("n_cde_dpm_dpi").str.len_chars()
        expression = (
            pl.when(
                (pl.col("n_cde_dpm_dpi").str.ends_with("001")) 
                & (~pl.col("n_cde_dpm_dpi").str.starts_with("E_"))
                & (~pl.col("n_cde_dpm_dpi").str.starts_with("D_"))
                )
            .then(pl.col("n_cde_dpm_dpi").str.slice(0, length_expr - 3))
            .otherwise(pl.col("n_cde_dpm_dpi"))
        )

        self.df = self.df.with_columns(expression.alias("n_cde_dpm_dpi"))

@get_execution_time
class StockHisto():

    def __init__(self, folder_path, file_name):
        self.df = read_csv(folder_path, file_name)
        self._rename_columns()
        self.clean_columns()
        self.change_type_columns("qte_stock", pl.Float32)
        self.clean_column_pump()
        self.change_type_columns("pump_date_du_stock", pl.Float32)
        self.convert_to_datetime("dt_reception", "%Y/%m/%d %H:%M:%S")
        self.convert_to_datetime("dt_stock", "%Y/%m/%d %H:%M:%S")
        self.add_column_valo_stock()
        
    def convert_to_datetime(self, column_name, format_datetime):
        self.df = self.df.with_columns(pl.col(column_name).str.to_datetime(format=format_datetime))

    def _rename_columns(self):
        transco_columns = {"lot1_lot_fabricant": "n_lot", 
                           "lot2_n_serie": "n_serie", 
                           "code_qualite": "qualite", 
                           "code_magasin_tiers": "code_magasin", 
                           "type_emplacement_a4": "transit", 
                           "dt_reception_emplacement": "dt_reception", 
                           "code_dossier": "code_projet"}
        
        self.df = self.df.rename(transco_columns)

    def clean_columns(self):
        expression = [(
            pl.when(pl.col(name_column) == "")
            .then(None)
            .otherwise(pl.col(name_column))
            .alias(name_column)
            ) for name_column in self.df.columns]

        self.df = self.df.with_columns(expression)

    def clean_column_pump(self):
        self.df = self.df.with_columns(pl.col("pump_date_du_stock").str.replace(",", "."))

    def change_type_columns(self, name_column, column_type):
        self.df = self.df.with_columns(pl.col(name_column).cast(column_type))

    def add_column_valo_stock(self):
        self.df = self.df.with_columns((pl.col("qte_stock") * pl.col("pump_date_du_stock")).alias("valo_stock"))


