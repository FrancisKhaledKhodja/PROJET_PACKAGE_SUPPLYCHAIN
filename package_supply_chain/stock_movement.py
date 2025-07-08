

import polars as pl

from package_supply_chain.excel_csv_to_dataframe import read_csv
from package_supply_chain.miscellaneous_functions import get_execution_time


class MovementSpeed():

    @get_execution_time
    def __init__(self, folder_path, file_name):
        self.df = read_csv(folder_path, file_name)
        self._rename_columns()
        self._cleaning_columns()
        self._convert_to_datetime("date_mvt")
        self._convert_type_columns()
        self._create_bt_columns()
        self._remove_duplicate_rows()
        self._item_owner()
        self._identify_spare_or_projet_stock()
        self._sort_dataframe()
        self._put_in_the_order_the_columns()
        self._correct_error_mvt_label()
        self._change_type_columns()
    
    def _change_type_columns(self):
        self.df = self.df.with_columns(pl.col("flag_panne_sur_stock").cast(pl.Int8))

    def _create_bt_columns(self):
        self.df = self.df.with_columns(pl.coalesce(["n_bt_mvt", "n_bt_stock"]).alias("n_bt"))
        self.df = self.df.drop(["n_bt_mvt", "n_bt_stock"])


    def _put_in_the_order_the_columns(self):
        list_new_order = ["n_mvt", "date_mvt", "code_motif_mvt", "lib_motif_mvt", "lib_type_mvt_speed", "magasin",
                          "emplacement", "proprietaire_article", "code_article", "libelle_article", "n_lot",
                          "n_serie", "qualite", "qte_mvt", "sens_mvt", "code_magasin_destinataire", "code_projet",
                          "flag_stock_m_d", "n_bt", "code_ig_intervention", "n_cde", "n_cde_speed", "n_ligne_cde_speed",
                          "n_cde_dpm_dpi", "n_dossier_reparation", "cause_retour_equipement", "flag_panne_sur_stock", "n_rma"]
        
        self.df = self.df.select(list_new_order)


    def _convert_type_columns(self):
        type_columns = {"n_mvt": pl.Int64, 
                        "code_article": pl.String, 
                        "n_dossier_reparation": pl.String, 
                        "cause_retour_equipement": pl.String, 
                        "code_motif_mvt": pl.Int16,
                        "lib_motif_mvt": pl.String,
                        "n_lot": pl.String,
                        "n_serie": pl.String,
                        "qualite": pl.String,
                        "qte_mvt": pl.Int16,
                        "sens_mvt": pl.String,
                        "magasin": pl.String,
                        "emplacement": pl.String,
                        "n_cde": pl.String,
                        "code_projet": pl.String,
                        "n_cde_speed": pl.String,
                        "n_ligne_cde_speed": pl.String,
                        "n_cde_dpm_dpi": pl.String,
                        "code_ig_intervention": pl.String,
                        "n_bt_mvt": pl.String,
                        "n_bt_stock": pl.String,
                        "n_bt": pl.String,
                        "flag_panne_sur_stock": pl.String,
                        "n_rma": pl.String,
                        "code_magasin_destinataire": pl.String,
                        "lib_type_mvt_speed": pl.String}
        self.df = self.df.with_columns([pl.col(column_name).cast(type_column) for column_name, type_column in type_columns.items()])


    def _rename_columns(self):
        transco = {"stk_lot1___lot_fabricant": "n_lot", 
                   "lot1_ref_fabricant": "n_lot", 
                   "lot2_n_serie": "n_serie",
                   "stk_lot2___n_serie": "n_serie", 
                   "dt_mvt": "date_mvt", 
                   "description_article_tdf": "libelle_article",
                   "code_qualite": "qualite", 
                   "magasin_emplacement": "magasin", 
                   "n_commande_stk_stk_alpha20": "n_cde", 
                   "code_dossier": "code_projet", 
                   "n_cde_speed_mvt": "n_cde_speed", 
                   "n_lg_cde_speed_mvt": "n_ligne_cde_speed", 
                   "n_cde_ref_dpm_pdi_mvt": "n_cde_dpm_dpi", 
                   "ig_intervention": "code_ig_intervention", 
                   "num_rma": "n_rma"}
        
        self.df = self.df.rename(transco, strict=False)


    def _cleaning_columns(self):
        columns_to_clean = ["sens_mvt", "lib_motif_mvt"]
        self.df = self.df.with_columns([pl.col(col).str.strip_chars() for col in columns_to_clean])

        for column_name in self.df.columns:
            self.df = self.df.with_columns(
                pl.when(pl.col(column_name) == "")
                .then(None)
                .otherwise(pl.col(column_name))
                .alias(column_name))

    def _remove_duplicate_rows(self):
        self.df = self.df.unique()


    def _sort_dataframe(self):
        self.df = self.df.sort(by=["n_mvt", "date_mvt"], descending=False)


    def _convert_to_datetime(self, column_name):
        self.df = self.df.with_columns(pl.col(column_name).str.to_datetime())


    def _identify_spare_or_projet_stock(self):
        expression = (
            pl.when(pl.col("code_projet").is_not_null())
            .then(pl.lit("D"))
            .otherwise(pl.lit("M"))
        ) 

        self.df = self.df.with_columns(expression.alias("flag_stock_m_d"))

    def _item_owner(self):
        expression = (
            pl.col("code_article").str.slice(0,3)
        )

        self.df = self.df.with_columns(expression.alias("proprietaire_article"))

    def _correct_error_mvt_label(self):
        expression =(
            pl.when(pl.col("lib_motif_mvt") == "SORTIE ECLATEMENT REFENCE")
            .then(pl.lit("SORTIE ECLATEMENT REFERENCE"))
            .otherwise(pl.col("lib_motif_mvt"))
        )

        self.df = self.df.with_columns(expression.alias("lib_motif_mvt"))




class MovementOracle():
    
    def __init__(self, folder_path, file_name):
        self.df = read_csv(folder_path, file_name)
        self._rename_columns()
        self._cleaning_columns()
        self._modify_type_column()
        self._remove_row_without_item()
        self._correct_pump_column()
        self._correct_mvt_qty_column()
        self._correct_mvt_sign_column()
        self._correct_date_column()
        self._identify_store_code()
        self._identify_quality()
        self._creating_code_ig_column()
        self._identify_project_code()
        self._identify_spare_or_projet_stock()
        self._item_owner()
        self._removing_columns()
        self._put_in_the_order_the_columns()




    def _modify_type_column(self):
        self.df = self.df.with_columns(pl.col("n_mvt").cast(pl.Int32))

    def _remove_row_without_item(self):
        self.df = self.df.filter(pl.col("code_article").is_not_null())


    def _put_in_the_order_the_columns(self):
        list_new_order = ["n_mvt", "date_mvt", "lib_motif_mvt", "code_magasin_corrige", "code_magasin", "libelle_magasin",
                          "type_depot", "equipe", "region", "proprietaire_article", "code_article", "libelle_article",
                          "stock_secu", "feuille_catalogue", "article_consommable", "statut_article", "pump", "n_lot",
                          "n_serie", "qualite", "qte_mvt", "sens_mvt", "code_projet", "flag_stock_m_d", "code_emplacement_expediteur",
                          "code_magasin_expediteur", "lieu_silex_expediteur", "lib_programme_pe_expediteur", "code_emplacement_destinataire",
                          "code_magasin_destinataire", "lieu_silex_destinataire", "lib_programme_pe_destinataire", "type_de_ci",
                          "n_ci", "n_di", "type_de_di", "n_bt", "code_ig"]
        self.df = self.df.select(list_new_order)




    def _creating_code_ig_column(self):
        expression = (
            pl.when(pl.col("code_ig_bt_intervention").is_not_null())
            .then(pl.col("code_ig_bt_intervention"))
            .otherwise(pl.col("code_ig_intervention"))
        )

        self.df = self.df.with_columns(expression.alias("code_ig"))


    def _removing_columns(self):
        for column_name in ("code_ig_bt_intervention", "code_ig_intervention", "code_projet_expediteur", "nb_mvt"):
            self.df = self.df.drop(column_name)


    def _rename_columns(self):
        transco = {"n_mouvement": "n_mvt", 
                       "dt_mouvement": "date_mvt",
                       "code_article": "code_article",
                       "code_qualite": "qualite",
                       "code_magasin_3": "emplacement",
                       "n_lot": "n_lot",
                       "n_serie": "n_serie",
                       "pump_a_la_date_du_mvt_eur": "pump",
                       "libelle_type_mvt": "lib_motif_mvt", 
                       "code_ig_mvt": "code_ig_intervention",
                       "projet_pa_destinataire": "code_projet",  
                       "n_bt_intervention": "n_bt", 
                       "code_motif_mvt": "code_motif_mvt", 
                       'n_cde_ref_dpm,_pdi_mvt': 'n_cde_dpm_dpi', 
                       "nb_mouvements_ns": "nb_mvt", 
                       "libelle_court_article": "libelle_article", 
                       "stocksecu_inv_oui_non": "stock_secu", 
                       "feuille_du_catalogue": "feuille_catalogue", 
                       "article_de_consommable": "article_consommable", 
                       "qte_du_mouvement_ns": "qte_mvt", 
                       "statut_abrege_article": "statut_article", 
                       "do_equipe_du_magasin": "equipe", 
                       "dr_region_du_magasin": "region",
                       "flag_sens_et": "sens_mvt", 
                       "projet_pa_expediteur": "code_projet_expediteur"}
        
        self.df = self.df.rename(transco, strict=False)


    def _correct_pump_column(self):
        self.df = self.df.with_columns(pl.col("pump").str.replace(",", ".").cast(pl.Float32, strict=False))


    def _correct_mvt_qty_column(self):
        self.df = self.df.with_columns(pl.col("qte_mvt").str.replace(",", ".").cast(pl.Float32))


    def _correct_mvt_sign_column(self):
        self.df = self.df.with_columns(pl.col("sens_mvt").str.strip_chars())


    def _correct_date_column(self):
        self.df = self.df.with_columns(pl.col("date_mvt").str.to_datetime("%Y/%m/%d %H:%M:%S"))
    

    def _identify_quality(self):
        expression = (
            pl.when(
                pl.col("code_magasin_destinataire").is_not_null()
            )
            .then(
                pl.when(
                    pl.col("code_magasin_destinataire").str.contains("ECR")
                )
                .then(pl.lit("BAD"))
                .otherwise(pl.lit("GOOD"))
            )
            .otherwise(
                pl.when(
                    pl.col("code_magasin_expediteur").is_not_null()
                )
                .then(
                    pl.when(
                        pl.col("code_magasin_expediteur").str.contains("ECR")
                    )
                    .then(pl.lit("BAD"))
                    .otherwise(pl.lit("GOOD"))
                )
                .otherwise(pl.lit("GOOD"))
            )
        )
        self.df = self.df.with_columns(expression.alias("qualite"))


    def _cleaning_columns(self):
        for column_name in self.df.columns:
            self.df = self.df.with_columns(
                pl.when(pl.col(column_name) == "")
                .then(None)
                .otherwise(pl.col(column_name))
                .alias(column_name))


    def _identify_store_code(self):
        expression = (
            pl.when(
                pl.col("code_magasin_destinataire").is_not_null()
            )
            .then(
                pl.when(
                    ~pl.col("code_magasin_destinataire").str.contains("TRZ")
                    & ~pl.col("code_magasin_destinataire").str.contains("ECR")
                )
                .then(pl.col("code_magasin_destinataire"))
                .otherwise(pl.col("lieu_silex_destinataire"))
            ).otherwise(
                pl.when(
                    ~pl.col("code_magasin_expediteur").str.contains("TRZ")
                    & ~pl.col("code_magasin_expediteur").str.contains("ECR")
                )
                .then(pl.col("code_magasin_expediteur"))
                .otherwise(pl.col("lieu_silex_expediteur"))
            )
        )

        self.df = self.df.with_columns(expression.alias("code_magasin_corrige"))


    def _identify_project_code(self):
        expression = (
            pl.when(pl.col("code_projet").is_not_null())
            .then(pl.col("code_projet"))
            .otherwise(pl.col("code_projet_expediteur"))
            )
        self.df = self.df.with_columns(expression.alias("code_projet"))


    def _identify_spare_or_projet_stock(self):
        expression = (
            pl.when(pl.col("code_projet").is_not_null())
            .then(pl.lit("D"))
            .otherwise(pl.lit("M"))
        ) 

        self.df = self.df.with_columns(expression.alias("flag_stock_m_d"))

    def _item_owner(self):
        expression = (
            pl.col("code_article").str.slice(0,3)
        )

        self.df = self.df.with_columns(expression.alias("proprietaire_article"))

