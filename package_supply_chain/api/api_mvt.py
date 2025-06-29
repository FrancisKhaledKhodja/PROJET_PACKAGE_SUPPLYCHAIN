
import os
import polars as pl
from package_supply_chain.miscellaneous_functions import get_execution_time
from package_supply_chain.my_loguru import logger

folder_parquet = r".\data_output"

@get_execution_time
def get_movements_oracle_intermediate():
    logger.info("Appel de l'api get_movements_oracle_intermediate")
    last_folder_parquet = get_last_folder_output_parquet()
    mvt_oracle = pl.read_parquet(os.path.join(folder_parquet, last_folder_parquet, "mvt_oracle.parquet"))
    correspondence_mvt_label = pl.read_parquet(os.path.join(folder_parquet, last_folder_parquet, "correspondence_mvt_oracle_speed.parquet"))
    correspondence_store_code = pl.read_parquet(
        os.path.join(folder_parquet, last_folder_parquet, "correspondence_store_code_oracle_speed.parquet")
        )
    
    mvt_oracle = mvt_oracle.unique()
    mvt_oracle = mvt_oracle.join(
        correspondence_mvt_label.select(
            pl.col("libelle_mvt_oracle", "code_mvt_speed", "libelle_mvt_speed")
            ), 
            how="left", 
            left_on="lib_motif_mvt", 
            right_on="libelle_mvt_oracle"
            )
    

    expression =(
        pl.when(pl.col("lib_motif_mvt") == "Ajustement inventaire physique")
        .then(
            pl.when(pl.col("sens_mvt") == "-")
            .then(pl.lit("2003"))
            .otherwise(pl.lit("1003"))
        )
        .otherwise(pl.col("code_mvt_speed"))
    ) 

    mvt_oracle = mvt_oracle.with_columns(expression.alias("code_mvt_speed"))

    expression = (
        pl.when(pl.col("code_mvt_speed") == "1003")
        .then(pl.lit("ENTREE AJUSTEMENT STOCK"))
        .otherwise(
            pl.when(pl.col("code_mvt_speed") == "2003")
            .then(pl.lit("SORTIE AJUSTEMENT STOCK"))
            .otherwise(pl.col("libelle_mvt_speed"))
        )
    )
    
    mvt_oracle = mvt_oracle.with_columns(expression.alias("libelle_mvt_speed"))

    mvt_oracle = mvt_oracle.join(
        correspondence_store_code.select(
            pl.col("code_magasin_oracle", "tiers_speed")), 
            how="left", 
            left_on="code_magasin_corrige", 
            right_on="code_magasin_oracle"
            )
    
    # Selection of the columns of the dataframe oracle
    columns_selected_oracle = ["n_mvt", "date_mvt", "proprietaire_article", "code_article", "libelle_article",
                               "n_lot", "n_serie", "qualite", "qte_mvt", "sens_mvt", "code_projet", 
                               "flag_stock_m_d", "n_bt", "code_ig", "code_mvt_speed", "libelle_mvt_speed", 
                               "tiers_speed"]
    
    mvt_oracle = mvt_oracle.select(columns_selected_oracle)
    mvt_oracle = mvt_oracle.rename({"code_mvt_speed": "code_motif_mvt", 
                                    "libelle_mvt_speed": "lib_motif_mvt", 
                                    "tiers_speed": "magasin"})

    logger.info("Fin de l'appel de l'api get_movements_oracle_intermediate")

    return mvt_oracle


def get_last_folder_output_parquet():
    return sorted(os.listdir(folder_parquet), reverse=True)[0]

@get_execution_time
def get_movements_oracle_and_speed():
    logger.info("Appel de l'api get_movements_oracle_and_speed")
    last_folder_parquet = get_last_folder_output_parquet()
    #mvt_oracle = pl.read_parquet(os.path.join(folder_parquet, last_folder_parquet, "mvt_oracle_intermediaire.parquet"))
    mvt_oracle = get_movements_oracle_intermediate()
    bu_sheet_repartition = pl.read_parquet(os.path.join(folder_parquet, last_folder_parquet, "bu_sheet_repartition.parquet"))
    bu_project_repartition = pl.read_parquet(os.path.join(folder_parquet, last_folder_parquet, "bu_project_repartition.parquet"))
    
    mvt_speed  = pl.scan_parquet(os.path.join(folder_parquet, last_folder_parquet, "mvt_speed.parquet"))
    mvt = pl.concat([mvt_oracle, mvt_speed], how="diagonal_relaxed")

    mvt = mvt.sort(pl.col("date_mvt", "n_mvt"))

    # identification de la feuille article
    items = pl.read_parquet(os.path.join(folder_parquet, last_folder_parquet, "items.parquet"))
    mvt = mvt.join(items.select(pl.col("code_article", "feuille_du_catalogue")), how="left", on="code_article")

    # idenification of the program project
    pe = pl.read_parquet(os.path.join(folder_parquet, last_folder_parquet, "pe.parquet"))
    pj = pl.read_parquet(os.path.join(folder_parquet, last_folder_parquet, "pj.parquet"))
    mvt = mvt.join(pe.select(pl.col("projet_elementaire", "code_programme_budgetaire_base_pe")), how="left", left_on="code_projet", right_on="projet_elementaire")
    mvt = mvt.join(pj.select(pl.col("projet_industrie", "code_programme_budgetaire_base_pe")), how="left", left_on="code_projet", right_on="projet_industrie")

    expression = (
        pl.when((pl.col("code_programme_budgetaire_base_pe").is_null()) 
                | (pl.col("code_programme_budgetaire_base_pe") == ""))
        .then(pl.col("code_programme_budgetaire_base_pe_right"))
        .otherwise(pl.col("code_programme_budgetaire_base_pe"))
    )
    mvt = mvt.with_columns(expression.alias("code_programme_budgetaire_base_pe"))
    mvt = mvt.drop(pl.col("code_programme_budgetaire_base_pe_right"))
    mvt = mvt.rename({"code_programme_budgetaire_base_pe": "code_programme"})

    # identification BU
    mvt = mvt.join(bu_sheet_repartition.select(pl.col("feuille_du_catalogue", "bu")), how="left", on="feuille_du_catalogue")
    mvt = mvt.join(bu_project_repartition.select(pl.col("code_programme", "bu")), how="left", on="code_programme")

    expression = (
        pl.when((pl.col("bu_right").is_null()) | (pl.col("bu_right") == ""))
        .then(pl.col("bu"))
        .otherwise(pl.col("bu_right"))
    )

    mvt = mvt.with_columns(expression.alias("bu"))
    mvt = mvt.drop("bu_right")

    logger.info("Fin de l'appel de l'api get_movements_oracle_and_speed")

    return mvt
    