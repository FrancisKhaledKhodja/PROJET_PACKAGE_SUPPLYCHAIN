import os
import polars as pl

from package_supply_chain.constants import folder_path_output
from package_supply_chain.miscellaneous_functions import get_execution_time
from package_supply_chain.my_loguru import logger

@get_execution_time
def get_stock_histo():
    logger.info("Appel de l'api get_histo_stock")
    last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0]
    stock_histo = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stock_histo_compil.parquet"))
    items = pl.read_parquet(os.path.join(folder_path_output, last_folder, "items.parquet"))
    stock_oracle = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stock_oracle.parquet"))
    correspondence_store_codes = pl.read_parquet(os.path.join(folder_path_output, last_folder,"correspondence_store_code_oracle_speed.parquet"))
    bu_sheet_repartition = pl.read_parquet(os.path.join(folder_path_output, last_folder, "bu_sheet_repartition.parquet"))
    bu_project_repartition = pl.read_parquet(os.path.join(folder_path_output, last_folder, "bu_project_repartition.parquet"))
    pe = pl.read_parquet(os.path.join(folder_path_output, last_folder, "pe.parquet"))
    pj = pl.read_parquet(os.path.join(folder_path_output, last_folder, "pj.parquet"))

    # Cleaning the stock oracle
    
    # expression = [
    #     (
    #         pl.when(pl.col(column_name) == "")
    #         .then(None)
    #         .otherwise(pl.col(column_name))
    #         .alias(column_name)
    #     ) for column_name in stock_oracle.columns if stock_oracle.schema[column_name] == pl.String
    # ]

    # stock_oracle = stock_oracle.with_columns(expression)


    # Add store code speed ont stock oracle
    stock_oracle = stock_oracle.join(correspondence_store_codes.select(pl.col("code_magasin_oracle", "tiers_speed")), 
                                     how="left", 
                                     left_on="code_magasin", 
                                     right_on="code_magasin_oracle")


    # Add reception date oracle
    stock_histo = stock_histo.join(stock_oracle.select(pl.col("tiers_speed", "code_article", "n_lot", "n_serie", "dt_reception_emplacement")), 
                                   how="left", 
                                   left_on=["code_magasin", "code_article", "n_lot", "n_serie"], 
                                   right_on=["tiers_speed", "code_article", "n_lot", "n_serie"])
    

    # Add new column with correct reception date in stock
    expression = (
        pl.when(pl.col("dt_reception_emplacement").is_null())
        .then(pl.col("dt_reception"))
        .otherwise(pl.col("dt_reception_emplacement"))
        .alias("dt_reception_corrigee")
    )
    stock_histo = stock_histo.with_columns(expression)

    # add column to display the age of the stock
    stock_histo = stock_histo.with_columns((pl.col("dt_stock") - pl.col("dt_reception_corrigee")).dt.total_days().alias("nb_jours_anciennete"))

    # Add column to display the age category of the stock

    column_age_of_the_stock = pl.col("nb_jours_anciennete")
    expression = (
        pl.when(column_age_of_the_stock < 2 * 30)
        .then(pl.lit("1 - moins de 2 mois"))
        .otherwise(
            pl.when(column_age_of_the_stock < 6 * 30)
            .then(pl.lit("2 - entre 2 et 6 mois"))
            .otherwise(
                pl.when(column_age_of_the_stock < 12 * 30)
                .then(pl.lit("3 - entre 6 et 12 mois"))
                .otherwise(
                    pl.when(column_age_of_the_stock < 2 * 12 * 30)
                    .then(pl.lit("4 - entre 1 et 2 ans"))
                    .otherwise(
                        pl.when(column_age_of_the_stock <  5 * 12 * 30)
                        .then(pl.lit("5 - entre 2 et 5 ans"))
                        .otherwise(
                            pl.when(column_age_of_the_stock < 10 * 12 * 30)
                            .then(pl.lit("6 - entre 5 et 10 ans"))
                            .otherwise(
                                pl.when(column_age_of_the_stock < 15 * 12 * 30)
                                .then(pl.lit("7 - entre 10 et 15 ans"))
                                .otherwise(
                                    pl.when(column_age_of_the_stock < 18 * 12 * 30)
                                    .then(pl.lit("8 - entre 15 et 18 ans"))
                                    .otherwise(pl.lit("9 - plus de 18 ans"))
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    
    stock_histo = stock_histo.with_columns(expression.alias("categorie_anciennete_stock"))


    # Add the missing pump
    stock_histo = stock_histo.join(items.select(pl.col("code_article", "pump")), how="left", on="code_article")

    expression = (
    pl.when(pl.col("pump_date_du_stock").is_null())
    .then(pl.col("pump"))
    .otherwise(pl.col("pump_date_du_stock"))
    .alias("pump_date_du_stock")
    )

    stock_histo = stock_histo.with_columns(expression)
    stock_histo = stock_histo.drop("pump")

    # New computation of the value of the stock
    stock_histo = stock_histo.with_columns((pl.col("qte_stock") * pl.col("pump_date_du_stock")).alias("valo_stock"))
    logger.info("Fin de l'Appel de l'api get_histo_stock")

    # Add bu sheet items (for the computation of the bu)
    stock_histo = stock_histo.join(items.select(pl.col("code_article", "feuille_du_catalogue")), how="left", on="code_article")
    stock_histo = stock_histo.join(bu_sheet_repartition.select(pl.col("feuille_du_catalogue", "bu")), how="left", on="feuille_du_catalogue")

    # Add program code
    stock_histo = stock_histo.join(pe.select(pl.col("projet_elementaire", "code_programme_budgetaire_base_pe")), 
                                   how="left", 
                                   left_on="code_projet", 
                                   right_on="projet_elementaire")
    stock_histo = stock_histo.join(pj.select(pl.col("projet_industrie", "code_programme_budgetaire_base_pe")), how="left", left_on="code_projet", right_on="projet_industrie")

    expression = (
        pl.when(pl.col("code_programme_budgetaire_base_pe_right").is_null())
        .then(pl.col("code_programme_budgetaire_base_pe"))
        .otherwise(pl.col("code_programme_budgetaire_base_pe_right"))
        .alias("code_programme")
    )

    stock_histo = stock_histo.with_columns(expression)

    stock_histo = stock_histo.drop(pl.col("code_programme_budgetaire_base_pe", "code_programme_budgetaire_base_pe_right"))

    # Add bu project
    stock_histo = stock_histo.join(bu_project_repartition.select(pl.col("code_programme", "bu")), how="left", on="code_programme")

    # Compute the correct bu
    expression = (
        pl.when(pl.col("bu_right").is_null())
        .then(pl.col("bu"))
        .otherwise(pl.col("bu_right"))
        .alias("bu")
    )

    stock_histo = stock_histo.with_columns(expression)

    stock_histo = stock_histo.drop("bu_right")


    return stock_histo



