import os
import datetime as dt
import polars as pl

from package_supply_chain.constants import folder_path_output
from package_supply_chain.my_loguru import logger


def get_items_without_exit():
    logger.info("Appel de l'api get_items_without_exit")
    label_mvt_exit = ["SORTIE INTERVENTION", "SORTIE PRODUCTION", "SORTIE CONSOMMATION"]
    date_today = dt.datetime.today().date()
    last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0]
    mvt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))
    last_mvt_exit_items = mvt.filter(pl.col("lib_motif_mvt").is_in(label_mvt_exit)).sort(pl.col("code_article", "date_mvt", "n_mvt")).unique(subset="code_article", keep="last").select(pl.col("date_mvt", "code_article"))
    last_mvt_exit_items = last_mvt_exit_items.with_columns(((date_today - pl.col("date_mvt")).dt.total_days()).alias("nbre_jours_sans_sortie"))

    
    
    expression = (
        pl.when(pl.col("nbre_jours_sans_sortie") < 2 * 30)
        .then(pl.lit("1 - moins de 2 mois"))
        .otherwise(
            pl.when(pl.col("nbre_jours_sans_sortie") < 6 * 30)
            .then(pl.lit("2 - entre de 2 et 6 mois"))
            .otherwise(
                pl.when(pl.col("nbre_jours_sans_sortie") < 12 * 30)
                .then(pl.lit("3 - entre de 6 mois et 1 an"))
                .otherwise(
                    pl.when(pl.col("nbre_jours_sans_sortie") < 2 * 12 * 30)
                    .then(pl.lit("3 - entre de 1 et 2 ans"))
                    .otherwise(
                        pl.when(pl.col("nbre_jours_sans_sortie") < 5 * 12 * 30)
                        .then(pl.lit("4 - entre 2 et 5 ans"))
                        .otherwise(
                            pl.when(pl.col("nbre_jours_sans_sortie") < 10 * 12 * 30)
                            .then(pl.lit("5 - entre 5 et 10 ans"))
                            .otherwise(
                                pl.when(pl.col("nbre_jours_sans_sortie") < 15 * 12 * 30)
                                .then(pl.lit("6 - entre 10 et 15 ans"))
                                .otherwise(
                                    pl.when(pl.col("nbre_jours_sans_sortie") < 18 * 12 * 30)
                                    .then(pl.lit("7 - entre 15 et 18 ans"))
                                    .otherwise(
                                        pl.lit("8 - plus de 18 ans")
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )

    last_mvt_exit_items = last_mvt_exit_items.with_columns(expression.alias("categorie_sans_sortie"))    

    logger.info("Fin de l'appel de l'api get_items_without_exit")
    return last_mvt_exit_items

