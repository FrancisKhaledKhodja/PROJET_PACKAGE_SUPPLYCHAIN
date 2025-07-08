import os
import polars as pl

from package_supply_chain.constants import folder_path_output
from package_supply_chain.my_loguru import logger
from package_supply_chain.miscellaneous_functions import get_execution_time


def get_tracks_of_good_return(mvt, cle_article: str) -> list[dict]:
    
    mvt_cle = mvt.filter((pl.col("cle_article") == cle_article) & (pl.col("n_bt").is_not_null()))
    mvt_cle = mvt_cle.with_row_index()
    start_index = -1
    blocs_mvt = []
    for row in mvt_cle.filter(pl.col("flag_mvt") != 0).iter_rows(named=True):
        if row["flag_mvt"] == 1:
            start_index = row["index"]
        if start_index != -1 and row["flag_mvt"] == 2:
            end_index = row["index"]
            blocs_mvt.append(mvt_cle.filter((pl.col("index") >= start_index) & (pl.col("index") <= end_index)))
            start_index = -1

    responses = []
    for bloc_mvt in blocs_mvt:
        row_1 = bloc_mvt.head(1)
        row_2 = bloc_mvt.tail(1)
        nbre_jours = row_2["date_mvt"].to_list()[0] - row_1["date_mvt"].to_list()[0]
        nbre_jours = nbre_jours.days
        cle_article = "{}-{}-{}".format(row_1["code_article"].to_list()[0], row_1["n_lot"].to_list()[0], row_1["n_serie"].to_list()[0])
        responses.append({"date_expe": row_1["date_mvt"].to_list()[0],
                          "code_mag_expe": row_1["magasin"].to_list()[0], 
                          "code_article": row_1["code_article"].to_list()[0], 
                          "n_lot": row_1["n_lot"].to_list()[0], 
                          "n_serie": row_1["n_serie"].to_list()[0], 
                          "n_cde": row_1["n_cde"].to_list()[0], 
                          "n_bt": row_1["n_bt"].to_list()[0], 
                          "date_recep": row_2["date_mvt"].to_list()[0], 
                          "code_mag_recep": row_2["magasin"].to_list()[0], 
                          "nbre_jours": nbre_jours, 
                          "cle_article": cle_article})
        
    return responses


@get_execution_time
def get_items_with_return_good():
    logger.info("Appel de l'api get_items_with_return_good")
    last_folder = os.listdir(folder_path_output)[-1]
    mvt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))
    mvt = mvt.with_columns(pl.concat_str(pl.col("code_article", "n_lot", "n_serie"), separator="-").alias("cle_article"))
    mvt = mvt.sort(pl.col("cle_article", "date_mvt", "n_mvt"), descending=False)

    expression_1 = (
        pl.when(pl.col("code_motif_mvt").is_in(["3016", "3021"]))
        .then(1)
        .otherwise(0)
    )

    mvt = mvt.with_columns(expression_1.alias("flag_mvt"))

    expression_2 = (
        pl.when((pl.col("code_motif_mvt").is_in(["3004"])) & (pl.col("sens_mvt") == "-"))
        .then(2)
        .otherwise(pl.col("flag_mvt"))
    )

    mvt = mvt.with_columns(expression_2.alias("flag_mvt"))

    mvt_to_analyze = mvt.filter((pl.col("n_serie").is_not_null()) 
                            & (pl.col("qualite") == "GOOD") 
                            & (pl.col("code_motif_mvt") == "3004")
                            & (pl.col("n_bt").is_not_null()))

    list_items_to_analyze = mvt_to_analyze.select(pl.col("cle_article")).unique().to_series().to_list()

    responses = []
    for cle_article in list_items_to_analyze:
        resp = get_tracks_of_good_return(mvt, cle_article)
        responses.extend(resp)

    responses = pl.DataFrame(responses)

    logger.info("Fin de l'appel de l'api get_items_with_return_good")

    return responses

