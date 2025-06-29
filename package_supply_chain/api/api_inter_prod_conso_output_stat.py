import os
import datetime as dt
import polars as pl

from package_supply_chain.constants import folder_path_output
from package_supply_chain.miscellaneous_functions import get_execution_time
from package_supply_chain.my_loguru import logger

get_execution_time
def get_inter_conso_output_dataframe(mvt: pl.DataFrame, year: str) -> pl.DataFrame:
    inter_conso_output = mvt.filter(
        (pl.col("date_mvt").dt.year() == year)
        & (pl.col("lib_motif_mvt").is_in(["SORTIE INTERVENTION", "SORTIE CONSOMMATION"]))
    )
    inter_conso_output = inter_conso_output.group_by(pl.col("code_article")).agg(pl.col("qte_mvt").sum())
    inter_conso_output = inter_conso_output.rename({"qte_mvt": "sorties_{}".format(year)})
    return inter_conso_output

def get_inter_conso_output_statistics():
    logger.info("Appel de l'api get_inter_prod_conso_output")
    last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0]
    mvt_oracle_speed = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))
    items = pl.read_parquet(os.path.join(folder_path_output, last_folder, "items.parquet"))

    list_items = items.select(pl.col("code_article")).to_series().to_list()
    inter_conso_output_compil = pl.DataFrame({"code_article": list_items})

    for year in range(2020, 2026):
        inter_conso_output = get_inter_conso_output_dataframe(mvt_oracle_speed, year)
        inter_conso_output_compil = inter_conso_output_compil.join(inter_conso_output, how="left", on="code_article")

    expression = (
        pl.sum_horizontal([pl.col(col_name) for col_name in inter_conso_output_compil.columns if "sorties_" in col_name]).alias("total")
    )

    inter_conso_output_compil = inter_conso_output_compil.with_columns(expression)

    logger.info("Fin de l'appel de l'api get_intervention_output")

    return inter_conso_output_compil

