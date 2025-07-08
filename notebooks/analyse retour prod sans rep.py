import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import os

    import polars as pl
    from package_supply_chain.constants import folder_path_output
    return folder_path_output, os, pl


@app.cell
def _():
    items_to_exclude = ["TDF15739", "TDF000170", "TDF000886", "TDF152673", "TDF152751", "TDF152756", "TDF153548", "TDF156062", "TDF159700"]
    return (items_to_exclude,)


@app.cell
def _(folder_path_output, os):
    last_folder = os.listdir(folder_path_output)[-1]
    last_folder
    return (last_folder,)


@app.cell
def _(folder_path_output, items_to_exclude, last_folder, os, pl):
    stores = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stores_final.parquet"))
    list_store_codes_national_local = stores.filter((pl.col("type_de_depot").is_in(["NATIONAL", "LOCAL"])) & (pl.col("statut") == 0) & (~pl.col("code_magasin").is_in(["RMA", "PERZ"]))).select(pl.col("code_magasin")).to_series().to_list()
    list_store_code_repairers = stores.filter((pl.col("type_de_depot").str.contains("REPARATEUR")) & (pl.col("statut") == 0)).select(pl.col("code_magasin")).to_series().to_list()
    list_store_code_repairers.append("13AL")

    mvt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))

    stock = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stock_final.parquet"))
    stock = stock.filter((pl.col("n_serie").is_not_null() & pl.col("n_lot").is_not_null()) 
                     & (pl.col("code_magasin").is_in(list_store_codes_national_local)) 
                     & (pl.col("qualite").is_in(["GOOD", "BLOQG"])) 
                        & (~pl.col("code_article").is_in(items_to_exclude)) 
                         & (pl.col("code_article").str.contains("TDF")))
    stock = stock.with_columns(pl.concat_str(pl.col("code_article", "n_lot", "n_serie"), separator="-").alias("cle_article"))
    list_items_to_analyze = stock.select(pl.col("cle_article")).to_series().to_list()
    print(len(list_items_to_analyze))

    mvt_filtered = mvt.filter((pl.col("n_lot").is_not_null()) & (pl.col("n_serie").is_not_null()))
    mvt_filtered = mvt_filtered.sort(pl.col("code_article", "n_lot", "n_serie", "date_mvt", "n_mvt"), descending=True)
    mvt_filtered = mvt_filtered.with_columns(pl.concat_str(pl.col("code_article", "n_lot", "n_serie"), separator="-").alias("cle_article"))
    mvt_filtered = mvt_filtered.filter(pl.col("cle_article").is_in(list_items_to_analyze))
    print(mvt_filtered.shape)

    expression = (
        pl.when(pl.col("lib_motif_mvt") == "ENTREE RETOUR PRODUCTION")
        .then(1)
        .otherwise(0)
    )

    mvt_filtered = mvt_filtered.with_columns(expression.alias("flag_retour_prod"))

    list_items_to_analyze = mvt_filtered.filter(pl.col("flag_retour_prod") == 1).select(pl.col("cle_article")).to_series().to_list()
    print(len(list_items_to_analyze))
    mvt_filtered = mvt_filtered.filter(pl.col("cle_article").is_in(list_items_to_analyze))
    print(mvt_filtered.shape)
    return list_items_to_analyze, list_store_code_repairers, mvt_filtered


@app.cell
def _():
    return


@app.cell
def _(list_store_code_repairers, mvt_filtered, pl):
    #ENTREE RETOUR PRODUCTION

    def get_mvt_items(cle_article):
        mvt_filtered_2 = mvt_filtered.filter((pl.col("cle_article") == cle_article))
        mvt_filtered_2 = mvt_filtered_2.sort(pl.col("date_mvt", "n_mvt"), descending=True)
        return  mvt_filtered_2


    def identify_item_to_check(cle_article):
        mvt_n_serie = get_mvt_items(cle_article)
        lib_retour_prod = "ENTREE RETOUR PRODUCTION"
        if lib_retour_prod in mvt_n_serie.select(pl.col("lib_motif_mvt")).to_series():
            date_retour_prod = mvt_n_serie.filter(pl.col("lib_motif_mvt") == lib_retour_prod).head(1).select(pl.col("date_mvt")).to_series().to_list()[0]
            mvt_filtered = mvt_n_serie.filter(pl.col("date_mvt") >= date_retour_prod)
            store_codes_filtered = mvt_filtered.select(pl.col("magasin")).to_series().to_list()
            if set(store_codes_filtered).intersection(list_store_code_repairers):
                return False
            return True
        return False
    return (identify_item_to_check,)


@app.cell
def _(identify_item_to_check, list_items_to_analyze):
    list_items_to_check = []
    for cle in list_items_to_analyze:
        if identify_item_to_check(cle) == True:
            list_items_to_check.append(cle)
    return (list_items_to_check,)


@app.cell
def _(list_items_to_check):
    list_items_to_check
    return


@app.cell
def _(mvt_filtered, pl):
    mvt_filtered.filter(pl.col("cle_article") == "TDF000157-OMNIA6 HD EX-4700FW6198")
    return


@app.cell
def _(list_store_code_repairers):
    list_store_code_repairers
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
