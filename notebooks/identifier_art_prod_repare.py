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
def _(folder_path_output, os):
    last_folder = os.listdir(folder_path_output)[-1]
    last_folder
    return (last_folder,)


@app.cell
def _(folder_path_output, last_folder, os, pl):
    stores = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stores_final.parquet"))
    list_stores_repairers = stores.filter(pl.col("type_de_depot").str.contains("REPARATEUR")).select(pl.col("code_magasin")).to_series().to_list()
    list_stores_repairers.append("13AL")
    list_stores_repairers.append("MITC")

    items_to_check = pl.read_parquet(os.path.join(folder_path_output, last_folder, "items_to_check_after_return_from_prod.parquet"))
    items_to_check = items_to_check.with_columns(pl.concat_str(pl.col("code_article", "n_lot", "n_serie"), separator="-").alias("cle_article"))

    mvt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))
    mvt = mvt.with_columns(pl.concat_str(pl.col("code_article", "n_lot", "n_serie"), separator="-").alias("cle_article"))
    mvt = mvt.sort(pl.col("cle_article", "date_mvt", "n_mvt"), descending=True)

    list_items_to_analyze_mvt = mvt.filter((pl.col("lib_motif_mvt") == "ENTREE RETOUR PRODUCTION") 
                                    & (pl.col("n_lot").is_not_null()) 
                                    & (pl.col("n_serie").is_not_null()) 
                                       & (pl.col("code_article").str.starts_with("TDF"))).select(pl.col("cle_article")).to_series().to_list()

    stock = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stock_final.parquet"))
    stock = stock.with_columns(pl.concat_str(pl.col("code_article", "n_lot", "n_serie"), separator="-").alias("cle_article"))

    list_items_to_analyze_stock = stock.filter((pl.col("code_magasin") == "MPLC") 
                                         & (pl.col("n_serie").is_not_null()) 
                                            & (pl.col("qualite") == "GOOD") 
                                            & (pl.col("flag_stock_d_m") == "M") 
                                         & (pl.col("code_article").str.starts_with("TDF")) 
                                               & (pl.col("transit").is_null())).select(pl.col("cle_article")).to_series().to_list()

    list_items_to_analyze_stock_mvt = list(set(list_items_to_analyze_mvt).intersection(list_items_to_analyze_stock))

    #mvt = mvt.filter(pl.col("cle_article").is_in(list_items_to_analyze_stock_mvt))
    return (
        items_to_check,
        list_items_to_analyze_stock_mvt,
        list_stores_repairers,
        mvt,
        stock,
    )


@app.cell
def _(list_stores_repairers, mvt, pl):
    def identify_items_to_back_in_stock_D(cle_article):
        mvt_cle = mvt.filter(pl.col("cle_article") == cle_article)
        date_retour_prod = mvt_cle.filter(pl.col("lib_motif_mvt") == "ENTREE RETOUR PRODUCTION").head(1).select(pl.col("date_mvt")).to_series().to_list()[0]
        mvt_cle_filtered = mvt_cle.filter(pl.col("date_mvt") >= date_retour_prod)
        list_mag = set(mvt_cle_filtered.select(pl.col("magasin")).to_series().to_list())
        if list_mag.intersection(list_stores_repairers):
            return True
        return False
    return (identify_items_to_back_in_stock_D,)


@app.cell
def _(identify_items_to_back_in_stock_D, list_items_to_analyze_stock_mvt):
    list_items_to_back_in_stock_d = []
    for cle in list_items_to_analyze_stock_mvt:
        if identify_items_to_back_in_stock_D(cle):
            list_items_to_back_in_stock_d.append(cle)

    return (list_items_to_back_in_stock_d,)


@app.cell
def _(list_items_to_back_in_stock_d):
    list_items_to_back_in_stock_d
    return


@app.cell
def _(items_to_check, pl):
    list_items_to_check_mplc = items_to_check.filter((pl.col("code_magasin") == "MPLC") & (pl.col("transit").is_not_null())).select(pl.col("cle_article")).to_series().to_list()
    return (list_items_to_check_mplc,)


@app.cell
def _(list_items_to_back_in_stock_d, list_items_to_check_mplc):
    set(list_items_to_check_mplc).intersection(list_items_to_back_in_stock_d)
    return


@app.cell
def _(list_items_to_back_in_stock_d, pl, stock):
    stock.filter(pl.col("cle_article").is_in(list_items_to_back_in_stock_d)).write_excel("stock_m_a_mettre_en_d.xlsx")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
