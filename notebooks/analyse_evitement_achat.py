import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    import datetime as dt
    import polars as pl
    import marimo as mo
    return dt, os, pl


@app.cell
def _():
    folder_path_output = r"C:\Users\Khaled-Khodja\Documents\PROJETS_PYTHON\PROJET_PACKAGE_SUPPLYCHAIN\data_output"
    return (folder_path_output,)


@app.cell
def _():
    codes_projet_a_ecarter = ["PJ00000564"]

    # quid projet PJ00000421?
    return (codes_projet_a_ecarter,)


@app.cell
def _(folder_path_output, os):
    last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0]
    last_folder
    return (last_folder,)


@app.cell
def _(codes_projet_a_ecarter, folder_path_output, last_folder, os, pl):

    #items = pl.read_parquet(os.path.join(output_folder, last_folder, "items.parquet"))
    #bu_project_repartition = pl.read_parquet(os.path.join(output_folder, last_folder, "bu_project_repartition.parquet"))
    #pe = pl.read_parquet(os.path.join(output_folder, last_folder, "pe.parquet"))
    #pj = pl.read_parquet(os.path.join(output_folder, last_folder, "pj.parquet"))
    stores = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stores.parquet"))

    mvt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))

    expression = (
            pl.when(pl.col("code_projet").is_in(codes_projet_a_ecarter))
            .then(pl.lit("M"))
            .otherwise(pl.col("flag_stock_m_d"))
        )

    mvt = mvt.with_columns(expression.alias("flag_stock_m_d_bis"))


    return mvt, stores


@app.cell
def _(pl, stores):
    list_repairers = stores.filter(pl.col("type_de_depot").str.contains("REPARATEUR")).select(pl.col("code_magasin")).to_series().to_list()
    return (list_repairers,)


@app.cell
def _(mvt, pl):
    art_a_analyser = mvt.pivot(index=["code_article", "n_lot", "n_serie"], on="flag_stock_m_d_bis", values="flag_stock_m_d_bis", aggregate_function="len").filter((pl.col("n_lot").is_not_null()) & (pl.col("n_serie").is_not_null()) & (pl.col("M") > 0) & (pl.col("D") > 0)).select(pl.col("code_article", "n_lot", "n_serie"))

    return


@app.cell
def _():
    """
    expression_2 = (
        pl.when(pl.col("flag_stock_m_d_bis") == "M")
        .then(0)
        .otherwise(1)
    )

    mvt = mvt.with_columns(expression_2.alias("flag_stock_m_d_bis"))
    """
    return


@app.cell
def _(codes_projet_a_ecarter, dt, folder_path_output, os, pl):

    def get_changement_m_to_d(mvt, list_reparaires, code_art: str, n_lot: str, n_serie: str):
        mvt_n_serie = mvt.filter((pl.col("code_article") == code_art)
                          & (pl.col("n_lot") == n_lot)
                          & (pl.col("n_serie") == n_serie)).sort(["date_mvt", "n_mvt"], descending=False)

        expression = (
            pl.when(pl.col("flag_stock_m_d_bis") == "M")
            .then(0)
            .otherwise(1)
        )

        mvt_n_serie = mvt_n_serie.with_columns(expression.alias("flag_stock_m_d_bis"))

        expression = (
            pl.when(pl.col("magasin").is_in(list_reparaires))
            .then(1)
            .otherwise(0)
        )

        mvt_n_serie = mvt_n_serie.with_columns(expression.alias("retour_reparation"))
    
        mvt_n_serie = mvt_n_serie.with_columns(pl.col("flag_stock_m_d_bis").diff().abs().fill_null(0).alias("diff_asc"))
        mvt_n_serie = mvt_n_serie.sort(["date_mvt", "n_mvt"], descending=True)
        mvt_n_serie = mvt_n_serie.with_columns(pl.col("flag_stock_m_d_bis").diff().abs().fill_null(0).alias("diff_desc"))
        mvt_n_serie = mvt_n_serie.with_columns((pl.col("diff_desc") + pl.col("diff_asc")).alias("diff"))

        info = {"code_article": code_art, "n_lot": n_lot, "n_serie": n_serie, "date_mvt": None, "ancien_code_projet": None, "code_projet": None, "retour_reparation": None}

        mvt_n_serie_filtered = mvt_n_serie.filter(pl.col("diff") == 1)

        if mvt_n_serie.shape[0] > 1 and mvt_n_serie_filtered.row(0, named=True)["flag_stock_m_d"] == "D" and mvt_n_serie_filtered.row(0, named=True)["code_projet"] not in codes_projet_a_ecarter:
            row_0 = mvt_n_serie_filtered.row(0, named=True)
            date_changement = row_0["date_mvt"]
            code_projet = row_0["code_projet"]
            row_1 = mvt_n_serie_filtered.row(1, named=True)
            ancien_code_projet = row_1["code_projet"]
            if 1 in mvt_n_serie.filter(pl.col("date_mvt") < date_changement).select(pl.col("retour_reparation")).to_series():
                retour_reparation = 1
            else:
                retour_reparation = 0
        
            info = {"code_article": code_art, "n_lot": n_lot, "n_serie": n_serie, "date_mvt": date_changement, "ancien_code_projet": ancien_code_projet, "code_projet": code_projet, "retour_reparation": retour_reparation}
            return info, mvt_n_serie

        return info, mvt_n_serie


    def get_items_m_to_d():
        codes_projet_a_ecarter = ["PJ00000564"]
    

        last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0]
        stores = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stores.parquet"))
        list_repairers = stores.filter(pl.col("type_de_depot").str.contains("REPARATEUR")).select(pl.col("code_magasin")).to_series().to_list()
        items = pl.read_parquet(os.path.join(folder_path_output, last_folder, "items.parquet"))
        bu_project_repartition = pl.read_parquet(os.path.join(folder_path_output, last_folder, "bu_project_repartition.parquet"))
        pe = pl.read_parquet(os.path.join(folder_path_output, last_folder, "pe.parquet"))
        pj = pl.read_parquet(os.path.join(folder_path_output, last_folder, "pj.parquet"))
        mvt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))
        mvt = mvt.filter(pl.col("date_mvt") >= dt.datetime(2021, 1, 1))

        expression = (
            pl.when(pl.col("code_projet").is_in(codes_projet_a_ecarter))
            .then(pl.lit("M"))
            .otherwise(pl.col("flag_stock_m_d"))
        )

        mvt = mvt.with_columns(expression.alias("flag_stock_m_d_bis"))

        list_items_to_analyze = mvt.pivot(index=["code_article", "n_lot", "n_serie"], on="flag_stock_m_d_bis" ,values="flag_stock_m_d_bis", aggregate_function="len").filter((pl.col("n_serie").is_not_null()) & (pl.col("n_lot").is_not_null())).filter((pl.col("D").is_not_null()) & (pl.col("M").is_not_null())).select(pl.col("code_article", "n_lot", "n_serie")).to_dicts()
        print("Nombre articles à analyser:", len(list_items_to_analyze))

        responses = []
        for i, row in enumerate(list_items_to_analyze):
            if i % 1000 == 0:
                print(i, "réalisé(s)")
            resp, _ = get_changement_m_to_d(mvt, list_repairers, row["code_article"], row["n_lot"], row["n_serie"])
            responses.append(resp)


        list_items_m_to_d = pl.DataFrame(responses, schema_overrides={"ancien_code_projet": pl.String})
        list_items_m_to_d = list_items_m_to_d.filter(pl.col("date_mvt").is_not_null())
        list_items_m_to_d = list_items_m_to_d.join(items.select("code_article", "pump"), how="left", on="code_article")
        list_items_m_to_d = list_items_m_to_d.join(pe.select("projet_elementaire", "libelle_pe", "projet_industrie_information"), how="left", left_on="code_projet", right_on="projet_elementaire")

        expression_2 = (
            pl.when(pl.col("code_projet").str.starts_with("PJ"))
            .then(pl.col("code_projet"))
            .otherwise(pl.col("projet_industrie_information"))
        )

        list_items_m_to_d = list_items_m_to_d.with_columns(expression_2.alias('projet_industrie_information'))

        list_items_m_to_d = list_items_m_to_d.join(pj.select("projet_industrie", "libelle_projet_industrie", "code_programme_budgetaire_base_pe", "lib_programme_budgetaire_base_pe"), how="left", left_on="projet_industrie_information", right_on="projet_industrie")
        list_items_m_to_d = list_items_m_to_d.join(bu_project_repartition, how="left", left_on="code_programme_budgetaire_base_pe", right_on="code_programme")


        return list_items_m_to_d



    return get_changement_m_to_d, get_items_m_to_d


@app.cell
def _(get_items_m_to_d):
    responses = get_items_m_to_d()
    return (responses,)


@app.cell
def _(responses):
    responses.write_excel("analyse_m_to_d.xlsx")
    return


@app.cell
def _(responses):
    responses
    return


@app.cell
def _(get_changement_m_to_d, list_repairers, mvt):



    info, mvt_n_serie = get_changement_m_to_d(mvt, list_repairers, "TDF152874", "858011", "85801100000146")
    return (mvt_n_serie,)


@app.cell
def _(mvt_n_serie):
    mvt_n_serie
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
