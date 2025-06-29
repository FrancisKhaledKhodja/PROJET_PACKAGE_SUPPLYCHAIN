import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    import datetime as dt
    import re
    import polars as pl
    import plotly.express as px
    import marimo as mo
    return dt, mo, os, pl, px


@app.cell
def _():
    output_folder = r"C:\Users\Khaled-Khodja\Documents\PROJETS_PYTHON\PROJET_PACKAGE_SUPPLYCHAIN\data_output"
    return (output_folder,)


@app.cell
def _(os, output_folder):
    last_folder = sorted(os.listdir(output_folder), reverse=True)[0]
    last_folder
    return (last_folder,)


@app.cell
def _(code_projet_PJ564, last_folder, os, output_folder, pl):
    mvt = pl.read_parquet(os.path.join(output_folder, last_folder, "mvt_oracle_and_speed_final.parquet"))
    stock = pl.read_parquet(os.path.join(output_folder, last_folder, "stock_final.parquet"))
    items = pl.read_parquet(os.path.join(output_folder, last_folder, "items.parquet"))
    #photos = pl.read_parquet(os.path.join(output_folder, last_folder, "list_photos.parquet"))
    helios = pl.read_parquet(os.path.join(output_folder, last_folder, "helios.parquet"))

    expression = (
        pl.when(pl.col("code_projet") == code_projet_PJ564)
        .then(pl.lit("M"))
        .otherwise(pl.col("flag_stock_m_d"))
    )

    mvt = mvt.with_columns(expression.alias("flag_stock_m_d_bis"))
    return helios, items, mvt, stock


@app.cell
def _(helios):
    helios
    return


@app.cell
def _(dt):
    start_date = dt.datetime.today() - dt.timedelta(days=1095)
    number_of_days = dt.datetime.today() - start_date
    return number_of_days, start_date


@app.cell
def _(mo):
    mo.Html("<H2>VALEUR STOCK POUR CALCUL ASSURANCE</H2>")
    return


@app.cell
def _(pl, stock):
    stock_mplc_qualite = stock.filter((pl.col("code_magasin") == "MPLC") & (pl.col("transit").is_null()))
    stock_mplc_qualite = stock_mplc_qualite.group_by(pl.col("code_magasin", "code_article", "libelle_court_article", "qualite", "pump")).agg(pl.col("qte_stock", "valo_stock").sum())
    stock_mplc_qualite = stock_mplc_qualite.with_columns(pl.col("valo_stock").round(2).alias("valo_stock"))
    stock_mplc_qualite.write_excel("valeur_stock_evaluation_assurance.xlsx")
    return (stock_mplc_qualite,)


@app.cell
def _(stock_mplc_qualite):
    stock_mplc_qualite
    return


@app.cell
def _(mo):
    mo.Html("<H2>ROTATION DES STOCK SUR MPLC</H2>")
    return


@app.cell
def _(pl, stock):
    stock_mplc = stock.filter((pl.col("code_magasin") == "MPLC") & (pl.col("transit").is_null()))
    stock_mplc = stock_mplc.group_by(pl.col("code_magasin", "code_article")).agg(pl.col("qte_stock").sum())
    return (stock_mplc,)


@app.cell
def _(mo):
    mo.Html("<h2> CLASSE ABC DES EXPEDITIONS MPLC</H2>")
    return


@app.cell
def _(items, mvt, number_of_days, pl, start_date):

    mvt_mplc_expe = mvt.filter((pl.col("lib_motif_mvt") == "ENVOI ORDRE DE LIVRAISON") 
               & (pl.col("magasin") == "MPLC") 
               & (pl.col("sens_mvt") == "-")
              & (pl.col('date_mvt') >= start_date))

    tcd = mvt_mplc_expe.group_by("code_article").agg(pl.col("qte_mvt").sum())
    tcd = tcd.join(items.select(pl.col("code_article", "libelle_court_article", "type_article", "statut_abrege_article", "criticite_pim", "feuille_du_catalogue")), how="left", on="code_article")
    tcd = tcd.with_columns((pl.col("qte_mvt") / number_of_days.days * 365).alias("qte_moy_expediee_par_an"))
    tcd = tcd.with_columns(pl.col("qte_moy_expediee_par_an").round(0)).sort("qte_moy_expediee_par_an", descending=True)
    tcd = tcd.with_columns((pl.col("qte_moy_expediee_par_an") / pl.col("qte_moy_expediee_par_an").sum()).alias("ratio_expe"))
    tcd = tcd.with_columns(pl.col("ratio_expe").cum_sum().alias("ratio_expe_cumulatif"))

    expression_2 = (
        pl.when(pl.col("ratio_expe_cumulatif") < 0.8)
        .then(pl.lit("A"))
        .otherwise(
            pl.when(pl.col("ratio_expe_cumulatif") < 0.95)
            .then(pl.lit("B"))
            .otherwise(pl.lit("C"))
        )
    )

    tcd = tcd.with_columns(expression_2.alias("classe_ABC_sur_3_ans"))
    tcd = tcd.select(pl.col(
        "code_article",
        "libelle_court_article",
        "type_article",
        "statut_abrege_article",
        "criticite_pim",
        "feuille_du_catalogue",
        "qte_mvt",
        "qte_moy_expediee_par_an",
        "ratio_expe",
        "ratio_expe_cumulatif",
        "classe_ABC_sur_3_ans")
    )

    return mvt_mplc_expe, tcd


@app.cell
def _(items, pl, stock_mplc, tcd):
    stock_mplc_rotation = stock_mplc.join(tcd.select(pl.col("code_article", "qte_moy_expediee_par_an")), how="left", on="code_article")
    stock_mplc_rotation = stock_mplc_rotation.with_columns((pl.col("qte_stock") / pl.col("qte_moy_expediee_par_an")).round(1).alias("couverture_en_annee"))
    stock_mplc_rotation = stock_mplc_rotation.join(items.select(pl.col("code_article", "libelle_court_article","type_article", "statut_abrege_article", "criticite_pim","feuille_du_catalogue")), how="left", on="code_article")
    stock_mplc_rotation = stock_mplc_rotation.select(pl.col([
      "code_magasin",
      "code_article",
    "libelle_court_article",
      "type_article",
      "statut_abrege_article",
      "criticite_pim",
      "feuille_du_catalogue",
      "qte_stock",
      "qte_moy_expediee_par_an",
      "couverture_en_annee"]))
    stock_mplc_rotation
    return (stock_mplc_rotation,)


@app.cell
def _(stock_mplc_rotation):
    stock_mplc_rotation.write_excel("couverture_stock_mplc.xlsx")
    return


@app.cell
def _(tcd):
    tcd.write_excel("classe_ABC_expeditions_mplc.xlsx")
    return


@app.cell
def _(items, mvt_mplc_expe, pl):
    mvt_expe_mplc_poids_volume = mvt_mplc_expe.join(items.select(pl.col("code_article", "poids_article", "volume_article")), how="left", on="code_article")
    mvt_expe_mplc_poids_volume = mvt_expe_mplc_poids_volume.with_columns((pl.col("qte_mvt") * pl.col("poids_article")).alias("poids_total"))
    mvt_expe_mplc_poids_volume = mvt_expe_mplc_poids_volume.with_columns((pl.col("qte_mvt") * pl.col("volume_article")).alias("volume_total"))
    mvt_expe_mplc_poids_volume.write_excel("expe_mplc_poids_volume.xlsx")
    return


@app.cell
def _(items):
    items.columns
    return


@app.cell
def _():
    return


@app.cell
def _(pl, px, tcd):
    px.scatter(tcd.select(pl.col("ratio_expe_cumulatif")))
    return


@app.cell
def _(mo):
    mo.Html("<H2>ANALYSE DES ENTREES STOCK MPLC</H2>")
    return


@app.cell
def _(mvt, pl):
    mvt.select(pl.col("lib_motif_mvt")).unique().sort("lib_motif_mvt")
    return


@app.cell
def _(items, mvt, pl, start_date):
    mvt_recep_mplc = mvt.filter((pl.col("lib_motif_mvt").is_in(["RECEPTION PRESTATAIRE", "RECEPTION ACHAT", "RECEPTION DOTATION CLIENT"])) 
                                & (pl.col("magasin") == "MPLC")
                                & (pl.col("sens_mvt") == "+") 
                                & (pl.col("date_mvt") >= start_date))
    mvt_recep_mplc = mvt_recep_mplc.join(items.select(pl.col("code_article", "lieu_de_reparation_pim")), how="left", on="code_article")

    expression_3 = (
        pl.when(pl.col("lieu_de_reparation_pim").is_in(["FMHS", "KMHS", "MATT", "MMHS"]))
        .then(pl.lit("NON REPARABLE"))
        .otherwise(pl.lit("REPARABLE"))
    )

    mvt_recep_mplc = mvt_recep_mplc.with_columns(expression_3.alias("reparable"))

    expression_4 = (
        pl.when(pl.col("qualite") == "BLOQB")
        .then(pl.lit("BAD"))
        .otherwise(
            pl.when(pl.col("qualite") == "BLOQG")
            .then(pl.lit("GOOD"))
            .otherwise(pl.col("qualite"))
        )
    )

    mvt_recep_mplc = mvt_recep_mplc.with_columns(expression_4.alias("qualite_simple"))
    return (mvt_recep_mplc,)


@app.cell
def _(mvt_recep_mplc, pl):
    mvt_recep_mplc.group_by("qualite_simple", "reparable").agg(pl.col("qte_mvt").sum()).sort("qualite_simple").write_excel("tcd_proportion_reparable.xlsx")
    return


@app.cell
def _(items, mvt_recep_mplc, pl):
    mvt_recep_mplc_poids_volume = mvt_recep_mplc.join(items.select("code_article", "poids_article", "volume_article"), how="left", on="code_article")
    mvt_recep_mplc_poids_volume = mvt_recep_mplc_poids_volume.with_columns((pl.col("qte_mvt") * pl.col("poids_article")).alias("poids_total"))
    mvt_recep_mplc_poids_volume = mvt_recep_mplc_poids_volume.with_columns((pl.col("qte_mvt") * pl.col("volume_article")).alias("volume_total"))
    mvt_recep_mplc_poids_volume.write_excel("recep_mplc_poids_volume.xlsx")
    return


@app.cell
def _(mo):
    mo.Html("<H2>IDENTIFIER PASSAGE DE M en D")
    return


@app.cell
def _():
    code_projet_PJ564 = "PJ00000564"
    return (code_projet_PJ564,)


@app.cell
def _(mvt, pl):
    def get_mvt_n_serie(n_serie: str):
        return mvt.filter(pl.col("n_serie") == n_serie).sort(["date_mvt", "n_mvt"], descending=True)
    return (get_mvt_n_serie,)


@app.cell
def _(get_mvt_n_serie):
    mvt_n_serie = get_mvt_n_serie("112007")
    return (mvt_n_serie,)


@app.cell
def _(mvt_n_serie):
    mvt_n_serie
    return


@app.cell
def _(ancien_code_projet, mvt_n_serie, pl):
    def get_changement_m_to_d(mvt_dataframe):
        info = {"code_article": None, "n_lot": None, "n_serie": None, "date_mvt": None, "ancien_code_projet": ancien_code_projet, "code_projet": None}
        # Y a-t-il eu un mvt en déploiement?
        set_flag_m_d = mvt_dataframe.select(pl.col("flag_stock_m_d_bis")).unique().to_series()
        if "D" in set_flag_m_d and "M" in set_flag_m_d:
            # A quelle date a eu lieu le changement de M en D?
            for i, row in enumerate(mvt_n_serie.iter_rows(named=True)):
                if row["flag_stock_m_d_bis"] == "M":
                    date_changement = row["date_mvt"]
                    print(i, date_changement)
                    ancien_code_projet = row["code_projet"]
                    code_projet = mvt_dataframe.row(i - 1, named=True)["code_projet"]
                    code_article = row["code_article"]
                    n_lot = row["n_lot"]
                    n_serie = row["n_serie"]
                    info = {"code_article": code_article, "n_lot": n_lot, "n_serie": n_serie, "date_mvt": date_changement, "code_projet": code_projet}
                    break
        return info
    return (get_changement_m_to_d,)


@app.cell
def _(get_changement_m_to_d, mvt_n_serie):
    get_changement_m_to_d(mvt_n_serie)
    return


@app.cell
def _(helios):
    helios
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
