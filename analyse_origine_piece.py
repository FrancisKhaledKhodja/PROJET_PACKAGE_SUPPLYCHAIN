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
    ignored_movements = ["CHANGEMENT CODE QUALITE", "RECEPTION PRESTATAIRE", "TRANSFERT RETOUR PRESTATAIRE", 
                        "REMISE DESTINATAIRE SUITE RAMASSE", "RAMASSE POINT RELAIS", "DEPOSE POINT RELAIS", 
                        "REMISE DESTINATAIRE", "RECEPTION MAGASIN", "REMISE POINT RELAIS", 
                        "TRANSFERT OL VERS DESTINATAIRE", "ENVOI ORDRE DE LIVRAISON", 
                        "RECEPTION PRESTATAIRE", "CHANGEMENT UMS", "RESERVATION-DERESERVATION", 
                        "TRANSFERT EMPLACEMENT", "TRANSFERT TECHNICIEN AUTOMATIQUE", 
                        "TRANSFERT TECHNICIEN", "TRANSFERT MAGASIN PIED DE SITE", "TRANSFERT MAGASIN", 
                        "RETOUR REPARATION", "RETOUR AUTO PR VERS PRESTATAIRE", "FIN TRAITEMENT RMA", 
                        "ENVOI REPARATION", "ENVOI EN MODE DEGRADE", "DEBUT TRAITEMENT RMA", 
                        "CHANGEMENT PROJET", "ECLATEMENT DE SUPPORT", "SORTIE PRODUCTION", 
                         "CHGT REF FABRICANT NS", 'CHGT REF FABRIQUANT NS', 'CHANGEMENT CODE ARTICLE',
                         "ENTREE BASCULE", "CHANGEMENT CODE QUALITE", "TRZ4 - Arrivée recomplétement", 
                         "TRZ3 - Envoi recomplétement", "Prélèvement commande interne", 'ENVOI PRET',
                         "Transfert de projet", "SORTIE MISE HS", "SORTIE INTERVENTION", "M_TRZ4 - Arrivée recompléteme",
                         "SORTIE CONSOMMATION", "ENTREE LITIGE", "SORTIE REVENTE", "SORTIE AJUSTEMENT STOCK", "M_ECR7 - Transfert divers"]
    return (ignored_movements,)


@app.cell
def _(folder_path_output, os):
    last_folder = os.listdir(folder_path_output)[-1]
    last_folder
    return (last_folder,)


@app.cell
def _(folder_path_output, last_folder, os, pl):
    mvt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))
    mvt = mvt.with_columns(pl.concat_str(pl.col("code_article", "n_lot", "n_serie"), separator="-").alias("cle_article"))
    mvt = mvt.sort(pl.col("cle_article", "date_mvt", "n_mvt"), descending=True)
    stock = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stock_final.parquet"))
    stock = stock.with_columns(pl.concat_str(pl.col("code_article", "n_lot", "n_serie"), separator="-").alias("cle_article"))
    list_items_to_analyze = stock.filter((pl.col("n_lot").is_not_null()) & 
                         (pl.col("n_serie").is_not_null()) 
                         & (pl.col("code_magasin").is_not_null())).select(pl.col("cle_article")).unique().to_series().to_list()
    return (mvt,)


@app.cell
def _(ignored_movements, mvt, pl):
    mvt_filtered = mvt.filter((~pl.col("lib_motif_mvt").is_in(ignored_movements)) 
                              & (pl.col("n_lot").is_not_null())
                              & (pl.col("n_serie").is_not_null()))
    return (mvt_filtered,)


@app.cell
def _(mvt_filtered, pl):
    results = mvt_filtered.group_by(pl.col("cle_article")).first().select(pl.col("cle_article", "date_mvt", "code_article", "n_lot", "n_serie", "lib_motif_mvt"))
    return (results,)


@app.cell
def _(results):
    results
    return


@app.cell
def _(pl, results):
    results.filter(pl.col("n_serie") == "L4991347570").select(pl.col("lib_motif_mvt"))
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
