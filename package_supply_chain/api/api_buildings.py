
import os
import polars as pl

from package_supply_chain.constants import folder_path_output
from package_supply_chain.items import Nomenclatures
from package_supply_chain.my_loguru import logger


def get_items_buildings():
    logger.info("Appel de l'api get_items_buildings")
    last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0]
    helios_equipement = pl.read_parquet(os.path.join(folder_path_output, last_folder, "helios_equipement.parquet"))
    nomenclatures = Nomenclatures(os.path.join(folder_path_output, last_folder), "nomenclatures.parquet")
    items = pl.read_parquet(os.path.join(folder_path_output, last_folder, "items.parquet"))
    helios = pl.read_parquet(os.path.join(folder_path_output, last_folder, "helios.parquet"))

    list_code_ig = []
    list_code_art_pere = []
    list_qte_pere_actif = []
    list_qte_pere_inactif = []
    list_code_art_fils = []
    list_qte_fils_actif = []
    list_qte_fils_inactif = []

    for row in helios_equipement.iter_rows(named=True):
        #if row["actif"]:
        code_ig = row["code_ig"]
        code_article_pere = row["code_article"]
        quantite_pere_actif = float(row["nb_sites_active"]) + float(row["nb_sites_en_cours_installation"]) + float(row["nb_sites_en_cours_modification"])
        quantite_pere_inactif = float(row["nb_sites_en_cours_desinstallation"]) + float(row["nb_sites_demontes"]) + float(row["nb_sites_desactivites"])
        resp = nomenclatures.get_item_nomenclature(code_article_pere)
        list_code_ig.append(code_ig)
        list_code_art_pere.append(code_article_pere)
        list_qte_pere_actif.append(quantite_pere_actif)
        list_qte_pere_inactif.append(quantite_pere_inactif)
        list_code_art_fils.append(code_article_pere)
        list_qte_fils_actif.append(quantite_pere_actif)
        list_qte_fils_inactif.append(quantite_pere_inactif)

        for row_fils in resp["code_article_fils"]:
            list_code_ig.append(code_ig)
            list_code_art_pere.append(code_article_pere)
            list_qte_pere_actif.append(0)
            list_qte_pere_inactif.append(0)
            list_code_art_fils.append(row_fils["code_article"])
            list_qte_fils_actif.append(row_fils["quantite"] * quantite_pere_actif)
            list_qte_fils_inactif.append(row_fils["quantite"] * quantite_pere_inactif)


    df_ig_art_pere_art_fils = pl.DataFrame({"code_ig": list_code_ig, 
                                           "code_article_pere": list_code_art_pere, 
                                           "quantite_pere_actif": list_qte_pere_actif, 
                                           "quantite_pere_inactif": list_qte_pere_inactif,
                                           "code_article_fils": list_code_art_fils, 
                                           "quantite_fils_actif": list_qte_fils_actif, 
                                           "quantite_fils_inactif": list_qte_fils_inactif})
    

    df_ig_art_pere_art_fils = df_ig_art_pere_art_fils.join(items.select(pl.col("code_article", "libelle_court_article")), 
                                                           how="left", 
                                                           left_on="code_article_pere", 
                                                           right_on="code_article")
    
    df_ig_art_pere_art_fils = df_ig_art_pere_art_fils.join(items.select(pl.col("code_article", "libelle_court_article")), 
                                                           how="left", 
                                                           left_on="code_article_fils", 
                                                           right_on="code_article")
    
    df_ig_art_pere_art_fils = df_ig_art_pere_art_fils.join(helios.select("code_ig", "libelle_long_ig"), how="left", on="code_ig")

    df_ig_art_pere_art_fils = df_ig_art_pere_art_fils.rename({"libelle_court_article": "libelle_court_article_pere", 
                                                              "libelle_court_article_right": "libelle_court_article_fils"})
    
    df_ig_art_pere_art_fils = df_ig_art_pere_art_fils.select(["code_ig", "libelle_long_ig", "code_article_pere", 
                                                              "libelle_court_article_pere", "quantite_pere_actif", "quantite_pere_inactif", 
                                                              "code_article_fils", "libelle_court_article_fils", 
                                                              "quantite_fils_actif", "quantite_fils_inactif"])
    
    

    df_art_fils = df_ig_art_pere_art_fils.group_by(pl.col("code_article_fils")).agg(pl.col("quantite_fils_actif").sum(), 
                                                                                    pl.col("quantite_fils_inactif").sum(), pl.col("code_ig").n_unique())
    
    df_art_fils = df_art_fils.join(items.select(pl.col("code_article", "libelle_court_article")), how="left", left_on="code_article_fils", right_on="code_article")

    df_art_fils = df_art_fils.select(["code_article_fils", "libelle_court_article", "quantite_fils_actif", "quantite_fils_inactif", "code_ig"])

    logger.info("Fin de l'appel de l'api get_items_buildings")

    return df_art_fils, df_ig_art_pere_art_fils

