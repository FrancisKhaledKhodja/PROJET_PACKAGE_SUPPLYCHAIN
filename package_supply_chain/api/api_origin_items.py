import os
import polars as pl
from package_supply_chain.constants import folder_path_output
from package_supply_chain.miscellaneous_functions import get_execution_time
from package_supply_chain.my_loguru import logger



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


def get_origin_items():
    logger.info("Appel de l'api get_origin_items")
    last_folder =os.listdir(folder_path_output)[-1]
    mvt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))
    mvt = mvt.with_columns(pl.concat_str(pl.col("code_article", "n_lot", "n_serie"), separator="-").alias("cle_article"))
    mvt = mvt.sort(pl.col("cle_article", "date_mvt", "n_mvt"), descending=True)

    mvt_filtered = mvt.filter((~pl.col("lib_motif_mvt").is_in(ignored_movements)) 
                          & (pl.col("n_lot").is_not_null())
                          & (pl.col("n_serie").is_not_null()))
    
    results = mvt_filtered.group_by(pl.col("cle_article")).first().select(pl.col("cle_article", "date_mvt", "code_article", "n_lot", "n_serie", "lib_motif_mvt"))
    results = results.unique(pl.col("cle_article"))
    logger.info("Fin de l'appel de l'api get_origin_items")
    return results