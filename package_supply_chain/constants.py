import os

STORES_CLOSED = ["1301", "3301", "8803", "4102", "1316"]
UPSTREAM_STORE_TYPES = ["FOURNISSEURS", "NATIONAL", "RESERVE", "LOCAL", "PIED DE SITE", 
                        "REPARATEUR INTERNE", "REPARATEUR EXTERNE", "DTOM", "DIVERS"]
UPSTREAM_STORES = ["MBZL", "MBAL", "35BS"]

folder_path_output = r".\data_output"

folder_path_mvt_speed = r".\data_input\MVT\SPEED"
folder_path_mvt_oracle = r".\data_input\MVT\ORACLE"

folder_backup_address_gps = r"./databases/database_gps_address"
file_name_database_address_gps = "database_gps_address.db"

folder_database_application = r"./databases/database_application"
file_name_database_application = "database_application.db"
folder_original_file = r".\test\original_files"
folder_test_output = r".\test\output"

folder_path_exit = r"\\apps\Vol1\Data\011-BO_XI_entrees\07-DOR_DP\Sorties"
folder_path_input = r".\data_input"

folder_input_photo = r"\\apps\Vol1\Data\35-Recherche_Stock\Photos"
folder_database_photo = r"./databases/database_photo"
file_name_database_photo = r"database_photo.db"

# referential items
file_name_521 = "521 - (PIM) - REFERENTIEL ARTICLES V2.xlsx"
sheet_names_521 = ["ARTICLES PIM", "ARTICLES PIM - TRANSPORT", "EQUIVALENT", "FABRICANT"]

# nomenclature items
file_name_531 = "531 - Nomenclature Equipement.xlsx"
sheet_name_531 = "Nomenclature Fils"

# projects
file_name_551 = "551 - (RAP SIP) - PROJET PE.xlsx"
file_name_552 = "552 - (RAP SIP) - PROJET PJ.xlsx"

# Min mMax stores
file_name_532 = "532 - (SPD TPS REEL) - MIN MAX MAGASINS.xlsx"

# referential Helios
file_name_515 = "515 - (IG) - SITES IG HELIOS.xlsx"
file_name_560 = "560 - (HELIOS) - IG PS SLA CCO EQUIP V2.xlsx"
sheet_name_560 = "IG_PNO_EQUIP"
sheet_name_560_pop = "POP"
sheet_name_560_pno = "PNO"
sheet_name_560_sla = "SLA"
sheet_name_560_cco = "CCO"

# Referential stores and pudo
file_name_545 = "545 - (STK TIERS SIP) - ANNUAIRE MAGASINS ET POINT RELAIS.xlsx"
sheet_name_545_stores = "LISTE MAG PR"
sheet_name_545_pudo = "LISTE PR"

# Stock real time
file_name_554 = "554 - (STK SPD TPS REEL) - STOCK TEMPS REEL.xlsx"

# Stock min max
file_name_532 = "532 - (SPD TPS REEL) - MIN MAX MAGASINS.xlsx"

# Movements stock
file_name_510 = "510 - (STK SPD TPS REEL) - MOUVEMENT STOCK.csv"

# Transco file store code oracle speed
file_name_file_transco_store_code = "TRANSCO MAG AU 20200722.xlsx"

# bu repartition
file_name_bu_sheet_distribution = "cle_repartition_bu_v2.xlsx"

# project bu repartion 
file_name_bu_project_distribution = "transco_libelle_programme_pe_v2.xlsx"

# Correspondence between movement label oracle and speed
file_name_correspondence_mvt_label_oracle_speed = "Liste des transcodifications des mouvements.xlsx"

# Correspondence between store code oracle and store code speed
file_name_correspondence_store_code_oracle_speed = "TRANSCO MAG AU 20200722.xlsx"

# Bt
file_name_559 = "559 - BT.csv"

# DPI
file_name_572 = "572 - (BOOST DPI TPS REEL) - PORTEFEUILLE DPI.xlsx"
sheet_name_572 =  "dde pce + dde liv ou transfert"

# Correspondance user_dpi
file_name_correspondance_user_dpi = "correspondance_user_cdp_deploiement.xlsx"

# historique stock
file_name_500 = "500 - (STK SPD SIP) - ETAT DES STOCKS (POWERBI).csv"

folder_path_exit_parquet = os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "FICHIERS_PARQUET")