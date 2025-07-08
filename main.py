import os
import shutil
import datetime as dt
from dotenv import load_dotenv

import polars as pl

from package_supply_chain.constants import *
from package_supply_chain.my_loguru import logger
from package_supply_chain.miscellaneous_functions import get_execution_time
from package_supply_chain.excel_csv_to_dataframe import read_excel, read_csv
from package_supply_chain.items import Items, Nomenclatures
from package_supply_chain.helios import Helios, HeliosEquip
from package_supply_chain.stock import LoadStock, StockHisto
from package_supply_chain.referential_stores import ReferentialStores
from package_supply_chain.stock_movement import MovementSpeed, MovementOracle
from package_supply_chain.bt import BT


from package_supply_chain.api.api_mvt import get_movements_oracle_and_speed, get_movements_oracle_intermediate
from package_supply_chain.api.api_stores import get_stores_reference
from package_supply_chain.api.api_stock import get_state_stocks
from package_supply_chain.api.api_stock_histo import get_stock_histo
from package_supply_chain.api.api_buildings import get_items_buildings
from package_supply_chain.api.api_inter_prod_conso_output_stat import get_inter_conso_output_statistics
from package_supply_chain.api.api_items_without_exit import get_items_without_exit
from package_supply_chain.api.api_priority_list_photo import get_priority_list_photo, get_list_items_photo
from package_supply_chain.api.api_analyze_m_to_d import get_items_m_to_d
from package_supply_chain.api.api_items_to_check_after_return_from_prod import get_items_to_check_after_return_from_prod
from package_supply_chain.api.api_items_with_return_good import get_items_with_return_good
from package_supply_chain.api.api_origin_items import get_origin_items



def create_new_foder_for_input():
    now = dt.datetime.now().strftime("%Y%m%d")
    for rep in (os.path.join(folder_path_input, "QUOTIDIEN", now), os.path.join(folder_path_output, now)):
        try:
            os.mkdir(rep)
        except FileExistsError:
            pass

    return now


def copy_excel_files_in_data_input(folder: str):
    folder_data_input_daily = os.path.join(folder_path_input, "QUOTIDIEN")
    if not os.path.exists(os.path.join(folder_data_input_daily, folder)):
        logger.info(f"Création du répertoire: {folder_data_input_daily}/{folder}")
        os.mkdir(os.path.join(folder_data_input_daily, folder))
    else:
        logger.info(f"Le répertoire existe déjà: {folder_data_input_daily}/{folder}")

    list_excel_files = (file_name_521, 
                        file_name_531, 
                        file_name_515, 
                        file_name_560, 
                        file_name_545, 
                        file_name_532, 
                        file_name_551, 
                        file_name_552, 
                        file_name_554, 
                        file_name_559, 
                        file_name_532, 
                        file_name_572, 
                        file_name_500)
    for file in list_excel_files:
        logger.info(f"Copie du fichier: {file}")
        shutil.copyfile(os.path.join(folder_path_exit, file), 
                        os.path.join(folder_data_input_daily, folder, file))
        
    for file in (file_name_bu_sheet_distribution, 
                 file_name_bu_project_distribution, 
                 file_name_correspondence_mvt_label_oracle_speed, 
                 file_name_correspondence_store_code_oracle_speed, 
                 file_name_correspondance_user_dpi):
        logger.info(f"Copie du fichier: {file}")
        shutil.copyfile(os.path.join(folder_path_input, "DIVERS", file), 
                        os.path.join(folder_data_input_daily, folder, file))


def copy_csv_file_movements():

    folder_name = dt.datetime.now().strftime("%Y")
    folder_path_mvt_speed = os.path.join(folder_path_input, "MVT", "SPEED")
    if not os.path.exists(os.path.join(folder_path_mvt_speed, folder_name)):
        logger.info(f"Création du répertoire: {folder_path_mvt_speed}/{folder_name}")
        os.mkdir(os.path.join(folder_path_mvt_speed, folder_name))
    else:
        logger.info(f"Le répertoire existe déjà: {folder_path_mvt_speed}/{folder_name}")

    logger.info(f"Copie du fichier: {file_name_510}")
    shutil.copyfile(os.path.join(folder_path_exit, file_name_510), 
                    os.path.join(folder_path_mvt_speed, folder_name, file_name_510))


def making_parquet_file(last_path_folder, now_output):
    # Correpondance user_dpi
    correspondance_user_dpi = read_excel(last_path_folder, file_name_correspondance_user_dpi)
    correspondance_user_dpi.write_parquet(os.path.join(now_output, "correspondance_user_dpi.parquet"))
    # BT
    bt = BT(last_path_folder, file_name_559)
    bt.df.write_parquet(os.path.join(now_output, "bt.parquet"))
    # ITEMS
    item_instance = Items(last_path_folder, file_name_521, sheet_names_521)
    item_instance._write_parquet(now_output)
    # Nomenclatures
    nomenclatures = Nomenclatures(last_path_folder, file_name_531, sheet_name_531)
    nomenclatures.df.write_parquet(os.path.join(now_output, "nomenclatures.parquet"))
    # HELIOS
    helios_instance = Helios(last_path_folder, file_name_515)
    helios_instance._write_parquet(now_output)
    helios_equipment_instance = HeliosEquip(last_path_folder, file_name_560, sheet_name_560)
    helios_equipment_instance._write_parquet(now_output)
    sla = read_excel(last_path_folder, file_name_560, sheet_name_560_sla)
    sla.write_parquet(os.path.join(now_output, "sla.parquet"))
    cco = read_excel(last_path_folder, file_name_560, sheet_name_560_cco)
    cco.write_parquet(os.path.join(now_output, "cco.parquet"))
    pno = read_excel(last_path_folder, file_name_560, sheet_name_560_pno)
    pno.write_parquet(os.path.join(now_output, "pno.parquet"))
    pop = read_excel(last_path_folder, file_name_560, sheet_name_560_pop)
    pop.write_parquet(os.path.join(now_output, "pop.parquet"))
    # PE
    pe = read_excel(last_path_folder,file_name_551)
    pe.write_parquet(os.path.join(now_output, "pe.parquet"))
    # PJ
    pj = read_excel(last_path_folder,file_name_552)
    pj.write_parquet(os.path.join(now_output, "pj.parquet"))
    # Stock real time
    stock = LoadStock(last_path_folder, file_name_554)
    stock._write_parquet(now_output)
    # STOCK ORACLE
    stock_oracle = read_excel(os.path.join(folder_path_input, "STOCK_ORACLE"), "STOCK ORACLE.xlsx")
    stock_oracle.write_parquet(os.path.join(now_output, "stock_oracle.parquet"))
    # Min Max
    minmax = read_excel(last_path_folder, file_name_532)
    minmax = minmax.rename({"code_magasin_ou_site": "code_magasin"})
    minmax.write_parquet(os.path.join(now_output, "minmax.parquet"))
    # Referential stores
    referential_stores = ReferentialStores(last_path_folder, file_name_545, sheet_name_545_stores)
    referential_stores._write_parquet(now_output)
    # SHEETS AND PROJECTS REPARTITION BU
    bu_sheet_repartition = read_excel(last_path_folder, file_name_bu_sheet_distribution)
    bu_sheet_repartition.write_parquet(os.path.join(now_output, "bu_sheet_repartition.parquet"))
    bu_project_distribution = read_excel(last_path_folder, file_name_bu_project_distribution)
    bu_project_distribution.write_parquet(os.path.join(now_output, "bu_project_repartition.parquet"))

    # DPI
    dpi = read_excel(folder_path_exit, file_name_572, sheet_name_572)
    dpi.write_parquet(os.path.join(now_output, "dpi.parquet"))

    # correspondence store code oracle and store code speed
    correspondence_store_code = read_excel(last_path_folder, file_name_correspondence_store_code_oracle_speed)
    correspondence_store_code.write_parquet(os.path.join(now_output, "correspondence_store_code_oracle_speed.parquet"))

    # Correspondence movement label oracle speed
    correspondence_mvt_label = read_excel(last_path_folder, file_name_correspondence_mvt_label_oracle_speed)
    correspondence_mvt_label.write_parquet(os.path.join(now_output, "correspondence_mvt_oracle_speed.parquet"))

    # stock historic compilation
    stock_histo_compil = pl.DataFrame()
    for folder in os.listdir(os.path.join(folder_path_input, "MENSUEL")):
        stock_histo = StockHisto(os.path.join(folder_path_input, "MENSUEL", folder), file_name_500)
        stock_histo_compil = pl.concat([stock_histo_compil, stock_histo.df])
    stock_histo_compil.write_parquet(os.path.join(now_output, "stock_histo_compil.parquet"))

    # speed movements compilation
    mvt_speed_compil = pl.DataFrame()
    for folder in os.listdir(folder_path_mvt_speed):
        for file in os.listdir(os.path.join(folder_path_mvt_speed, folder)):
            mvt_speed = MovementSpeed(os.path.join(folder_path_mvt_speed, folder), file)
            mvt_speed_compil = pl.concat([mvt_speed_compil, mvt_speed.df])

    mvt_speed_compil.write_parquet(os.path.join(now_output, "mvt_speed.parquet"))

    # oracle movements compilation
    mvt_oracle_compil = pl.DataFrame()
    for file in os.listdir(folder_path_mvt_oracle):
        mvt_oracle = MovementOracle(folder_path_mvt_oracle, file)
        mvt_oracle_compil = pl.concat([mvt_oracle_compil, mvt_oracle.df])

    mvt_oracle_compil.write_parquet(os.path.join(now_output, "mvt_oracle.parquet"))


def get_stores_references_and_edition(now_output):
    stores_references = get_stores_reference()
    stores_references.write_parquet(os.path.join(now_output, "stores_final.parquet"))
    stores_references.write_parquet(os.path.join(folder_path_exit_parquet, "stores_final.parquet"))
    stores_references.write_excel(os.path.join(".", "excel_files_output", "stores.xlsx"))

def get_mvt_oracle_speed_and_editions(now_output):
    mvt_oracle_and_speed = get_movements_oracle_and_speed()
    mvt_oracle_and_speed.write_parquet(os.path.join(now_output, "mvt_oracle_and_speed_final.parquet"))
    mvt_oracle_and_speed.write_parquet(os.path.join(folder_path_exit_parquet, "mvt_oracle_and_speed_final.parquet"))


def get_items_without_exit_and_editions(now_output):
    items_without_exit = get_items_without_exit()
    items_without_exit.write_parquet(os.path.join(now_output, "items_without_exit_final.parquet"))
    items_without_exit.write_parquet(os.path.join(folder_path_exit_parquet, "items_without_exit_final.parquet"))
    items_without_exit.write_excel(os.path.join(".", "excel_files_output", "items_without_exit.xlsx"))


def get_state_of_the_stock_and_editions(now_output):
    stock = get_state_stocks()
    stock.write_parquet(os.path.join(now_output, "stock_final.parquet"))
    stock.write_parquet(os.path.join(folder_path_exit_parquet, "stock_final.parquet"))
    stock.write_excel(os.path.join(".", "excel_files_output", "stock.xlsx"))


def get_histo_stock_and_editions(now_output):
    stock_histo = get_stock_histo()
    stock_histo.write_parquet(os.path.join(now_output, "stock_histo_compil_final.parquet"))
    stock_histo.write_parquet(os.path.join(folder_path_exit_parquet, "stock_histo_compil_final.parquet"))


def get_stats_breakdown_and_edition(now_output):
    stats_breakdowns = get_inter_conso_output_statistics()
    stats_breakdowns.write_parquet(os.path.join(now_output, "stats_breakdown.parquet"))
    stats_breakdowns.write_parquet(os.path.join(folder_path_exit_parquet, "stats_breakdown.parquet"))
    stats_breakdowns.write_excel(os.path.join(".", "excel_files_output", "stats_breakdown.xlsx"))


def get_items_buildings_and_editions(now_output):
    items_son, items_parent = get_items_buildings()
    items_son.write_parquet(os.path.join(now_output, "items_son_buildings.parquet"))
    items_son.write_parquet(os.path.join(folder_path_exit_parquet, "items_son_buildings.parquet"))
    items_parent.write_parquet(os.path.join(now_output, "items_parent_buildings.parquet"))
    items_parent.write_parquet(os.path.join(folder_path_exit_parquet, "items_parent_buildings.parquet"))
    items_son.write_excel(os.path.join(".", "excel_files_output", "items_son_buildings.xlsx"))
    items_parent.write_excel(os.path.join(".", "excel_files_output", "items_parent_buildings.xlsx"))

def get_list_photos_and_editions(now_output):
    list_items_photo, list_photos = get_list_items_photo()
    list_items_photo = pl.DataFrame({"code_article": list_items_photo})
    list_photos = pl.DataFrame({"nom_photo": list_photos})
    list_items_photo.write_parquet(os.path.join(now_output, "list_items_photo.parquet"))
    list_items_photo.write_parquet(os.path.join(folder_path_exit_parquet, "list_items_photo.parquet"))
    list_photos.write_parquet(os.path.join(now_output, "list_photos.parquet"))
    list_photos.write_parquet(os.path.join(folder_path_exit_parquet, "list_photos.parquet"))
    list_items_photo.write_excel(os.path.join(".", "excel_files_output", "list_items_photo.xlsx"))
    list_photos.write_excel(os.path.join(".", "excel_files_output", "list_photos.xlsx"))

def get_priority_list_photos_and_editions(now_output):
    priority_list_photo = get_priority_list_photo()
    priority_list_photo.write_parquet(os.path.join(now_output, "priority_list_photo.parquet"))
    priority_list_photo.write_parquet(os.path.join(folder_path_exit_parquet, "priority_list_photo.parquet"))
    priority_list_photo.write_excel(os.path.join(".", "excel_files_output", "priority_list_photo.xlsx"))

def get_items_m_to_d_and_editions(now_output):
    items_m_to_d = get_items_m_to_d()
    items_m_to_d.write_parquet(os.path.join(now_output, "items_m_to_d.parquet"))
    items_m_to_d.write_parquet(os.path.join(folder_path_exit_parquet, "items_m_to_d.parquet"))
    items_m_to_d.write_excel(os.path.join(".", "excel_files_output", "items_m_to_d.xlsx"))

def get_items_to_check_and_editions(now_output):
    items_to_check = get_items_to_check_after_return_from_prod()
    items_to_check.write_parquet(os.path.join(now_output, "items_to_check_after_return_from_prod.parquet"))
    items_to_check.write_parquet(os.path.join(folder_path_exit_parquet, "items_to_check_after_return_from_prod.parquet"))
    items_to_check.write_excel(os.path.join(".", "excel_files_output", "items_to_check_after_return_from_prod.xlsx"))

def get_items_with_good_return_and_editions(now_output):
    items_with_good_return = get_items_with_return_good()
    items_with_good_return.write_parquet(os.path.join(now_output, "items_with_good_return.parquet"))
    items_with_good_return.write_parquet(os.path.join(folder_path_exit_parquet, "items_with_good_return.parquet"))
    items_with_good_return.write_excel(os.path.join(".", "excel_files_output", "items_with_good_return.xlsx"))


def get_origin_items_and_editions(now_output):
    origin_items = get_origin_items()
    origin_items.write_parquet(os.path.join(now_output, "origin_items.parquet"))
    origin_items.write_parquet(os.path.join(folder_path_exit_parquet, "origin_items.parquet"))
    origin_items.write_excel(os.path.join(".", "excel_files_output", "origin_items.xlsx"))




@get_execution_time
def main(copy_file=True):
    now = create_new_foder_for_input()
    now_output = os.path.join(folder_path_output, now)
    if copy_file:
        # Making folder monthly
        folder_year_month = dt.datetime.today().strftime("%Y%m")
        try:
            os.makedirs(os.path.join(folder_path_input, "MENSUEL", folder_year_month))
        except FileExistsError:
            pass
        # Copy the file 500
        shutil.copyfile(os.path.join(folder_path_exit, file_name_500), 
                        os.path.join(folder_path_input, "MENSUEL", folder_year_month, file_name_500))
        
        
        copy_excel_files_in_data_input(now)
        copy_csv_file_movements()
        last_folder = sorted(os.listdir(os.path.join(folder_path_input, "QUOTIDIEN")), reverse=True)[0]
        last_path_folder = os.path.join(folder_path_input, "QUOTIDIEN", last_folder)
        
        making_parquet_file(last_path_folder, now_output)
   
    shutil.copyfile(os.path.join(now_output, "items.parquet"), 
                    os.path.join(folder_path_exit_parquet, "items.parquet"))
    
    shutil.copyfile(os.path.join(now_output, "bt.parquet"), 
                    os.path.join(folder_path_exit_parquet, "bt.parquet"))

    # Making stores references
    get_stores_references_and_edition(now_output)
    
    # Compilation oracle and speed movements
    get_mvt_oracle_speed_and_editions(now_output)

    # Making items without exit
    get_items_without_exit_and_editions(now_output)
    
    # Making the state of the stocks
    now = dt.datetime.today().date().strftime("%Y%m%d")
    get_state_of_the_stock_and_editions(now_output)

    # Making the stock histo
    get_histo_stock_and_editions(now_output)

    # Get stats breakdowns
    get_stats_breakdown_and_edition(now_output)

    # Get items per buildings
    get_items_buildings_and_editions(now_output)    
    
    # Get list photo
    get_list_photos_and_editions(now_output)
    
    # Gest priority list photo
    get_priority_list_photos_and_editions(now_output)
    
    # Get list items from stock m to d
    get_items_m_to_d_and_editions(now_output)
    
    # Get items to check after return from prod
    get_items_to_check_and_editions(now_output)
    
    # Get items with good return
    get_items_with_good_return_and_editions(now_output)

    get_origin_items_and_editions(now_output)
    

    shutil.copyfile(os.path.join(".", "excel_files_output", "stores.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "annuaire_magasins.xlsx"))

    shutil.copyfile(os.path.join(".", "excel_files_output", "items_without_exit.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "liste_articles_sans_sorties.xlsx"))

    shutil.copyfile(os.path.join(".", "excel_files_output", "stock.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", f"stock_{now}.xlsx"))

    shutil.copyfile(os.path.join(".", "excel_files_output", "stats_breakdown.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "stats_sorties_inter_conso.xlsx"))

    shutil.copyfile(os.path.join(".", "excel_files_output", "items_son_buildings.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "liste_articles_fils_dans_parc_helios.xlsx"))
    shutil.copyfile(os.path.join(".", "excel_files_output", "items_parent_buildings.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "liste_articles_parents_et_fils_dans_parc_helios.xlsx"))

    shutil.copyfile(os.path.join(".", "excel_files_output", "list_items_photo.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "liste_articles_avec_photos_disponibles.xlsx"))

    shutil.copyfile(os.path.join(".", "excel_files_output", "list_photos.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "liste_photos_disponibles.xlsx"))

    shutil.copyfile(os.path.join(".", "excel_files_output", "priority_list_photo.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "liste_articles_des_photos_a_realiser.xlsx"))

    shutil.copyfile(os.path.join(".", "excel_files_output", "items_m_to_d.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "histo_liste_art_de_m_vers_d.xlsx"))

    shutil.copyfile(os.path.join(".", "excel_files_output", "items_to_check_after_return_from_prod.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "liste_articles_en_retour_prod_sans_rep.xlsx"))

    shutil.copyfile(os.path.join(".", "excel_files_output", "items_with_good_return.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "liste_articles_en_retour_good_non_utilise_par_tech.xlsx"))
    
    shutil.copyfile(os.path.join(".", "excel_files_output", "origin_items.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "liste_articles_et_origine.xlsx"))


if __name__ == "__main__":
    main()

