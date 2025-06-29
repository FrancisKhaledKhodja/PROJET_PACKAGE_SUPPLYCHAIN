import os
import shutil
import sys
import datetime as dt
from dotenv import load_dotenv
from sqlmodel import create_engine
import polars as pl

sys.path.append(os.getcwd())

from package_supply_chain.copy_excel_files import excel_file_recovery, csv_file_movements_recovery
from package_supply_chain.excel_csv_to_dataframe import read_excel
from package_supply_chain.constants import (folder_path_input, 
                                            folder_path_exit,
                                            file_name_521, 
                                            file_name_531, 
                                            sheet_name_531, 
                                            sheet_names_521, 
                                            file_name_515,
                                            file_name_560,
                                            sheet_name_560,
                                            folder_database_application, 
                                            file_name_database_application,
                                            sheet_name_560_cco, 
                                            sheet_name_560_pop, 
                                            sheet_name_560_pno, 
                                            sheet_name_560_sla, 
                                            file_name_545, 
                                            sheet_name_545_stores, 
                                            sheet_name_545_pudo, 
                                            file_name_532, 
                                            file_name_551, 
                                            file_name_552, 
                                            file_name_554, 
                                            file_name_510, 
                                            name_file_transco_store_code)
from package_supply_chain.stock_movement import LoadMovements
from package_supply_chain.stock import LoadStock
from package_supply_chain.minmax import MinMax
from package_supply_chain.items import Items, Nomenclatures
from package_supply_chain.helios import Helios, HeliosEquip
from package_supply_chain.referential_stores import ReferentialStores
from package_supply_chain.my_loguru import logger
from package_supply_chain.models import (
    Item, 
    HeliosIG, 
    HeliosIGEquip, 
    Store, 
    Cco, 
    Sla, 
    Pno, 
    Pop, 
    Nomenclature, 
    EquivalentItem, 
    ManufacturerItem, 
    Pudo, 
    Min_Max, 
    PE, 
    PJ,
    Stock, 
    Movement, 
    TranscoStoreOracleSpeed
)
from package_supply_chain.import_to_db_functions import (
                                             import_data_equivalent, 
                                             import_data_items, 
                                             import_data_manufacturer, 
                                             import_data_nomenclature, 
                                             import_data_ig, 
                                             import_data_ig_equipement, 
                                             import_data_pop, 
                                             import_data_pno, 
                                             import_data_sla, 
                                             import_data_cco, 
                                             import_data_referential_stores, 
                                             import_data_pudo, 
                                             import_data_minmax, 
                                             import_data_pe, 
                                             import_data_pj, 
                                             import_data_stock, 
                                             import_data_mvt, 
                                             import_data_transco_code_mag_oracle_speed
                                             )
def create_database_sqlite():
    
    # Crée le répertoire s'il n'existe pas
    if not(os.path.exists(folder_database_application)):
        os.mkdir(folder_database_application)
        logger.info(f"Dossier créé : {folder_database_application}")
    
    # Crée l'URL de connexion
    db_path = os.path.join(folder_database_application, file_name_database_application)
    logger.info(f"Création de la base de données à : {db_path}")
    database_url = f"sqlite:///{db_path}"
    logger.info(f"URL de connexion : {database_url}")
    
    # Crée le moteur de base de données
    engine = create_engine(database_url, echo=False)
    
    # Crée les tables
    logger.info("Création des tables...")
    Item.__table__.create(engine)
    HeliosIG.__table__.create(engine)
    HeliosIGEquip.__table__.create(engine)
    Store.__table__.create(engine)
    Cco.__table__.create(engine)
    Sla.__table__.create(engine)
    Pno.__table__.create(engine)
    Pop.__table__.create(engine)
    Nomenclature.__table__.create(engine)
    EquivalentItem.__table__.create(engine)
    ManufacturerItem.__table__.create(engine)
    Pudo.__table__.create(engine)
    Min_Max.__table__.create(engine)
    PE.__table__.create(engine)
    PJ.__table__.create(engine)
    Stock.__table__.create(engine)
    Movement.__table__.create(engine)
    TranscoStoreOracleSpeed.__table__.create(engine)
    logger.info("Tables créées avec succès")
    
    return engine



def main():

    # Remove the old database of the application
    if os.path.exists(folder_database_application):
        shutil.rmtree(folder_database_application)
        logger.info("Le répertoire database_sqlite supprimé avec succés")


    # Copy the file movement
    csv_file_movements_recovery(folder_path_input, file_name_510)

    # load the file for the new data in the database of the application
    excel_file_recovery(folder_path_input, [file_name_521, 
                                            file_name_531, 
                                            file_name_515, 
                                            file_name_560, 
                                            file_name_545, 
                                            file_name_532, 
                                            file_name_551, 
                                            file_name_552, 
                                            file_name_554])
    
    # Create the database
    engine = create_database_sqlite()

    # import from excel and load data in database

    transco_store_code = read_excel(os.path.join(folder_path_input, "DIVERS"), name_file_transco_store_code)
    import_data_transco_code_mag_oracle_speed(engine, transco_store_code)


    # mvt_df_all_years = pl.DataFrame()
    # for rep_mvt in os.listdir(os.path.join(folder_path_input, "MVT")):
    #     mvt_df = LoadMovements(os.path.join(folder_path_input, "MVT", rep_mvt), file_name_510).df
    #     mvt_df_all_years = pl.concat([mvt_df_all_years, mvt_df])
    # import_data_mvt(engine, mvt_df_all_years)

    stock = LoadStock(folder_path_input, file_name_554)
    import_data_stock(engine, stock.df)

    pe = read_excel(folder_path_input, file_name_551)
    import_data_pe(engine, pe)

    pj = read_excel(folder_path_input, file_name_552)
    import_data_pj(engine, pj)

    items = Items(folder_path_input, file_name_521, sheet_names_521)
    import_data_items(engine, items.items_df)
    import_data_manufacturer(engine, items.manufacturer_df)
    import_data_equivalent(engine, items.equivalent_df)

    nomenclatures = Nomenclatures(folder_path_input, file_name_531, sheet_name_531)
    import_data_nomenclature(engine, nomenclatures.df)

    helios = Helios(folder_path_input, file_name_515)
    import_data_ig(engine, helios.df)

    helios_equipement = HeliosEquip(folder_path_input, file_name_560, sheet_name_560)
    import_data_ig_equipement(engine, helios_equipement.df)

    pop = read_excel(folder_path_input, file_name_560, sheet_name_560_pop)
    import_data_pop(engine, pop)

    pno = read_excel(folder_path_input, file_name_560, sheet_name_560_pno)
    import_data_pno(engine, pno)

    cco = read_excel(folder_path_input, file_name_560, sheet_name_560_cco)
    import_data_cco(engine, cco)

    sla = read_excel(folder_path_input, file_name_560, sheet_name_560_sla)
    import_data_sla(engine, sla)

    referential_stores = ReferentialStores(folder_path_input, file_name_545, sheet_name_545_stores)
    import_data_referential_stores(engine, referential_stores.df)

    referentiel_pudos = read_excel(folder_path_input, file_name_545, sheet_name_545_pudo)
    import_data_pudo(engine, referentiel_pudos)

    min_max = MinMax(folder_path_input, file_name_532)
    import_data_minmax(engine, min_max.df)

       
    
    logger.info("Base de données créée et données importées avec succès")
        


if __name__ == "__main__":
    load_dotenv()
    main()
