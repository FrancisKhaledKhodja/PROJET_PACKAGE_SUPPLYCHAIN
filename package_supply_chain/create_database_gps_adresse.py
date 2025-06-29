import os
import sys

sys.path.append(os.getcwd())

from sqlmodel import create_engine, Session, select
from package_supply_chain.my_loguru import logger
from package_supply_chain.api_address_gps import (
    get_cleaning_address, 
    get_latitude_and_longitude
    )
from package_supply_chain.models import Store, AddressGps, Pudo
from package_supply_chain.constants import (
    folder_database_application, 
    file_name_database_application, 
    folder_backup_address_gps, 
    file_name_database_address_gps
    )


def create_database_gps_address() -> None:
    logger.info("Création de la base de données des adresses gps si elle n'existe pas.")
    path_gps_address_db = f"{folder_backup_address_gps}/{file_name_database_address_gps}"
    if not os.path.exists(path_gps_address_db):
        engine = create_engine(f"sqlite:///{path_gps_address_db}")
        AddressGps.__table__.create(engine)
    logger.info("La base de données des adresses de leurs coordonnées gps existe")



def get_gps_adresses_from_store() -> list[dict]:
    logger.info("Début pour la récupération des coordonnées GPS manquants auprès de data.gouv")

    db_path_gps_address = f"sqlite:///{folder_backup_address_gps}/{file_name_database_address_gps}"
    engine_gps = create_engine(db_path_gps_address)
    with Session(engine_gps) as session:
        statement = select(AddressGps)
        results = session.exec(statement).all()
        list_adresses_from_db_gps_address = [row.model_dump()["adresse"] for row in results]
        logger.info(f"{len(list_adresses_from_db_gps_address)} adresses avec coordonnées GPS")
     

    db_path_application_url = f"sqlite:///{folder_database_application}/{file_name_database_application}"
    engine_application = create_engine(db_path_application_url, echo=False)
    list_results_gps_addresses = []
    fail_count = 0
    with Session(engine_application) as session_app:
        statement = select(Store)
        results = session_app.exec(statement)
        for i, row in enumerate(results):
            row_json = row.model_dump()
            if row_json["statut"] == 0:
                cleaned_address = get_cleaning_address(
                    row_json["adresse_1"], 
                    row_json["adresse_2"], 
                    row_json["code_postal"], 
                    row_json["ville"]
                    )
                if cleaned_address not in list_adresses_from_db_gps_address:
                    response = get_latitude_and_longitude(cleaned_address)
                    if response["latitude"] is not None:
                        list_results_gps_addresses.append(response)
                        fail_count += 1
    logger.info(f"{i} adresses interrogées auprés de data.gouv avec {fail_count} échec (absence de coordonnées GPS).")
    return list_results_gps_addresses


def load_gps_adresses_from_store_in_db(list_gps_addresses: list[dict]) -> None:
    logger.info("Ajout des coordonnées GPS manquantes dans la base de données.")
    db_path_gps_address = f"sqlite:///{folder_backup_address_gps}/{file_name_database_address_gps}"
    engine = create_engine(db_path_gps_address)
    count = 0
    with Session(engine) as session:
        for row in list_gps_addresses:
            label_address = row["address"]
            statement = select(AddressGps).where(AddressGps.adresse == label_address)
            result = session.exec(statement).first()
            if result is None:
                address_gps = AddressGps(
                    adresse=row["address"],
                    latitude=row["latitude"], 
                    longitude=row["longitude"]
                    )
                session.add(address_gps)
                count += 1
        session.commit()
    logger.info(f"{count} coordonnées GPS ajoutées dans la base de données.")


def get_gps_adresses_from_pudo() -> list[dict]:
    logger.info("Début pour la récupération des coordonnées GPS manquants auprès de data.gouv")

    db_path_gps_address = f"sqlite:///{folder_backup_address_gps}/{file_name_database_address_gps}"
    engine_gps = create_engine(db_path_gps_address)
    with Session(engine_gps) as session:
        statement = select(AddressGps)
        results = session.exec(statement).all()
        list_adresses_from_db_gps_address = [row.model_dump()["adresse"] for row in results]
        logger.info(f"{len(list_adresses_from_db_gps_address)} adresses avec coordonnées GPS")
     

    db_path_application_url = f"sqlite:///{folder_database_application}/{file_name_database_application}"
    engine_application = create_engine(db_path_application_url, echo=False)
    list_results_gps_addresses = []
    fail_count = 0
    with Session(engine_application) as session_app:
        statement = select(Pudo)
        results = session_app.exec(statement)
        for i, row in enumerate(results):
            row_json = row.model_dump()
            
            cleaned_address = get_cleaning_address(
                row_json["adresse_1"], 
                row_json["adresse_2"], 
                row_json["adresse_3"],
                row_json["code_postal"], 
                row_json["ville"]
                )
            if cleaned_address not in list_adresses_from_db_gps_address:
                response = get_latitude_and_longitude(cleaned_address)
                if response["latitude"] is not None:
                    list_results_gps_addresses.append(response)
                    fail_count += 1
    logger.info(f"{i} adresses interrogées auprés de data.gouv avec {fail_count} échec (absence de coordonnées GPS).")
    return list_results_gps_addresses




if __name__ == "__main__":
    create_database_gps_address()
    list_results_gps_addresses = get_gps_adresses_from_store()
    load_gps_adresses_from_store_in_db(list_results_gps_addresses)
    list_results_gps_addresses = get_gps_adresses_from_pudo()
    load_gps_adresses_from_store_in_db(list_results_gps_addresses)

    