import os
import shutil
import sys
from sqlmodel import SQLModel, create_engine

sys.path.append(os.getcwd())

from package_supply_chain.my_loguru import logger
from package_supply_chain.import_to_db_functions import import_data_image
from package_supply_chain.models import Image
from package_supply_chain.constants import (folder_input_photo, 
                                            folder_database_photo,
                                            file_name_database_photo)


def create_database_photo():

    if os.path.exists(folder_database_photo):
        shutil.rmtree(folder_database_photo)
        logger.info(f"Le répertoire {folder_database_photo} supprimé avec succés")

    if not(os.path.exists(folder_database_photo)):
        os.mkdir(folder_database_photo)
        logger.info(f"Dossier créé : {folder_database_photo}")
    db_photo_url = f"sqlite:///{os.path.join(folder_database_photo, file_name_database_photo)}"
    logger.info(f"URL de connexion : {db_photo_url}")
    logger.info(f"Création de la base de données à : {db_photo_url}")
    engine_photo = create_engine(db_photo_url, echo=False)
    logger.info("Création de la table...")
    Image.__table__.create(engine_photo)
    logger.info("Table créée avec succès")
    import_data_image(engine_photo, folder_input_photo)


if __name__ == "__main__":
    create_database_photo()
    
    