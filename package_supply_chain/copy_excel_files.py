import shutil
import os
import datetime as dt
from package_supply_chain.my_loguru import logger
from package_supply_chain.constants import folder_path_exit, file_name_510
from package_supply_chain.miscellaneous_functions import get_execution_time

@get_execution_time
def excel_file_recovery(folder_path_input: str, list_name_files: list[str]) -> None:
    for file in list_name_files:
        logger.info(f"Recovery file: {file}")
        shutil.copyfile(
            os.path.join(folder_path_exit, file), 
            os.path.join(folder_path_input, file)
            )



def csv_file_movements_recovery(folder_path_input: str, file_name: str) -> None:
    logger.info(f"Recovery file: {file_name}")
    year = dt.datetime.now().year

    if not os.path.exists(os.path.join(folder_path_input, "MVT", str(year))):
        os.makedirs(os.path.join(folder_path_input, "MVT", str(year)))
    
    shutil.copyfile(os.path.join(folder_path_exit, file_name), os.path.join(folder_path_input, "MVT", str(year), file_name))