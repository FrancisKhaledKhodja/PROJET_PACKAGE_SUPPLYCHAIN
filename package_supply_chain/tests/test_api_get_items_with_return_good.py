import os
import shutil

from package_supply_chain.constants import folder_path_output, folder_path_exit_parquet, folder_path_exit
from package_supply_chain.api.api_items_with_return_good import get_items_with_return_good


def test_get_items_with_return_good():
    last_folder = os.listdir(folder_path_output)[-1]
    items_with_good_return = get_items_with_return_good()
    items_with_good_return.write_parquet(os.path.join(folder_path_output, last_folder, "items_with_good_return.parquet"))
    items_with_good_return.write_parquet(os.path.join(folder_path_exit_parquet, "items_with_good_return.parquet"))
    items_with_good_return.write_excel(os.path.join(".", "excel_files_output", "items_with_good_return.xlsx"))

    shutil.copyfile(os.path.join(".", "excel_files_output", "items_with_good_return.xlsx"),
                    os.path.join(folder_path_exit, "FICHIERS_ANALYSES_SUPPLY_CHAIN", "liste_articles_en_retour_good_non_utilise_par_tech.xlsx"))