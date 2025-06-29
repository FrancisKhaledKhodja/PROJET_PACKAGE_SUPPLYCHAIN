import os

from package_supply_chain.constants import folder_path_output
from package_supply_chain.api.api_analyze_m_to_d import get_items_m_to_d

def test_get_list_items_m_to_d():
    last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0] 
    list_items_m_to_d = get_items_m_to_d()
    list_items_m_to_d.write_parquet(os.path.join(folder_path_output, last_folder, "items_m_to_d.parquet"))
    list_items_m_to_d.write_excel(os.path.join(".", "excel_files_output", "items_m_to_d.xlsx"))

