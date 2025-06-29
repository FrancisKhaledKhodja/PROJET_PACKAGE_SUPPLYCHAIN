import os
import datetime as dt
import polars as pl
from package_supply_chain.constants import folder_path_output
from package_supply_chain.api_address_gps import get_latitude_and_longitude, get_cleaning_address
from package_supply_chain.my_loguru import logger
from package_supply_chain.miscellaneous_functions import get_execution_time


@get_execution_time
def get_stores_reference():
    now = dt.datetime.today().strftime("%Y%m%d")
    logger.info("Appel de l'api get_stores_reference")

    backup = pl.read_parquet(os.path.join(".", "package_supply_chain", "backup_addresses", f"backup_addresses.parquet"))
    backup = backup.unique("adresse_envoyee")
    dico_backup = {row["adresse_envoyee"]: row for row in backup.iter_rows(named=True)}
    last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0]
    stores = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stores.parquet"))

    responses = []
    addresses = {}
    for row in stores.iter_rows(named=True):
        address = get_cleaning_address(row["adresse1"], row["adresse2"], row["code_postal"], row["ville"])
        addresses[row["code_magasin"]] = address
        if address not in dico_backup:
            response = get_latitude_and_longitude(address)
            if response["latitude"] is not None:
                response["code_magasin"] = row["code_magasin"]
                response["adresse_envoyee"] = address
                responses.append(response)

    if responses:
        df = pl.DataFrame(responses)
        df = df.unique("adresse_envoyee")
        df = df.with_columns(pl.lit(now).alias("date"))
        df = df.with_columns(pl.col("date").str.to_date("%Y%m%d"))
        backup = pl.concat([df.select(pl.col("address", "adresse_envoyee", "latitude", "longitude", "date")), backup])
        backup = backup.unique("adresse_envoyee")
        backup.write_parquet(os.path.join(".", "package_supply_chain", "backup_addresses", f"backup_addresses.parquet"))

    stores = stores.join(pl.DataFrame({"code_magasin": list(addresses.keys()), "adresse_envoyee": list(addresses.values())}), how="left", on="code_magasin")
    stores = stores.join(backup.select(pl.col("adresse_envoyee", "latitude", "longitude")), how="left", on="adresse_envoyee")

    logger.info("Fin de l'appel de l'api get_stores_references")

    return stores
            

                

    
    

