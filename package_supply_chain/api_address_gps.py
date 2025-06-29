import os
import requests
import re
from string import punctuation

import unidecode
from dotenv import load_dotenv

from package_supply_chain.my_loguru import logger

load_dotenv()
proxy_tdf = os.getenv("proxy_tdf")
login = os.getenv("login")
password = os.getenv("password")

def get_datagouv_response_gps_for_address(address: str) -> dict:
    URL_DATA_GOUV = "https://api-adresse.data.gouv.fr/search/"
    os.environ["https_proxy"] = f"http://{login}:{password}@{proxy_tdf}"
    address = get_cleaning_address(address)
    params = {"q": address, "limit": 5}
    results = requests.get(URL_DATA_GOUV, params=params).json()
    if "features"in results and results["features"]:
        result = sorted(results["features"], key=lambda row: row["properties"]["score"], reverse=True)[0]
        return result
    else:
        return None


def get_cleaning_address(*address):
    cleaned_address = []
    for element in address:
        for punct in punctuation:
            if element is not None and punct in element:
                element = element.replace(punct, " ")
        if element is not None and "n°" in element:
            element = element.replace("n°", "numero")
        if element is not None:
            cleaned_address.append(unidecode.unidecode(element.lower().strip()))
    cleaned_address = " ".join(cleaned_address)
    cleaned_address = re.sub(r"\s+", r" ", cleaned_address)
    cleaned_address = cleaned_address.lower()
    return cleaned_address


def get_latitude_and_longitude(address: str):
    result = get_datagouv_response_gps_for_address(address)
    if result:
        label = result["properties"]["label"]
        longitude, latitude = result["geometry"]["coordinates"]
        return {"address": label, "latitude": latitude, "longitude": longitude}
    else:
        return {"address": None, "latitude": None, "longitude": None}




