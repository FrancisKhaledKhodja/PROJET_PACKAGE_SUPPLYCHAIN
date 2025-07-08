import os
import datetime as dt
import polars as pl

from package_supply_chain.constants import folder_path_output
from package_supply_chain.miscellaneous_functions import get_execution_time
from package_supply_chain.my_loguru import logger

@get_execution_time
def get_state_stocks():
    logger.info("Appel de l'api get_state_stock")
    last_folder = sorted(os.listdir(folder_path_output), reverse=True)[0]
    helios = pl.read_parquet(os.path.join(folder_path_output, last_folder, "helios.parquet"))
    stock = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stock.parquet"))
    stock_oracle = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stock_oracle.parquet"))
    correspondence_store_codes = pl.read_parquet(os.path.join(folder_path_output, last_folder,"correspondence_store_code_oracle_speed.parquet"))
    min_max = pl.read_parquet(os.path.join(folder_path_output, last_folder, "minmax.parquet"))
    stores = pl.read_parquet(os.path.join(folder_path_output, last_folder, "stores.parquet"))
    items = pl.read_parquet(os.path.join(folder_path_output, last_folder, "items.parquet"))
    pe = pl.read_parquet(os.path.join(folder_path_output, last_folder, "pe.parquet"))
    pj = pl.read_parquet(os.path.join(folder_path_output, last_folder, "pj.parquet"))
    repartition_sheet_bu = pl.read_parquet(os.path.join(folder_path_output, last_folder, "bu_sheet_repartition.parquet"))
    repartition_project_bu = pl.read_parquet(os.path.join(folder_path_output, last_folder, "bu_project_repartition.parquet"))
    dpi = pl.read_parquet(os.path.join(folder_path_output, last_folder, "dpi.parquet"))
    correspondance_user_dpi = pl.read_parquet(os.path.join(folder_path_output, last_folder, "correspondance_user_dpi.parquet"))
    mvt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))
    items_without_exit = pl.read_parquet(os.path.join(folder_path_output, last_folder, "items_without_exit_final.parquet"))

    # Select columns
    stock_columns = ["date_stock", "code_magasin", "flag_stock_d_m", "emplacement", "transit", "code_article", "n_lot", "n_serie", "n_version",
                     "qualite", "qte_stock", "date_reception", "code_projet", "n_cde_dpm_dpi", "n_ums", "n_bt", "ig_installation", 
                     "commentaires_operateurs_tdf"]
    stock = stock.select(stock_columns)

    stock = stock.with_columns(pl.col("n_lot").str.strip_chars().alias("n_lot"))
    stock = stock.with_columns(pl.col("n_serie").str.strip_chars().alias("n_serie"))

    stock = stock.with_columns(pl.col("n_lot").str.to_uppercase().alias("n_lot"))
    stock = stock.with_columns(pl.col("n_serie").str.to_uppercase().alias("n_serie"))

    # Add store code speed ont stock oracle
    stock_oracle = stock_oracle.join(correspondence_store_codes.select(pl.col("code_magasin_oracle", "tiers_speed")), 
                                     how="left", 
                                     left_on="code_magasin", 
                                     right_on="code_magasin_oracle")

    # Add reception date stock oracle
    stock = stock.join(stock_oracle.select(pl.col("tiers_speed", "code_article", "n_lot", "n_serie", "dt_reception_emplacement")), 
                       how="left", 
                       left_on=["code_magasin", "code_article", "n_lot", "n_serie"], 
                       right_on=["tiers_speed", "code_article", "n_lot", "n_serie"])
    
    # Add corrected reception date
    expression = (
        pl.when((pl.col("dt_reception_emplacement").is_not_null()) & (pl.col("n_serie").is_not_null()))
        .then(pl.col("dt_reception_emplacement"))
        .otherwise(pl.col("date_reception"))
    )

    stock = stock.with_columns(expression.alias("date_reception_corrigee"))

    # Calculate the number of days in stock
    date_now = dt.datetime.today()
    stock = stock.with_columns((pl.lit(date_now) - pl.col("date_reception_corrigee")).alias("delai_anciennete_jours"))
    stock = stock.with_columns(pl.col("delai_anciennete_jours").dt.total_days().alias("delai_anciennete_jours"))

    # Classification of the seniority items
    expression = (
        pl.when(pl.col("delai_anciennete_jours") < 2 * 30)
        .then(pl.lit("1 - moins de 2 mois"))
        .otherwise(
            pl.when(pl.col("delai_anciennete_jours") < 6 * 30)
            .then(pl.lit("2 - entre 2 et 6 mois"))
            .otherwise(
                pl.when(pl.col("delai_anciennete_jours") < 12 * 30)
                .then(pl.lit("3 - entre 6 mois et 1 an"))
                .otherwise(
                    pl.when(pl.col("delai_anciennete_jours") < 2 * 12 * 30)
                    .then(pl.lit("4 - entre 1 et 2 ans"))
                    .otherwise(
                        pl.when(pl.col("delai_anciennete_jours") < 5 * 12 * 30)
                        .then(pl.lit("5 - entre 2 et 5 ans"))
                        .otherwise(
                            pl.when(pl.col("delai_anciennete_jours") < 10 * 12 * 30)
                            .then(pl.lit("6 - entre 5 et 10 ans"))
                            .otherwise(
                                pl.when(pl.col("delai_anciennete_jours") < 15 * 12 * 30)
                                .then(pl.lit("7 - entre 10 et 15 ans"))
                                .otherwise(
                                    pl.when(pl.col("delai_anciennete_jours") < 18 * 12 *30)
                                    .then(pl.lit("8 - entre 15 et 18 ans"))
                                    .otherwise(pl.lit("9 - plus de 18 ans"))
                                )
                            )
                        )
                    )
                )
            )
        )
    )

    stock = stock.with_columns(expression.alias("categorie_anciennete"))
    

    # Add stores with no stock and Min Max
    stock_stores_items = stock.select(pl.col("code_magasin", "code_article")).unique()

    minmax_stores_items = min_max.filter(pl.col("qte_min") != 0).select(pl.col("code_magasin", "code_article")).unique()
    df = minmax_stores_items.join(stock_stores_items, how="anti", on=["code_magasin", "code_article"])
    stock = pl.concat([stock, df], how="diagonal")

    # Add Min Max
    stock = stock.join(min_max.select(pl.col("code_magasin", "code_article", "qte_min", "qte_max")), how="left", on=["code_magasin", "code_article"])

    # Fill column "qte_min" and "qte_max" with 0 if null
    for name_col in ("qte_min", "qte_max"):
        expression = (
            pl.when(pl.col(name_col).is_null())
            .then(0)
            .otherwise(pl.col(name_col))
        )
        stock = stock.with_columns(expression.alias(name_col))

    # Fill column "flag_stock_d_m" with "M" if null
    expression = (
        pl.when(pl.col("flag_stock_d_m").is_null())
        .then(pl.lit("M"))
        .otherwise(pl.col("flag_stock_d_m"))
    )
    stock = stock.with_columns(expression.alias("flag_stock_d_m"))

    # Fill column "qte_stock" with 0 if null
    expression = (
        pl.when(pl.col("qte_stock").is_null())
        .then(0)
        .otherwise(pl.col("qte_stock"))
    )
    stock = stock.with_columns(expression.alias("qte_stock"))

    # Add stores with no stock and no Min Max and active
    stock_stores_list = set(stock.select(pl.col("code_magasin")).unique().to_series().to_list())
    stores_stores_list = set(stores.filter(pl.col("statut") == 0).select(pl.col("code_magasin")).unique().to_series().to_list())
    difference_stores = stores_stores_list.difference(stock_stores_list)
    stock = pl.concat([stock, pl.DataFrame({"code_magasin": list(difference_stores)})], how="diagonal")

    # Add date_stock if it is null
    date_stock = stock.select(pl.col("date_stock")).head(1).to_series().to_list()[0]
    stock = stock.with_columns(pl.lit(date_stock).alias("date_stock"))

    # Add store informations
    columns_stores = ["code_magasin", "libelle_magasin", "statut", "type_de_depot", "amont_aval"]
    stock = stock.join(stores.select(columns_stores), how="left", on=["code_magasin"])
    

    # Add item information
    columns_items = ["code_article", "libelle_court_article", "type_article", "statut_abrege_article", "criticite_pim", "feuille_du_catalogue", 
                     "stocksecu_inv_oui_non", "pump", "lieu_de_reparation_pim", "commentaire_logistique", "est_oc_ou_ol"]
    stock = stock.join(items.select(columns_items), how="left", on=["code_article"])

    # Convert column "pump" to float32
    stock = stock.with_columns(pl.col("pump").cast(pl.Float32, strict=False))

    # Add values of the stock
    stock = stock.with_columns((pl.col("qte_stock") * pl.col("pump")).alias("valo_stock"))

    # Add pe informations
    columns_pe = ["projet_elementaire", "libelle_pe", "statut_pe", "projet_industrie_information", "responsable_pe_nom_prenom", "code_programme_budgetaire_base_pe", "libelle_programme_pe"]
    stock = stock.join(pe.select(columns_pe), how="left", left_on=["code_projet"], right_on=["projet_elementaire"])

    # add pj informations
    columns_pj = ["projet_industrie", "libelle_projet_industrie", "statut_projet_industrie", "chef_projet_projet_industrie_nom_prenom", 
                  "code_programme_budgetaire_base_pe", "lib_programme_budgetaire_base_pe"]
    stock = stock.join(pj.select(columns_pj), how="left", left_on=["code_projet"], right_on=["projet_industrie"])

    # Merge program code and remove code_programme_budgetaire_base_pe_right and lib_programme_budgetaire_base_pe
    expression = (
        pl.when(pl.col("code_programme_budgetaire_base_pe").is_null())
        .then(pl.col("code_programme_budgetaire_base_pe_right"))
        .otherwise(pl.col("code_programme_budgetaire_base_pe"))
    )
    stock = stock.with_columns(expression.alias("code_programme_budgetaire_base_pe"))

    expression = (
        pl.when(pl.col("libelle_programme_pe").is_null())
        .then(pl.col("lib_programme_budgetaire_base_pe"))
        .otherwise(pl.col("libelle_programme_pe"))
    )

    stock = stock.with_columns(expression.alias("libelle_programme_pe"))
    stock = stock.drop(["code_programme_budgetaire_base_pe_right", "lib_programme_budgetaire_base_pe"])

    # Add pj code in column projet_industrie_information when code_projet is a pj code
    expression = (
        pl.when(pl.col("projet_industrie_information").is_null())
        .then(pl.col("code_projet"))
        .otherwise(pl.col("projet_industrie_information"))
    )
    stock = stock.with_columns(expression.alias("projet_industrie_information"))

    stock = stock.join(pj.select(pl.col("projet_industrie", "libelle_projet_industrie", "statut_projet_industrie", "chef_projet_projet_industrie_nom_prenom")), how="left", left_on=["projet_industrie_information"], right_on=["projet_industrie"])

    for name_column, name_column_2 in (("libelle_projet_industrie", "libelle_projet_industrie_right"), 
                                       ("statut_projet_industrie", "statut_projet_industrie_right"), 
                                       ("chef_projet_projet_industrie_nom_prenom", "chef_projet_projet_industrie_nom_prenom_right")):
        expression = (
            pl.when(pl.col(name_column).is_null())
            .then(pl.col(name_column_2))
            .otherwise(pl.col(name_column))
            )
        stock = stock.with_columns(expression.alias(name_column))
        stock = stock.drop(name_column_2)

    stock = stock.join(helios.select(pl.col("code_ig", "libelle_long_ig")), how="left", left_on="ig_installation", right_on="code_ig")

    # Add sheet bu
    stock = stock.join(repartition_sheet_bu.select(pl.col("feuille_du_catalogue", "bu")), how="left", on="feuille_du_catalogue")

    # Add project bu
    stock = stock.join(repartition_project_bu.select(pl.col("code_programme", "bu")), how="left", left_on="code_programme_budgetaire_base_pe", right_on="code_programme")

    # Compute the bu for each stock row 
    expression = (
        pl.when(pl.col("bu_right").is_not_null())
        .then(pl.col("bu_right"))
        .otherwise(pl.col("bu"))
    )

    stock = stock.with_columns(expression.alias("bu"))
    stock = stock.drop("bu_right")


    # Add dpi creator
    logistic_personnel = ["homeyer_j", "postel", "minassian", "grimault patrick"]
    dpi_demandeur = dpi.select(pl.col("numero_demande", "demandeur", "responsable_projet", "n_de_demande_de_liv_trans", "user_creation_de_demande_de_liv_trans")).unique()


    expression = (
        pl.when(pl.col("n_de_demande_de_liv_trans").is_null())
        .then(
            pl.when(pl.col("demandeur").str.to_lowercase().is_in(logistic_personnel))
            .then(pl.col("responsable_projet"))
            .otherwise(pl.col("demandeur"))
        )
        .otherwise(
            pl.when(pl.col("user_creation_de_demande_de_liv_trans").str.to_lowercase().is_in(logistic_personnel))
            .then(
                pl.when(pl.col("demandeur").str.to_lowercase().is_in(logistic_personnel))
                    .then(pl.col("responsable_projet"))
                    .otherwise(pl.col("demandeur"))
            )
            .otherwise(pl.col("user_creation_de_demande_de_liv_trans"))
        )
    )

    dpi_demandeur = dpi_demandeur.with_columns(expression.alias("demandeur_dpi"))
    dpi_demandeur = dpi_demandeur.join(correspondance_user_dpi.select("nom", "user_dpi"), how="left", left_on="demandeur_dpi", right_on="user_dpi")

    dpi_demandeur = dpi_demandeur.drop("demandeur_dpi")
    dpi_demandeur = dpi_demandeur.rename({"nom": "demandeur_dpi"})
    dpi_demandeur_liv = dpi_demandeur.filter(pl.col("n_de_demande_de_liv_trans").is_not_null()).select(pl.col("n_de_demande_de_liv_trans", "demandeur_dpi")).unique()
    dpi_demandeur_liv = dpi_demandeur_liv.rename({"n_de_demande_de_liv_trans": "numero_demande"})

    dpi_demandeur = dpi_demandeur.filter(pl.col("n_de_demande_de_liv_trans").is_null()).filter(~pl.col("numero_demande").str.starts_with("S")).select(pl.col("numero_demande", "demandeur_dpi")).unique()
    dpi_demandeur_final = pl.concat([dpi_demandeur, dpi_demandeur_liv])
    dpi_demandeur_final = dpi_demandeur_final.unique()

    stock = stock.join(dpi_demandeur_final, how="left", left_on="n_cde_dpm_dpi", right_on="numero_demande")
    
    # add last known movement for each store
    
    last_mvt_per_store =  mvt.sort("magasin", "date_mvt", "n_mvt").select(pl.col("magasin", "date_mvt")).unique(subset="magasin", keep="last")
    last_mvt_per_store = last_mvt_per_store.rename({"date_mvt": "dernier_mvt_connu_du_magasin"})

    stock = stock.join(last_mvt_per_store, how="left", left_on="code_magasin", right_on="magasin")

    # add days without exit
    stock = stock.join(items_without_exit.select(pl.col("code_article", "nbre_jours_sans_sortie", "categorie_sans_sortie")), how="left", on="code_article")

    expression = (
        pl.when((pl.col("libelle_pe").is_null()) & (pl.col("flag_stock_d_m") == "D"))
        .then(pl.col("libelle_projet_industrie"))
        .otherwise(pl.col("libelle_pe"))
        .alias("libelle_pe")
    )

    stock = stock.with_columns(expression)

    expression = (
        pl.when((pl.col("statut_pe").is_null()) & (pl.col("flag_stock_d_m") == "D"))
        .then(pl.col("statut_projet_industrie"))
        .otherwise(pl.col("statut_pe"))
        .alias("statut_pe")
    )
    stock = stock.with_columns(expression)

    expression = (
        pl.when((pl.col("responsable_pe_nom_prenom").is_null()) & (pl.col("flag_stock_d_m") == "D"))
        .then(pl.col("chef_projet_projet_industrie_nom_prenom"))
        .otherwise(pl.col("responsable_pe_nom_prenom"))
        .alias("responsable_pe_nom_prenom")
    )
    stock = stock.with_columns(expression)


    stock = stock.rename({"libelle_pe": "libelle_projet", 
                          "projet_industrie_information": "code_pj", 
                          "libelle_projet_industrie": "libelle_pj", 
                          "code_programme_budgetaire_base_pe": "code_pg",
                          "libelle_programme_pe": "libelle_pg", 
                          "statut_projet_industrie": "statut_pj", 
                          "statut_pe": "statut_projet",
                          "responsable_pe_nom_prenom": "responsable_projet",
                          "chef_projet_projet_industrie_nom_prenom": "responsable_pj"})

    # Put the columns in the order
    columns_order = ["date_stock", "code_magasin", "libelle_magasin", "statut", "type_de_depot", "amont_aval", "flag_stock_d_m", "emplacement", "transit",
                     "code_article", "libelle_court_article", "type_article", "statut_abrege_article", "criticite_pim", "feuille_du_catalogue", 
                     "stocksecu_inv_oui_non", "pump", "lieu_de_reparation_pim", "commentaire_logistique", "est_oc_ou_ol", "qte_min", "qte_max", 
                     "n_lot", "n_serie", "n_version", "qualite", "qte_stock", "valo_stock", "date_reception", "n_cde_dpm_dpi", "demandeur_dpi", "n_ums", 
                     "n_bt", "ig_installation", "libelle_long_ig", "commentaires_operateurs_tdf", "code_projet", "libelle_projet", "statut_projet", 
                     "responsable_projet", "code_pj", "libelle_pj", "statut_pj", "responsable_pj", "code_pg", "libelle_pg", "date_reception_corrigee", 
                     "dernier_mvt_connu_du_magasin", "delai_anciennete_jours", "categorie_anciennete", "nbre_jours_sans_sortie", 
                     "categorie_sans_sortie", "bu"]
    stock = stock.select(columns_order)

    stock = stock.with_columns(pl.concat_str(pl.col("code_article", "n_lot", "n_serie"), separator="-").alias("cle_article"))

    logger.info("Fin de l'appel de l'api get_state_stock")

    return stock
    


