import os
import re
import polars as pl

from sqlmodel import Session
from package_supply_chain.models import (
                    Item, 
                    EquivalentItem, 
                    ManufacturerItem, 
                    Nomenclature, 
                    Image, 
                    HeliosIG,
                    HeliosIGEquip, 
                    Pop, 
                    Pno, 
                    Sla, 
                    Cco, 
                    Store, 
                    Pudo, 
                    Min_Max, 
                    PE, 
                    PJ, 
                    Stock, 
                    Movement, 
                    TranscoStoreOracleSpeed
                    )


from package_supply_chain.my_loguru import logger
from package_supply_chain.photo_processing import resize_image


def import_data_minmax(engine, minmax_dataframe: pl.DataFrame) -> None:
    logger.info("Importation des min max ...")
    with Session(engine) as session:
        for i, row in enumerate(minmax_dataframe.iter_rows(named=True)):
            minmax = Min_Max(
                code_article=row["code_article"], 
                code_magasin=row["code_magasin"], 
                flag_gestion=row["flag_gestion_du_reappro_1_0"],
                qte_min=row["qte_min"], 
                qte_max=row["qte_max"])
            session.add(minmax)
        session.commit()
    logger.info(f"{i} min max importés")


def import_data_pudo(engine, pudo_dataframe: pl.DataFrame) -> None:
    logger.info("Import des points-relais ...")
    with Session(engine) as session:
        for i, row in enumerate(pudo_dataframe.iter_rows(named=True)):
            pudo = Pudo(code_point_relais=row["code_point_relais"], 
                        nom_point_relais=row["nom_point_relais"], 
                        code_transporteur=row["code_transporteur"], 
                        eligibilite=row["eligibilite"], 
                        flag_hors_norme=row["flag_hors_norme"], 
                        flag_actif=row["flag_actif"], 
                        adresse_1=row["adresse_1"],
                        adresse_2=row["adresse_2"],
                        adresse_3=row["adresse_3"],
                        adresse_4=row["adresse_4"], 
                        code_postal=row["code_postal"], 
                        ville=row["ville"], 
                        categorie_de_point_relais=row["categorie_de_point_relais"], 
                        code_activite=row["code_activite"])
            session.add(pudo)
        session.commit()
    logger.info(f"{i} point-relais importés")


def import_data_items(engine, items_dataframe: pl.DataFrame) -> None:

    logger.info("Import des articles...")
    with Session(engine) as session:
        for i, row in enumerate(items_dataframe.iter_rows(named=True)):
            article = Item(
                code_article=row["code_article"],
                proprietaire_article=row["proprietaire_article"],
                type_article=row["type_article"],
                libelle_court_article=row["libelle_court_article"],
                libelle_long_article=row["libelle_long_article"],
                description_famille_d_achat=row["description_famille_d_achat"],
                commentaire_technique=row["commentaire_technique"],
                commentaire_logistique=row["commentaire_logistique"],
                statut_abrege_article=row["statut_abrege_article"],
                cycle_de_vie_achat=row["cycle_de_vie_achat"],
                cycle_de_vie_de_production_pim=row["cycle_de_vie_de_production_pim"],
                feuille_du_catalogue=row["feuille_du_catalogue"],
                description_de_la_feuille_du_catalogue=row["description_de_la_feuille_du_catalogue"],
                famille_d_achat_feuille_du_catalogue=row["famille_d_achat_feuille_du_catalogue"],
                catalogue_consommable=row["catalogue_consommable"],
                criticite_pim=row["criticite_pim"],
                famille_immobilisation=row["famille_immobilisation"],
                categorie_immobilisation=row["categorie_immobilisation"],
                categorie_inv_accounting=row["categorie_inv_accounting"],
                suivi_par_num_serie_oui_non=row["suivi_par_num_serie_oui_non"] == "OUI",
                stocksecu_inv_oui_non=row["stocksecu_inv_oui_non"] == "OUI",
                article_hors_normes=row["article_hors_normes"] == "OUI",
                peremption=row["peremption"] == "OUI",
                retour_production=row["retour_production"] == "OUI",
                est_oc_ou_ol=row["est_oc_ou_ol"],
                a_retrofiter=row["a_retrofiter"],
                # Champs logistiques
                affretement=row["affretement"],
                fragile=row["fragile"],
                poids_article=row["poids_article"],
                volume_article=row["volume_article"],
                hauteur_article=row["hauteur_article"],
                longueur_article=row["longueur_article"],
                largeur_article=row["largeur_article"],
                matiere_dangereuse=row["matiere_dangereuse"],
                md_code_onu=row["md_code_onu"],
                md_groupe_emballage=row["md_groupe_emballage"],
                md_type_colis=row["md_type_colis"],
                prix_achat_prev=row["prix_achat_prev"],
                pump=row["pump"],
                prix_eur_catalogue_article=row["prix_EUR_catalogue_article"],
                compte_cg_achat=row["compte_cg_achat"],
                delai_approvisionnement=row["delai_approvisionnement"],
                delai_de_reparation_contractuel=row["delai_de_reparation_contractuel"],
                point_de_commande=row["point_de_commande"],
                quantite_a_commander=row["quantite_a_commander"],
                qte_cde_minimum_point_de_reappro=row["qte_cde_minimum_point_de_reappro"],
                qte_minimum_ordre_de_commande=row["qte_minimum_ordre_de_commande"],
                qte_maximum_ordre_de_commande=row["qte_maximum_ordre_de_commande"],
                qte_min_de_l_article=row["qte_min_de_l_article"],
                qte_max_de_l_article=row["qte_max_de_l_article"],
                qte_cde_maximum_quantite_d_ordre_de_commande=row["qte_cde_maximum_quantite_d_ordre_de_commande"],
                lieu_de_reparation_pim=row["lieu_de_reparation_pim"],
                description_lieu_de_reparation=row["description_lieu_de_reparation"],
                rma=row["rma"],
                role_responsable_et_equipement=row["role_responsable_et_equipement"],
                mnemonique=row["mnemonique"],
                date_creation_article=row["date_creation_article"],
                nom_createur_article=row["nom_createur_article"],
                date_derniere_modif_article=row["date_derniere_modif_article"],
                auteur_derniere_modif_article=row["auteur_derniere_modif_article"]
            )
            session.add(article)

        session.commit()
        logger.info(f"{i} articles importés")


def import_data_manufacturer(engine, items_manufacturer_dataframe):
    logger.info("Import de la table des article et de leurs fabricants...")
    with Session(engine) as session:
        for i, row in enumerate(items_manufacturer_dataframe.iter_rows(named=True)):
            item_manufacturer = ManufacturerItem(
                code_article=row["code_article"],
                nom_fabricant=row["nom_fabricant"],
                reference_article_fabricant=row["reference_article_fabricant"]
            )
            session.add(item_manufacturer)
        session.commit()
    logger.info(f"{i} articles avec fabricants importés")


def import_data_equivalent(engine, items_equivalent_dataframe: pl.DataFrame) -> None:
    logger.info("Import de la table des articles équivalents")
    with Session(engine) as session:
        for i, row in enumerate(items_equivalent_dataframe.iter_rows(named=True)):
            equivalent_item = EquivalentItem(
                code_article = row["code_article"], 
                code_article_equivalent = row["code_article_correspondant"],
                type_relation = row["type_de_relation"]
                )
            session.add(equivalent_item)
        session.commit()
    logger.info(f"{i} articles équivalents importés")


def import_data_nomenclature(engine, nomenclature_dataframe: pl.DataFrame) -> None:
    logger.info("Import des nomenclatures...")
    with Session(engine) as session:
        for i, row in enumerate(nomenclature_dataframe.iter_rows(named=True)):
            nomenclature_item = Nomenclature(
                code_article_parent=row["article"], 
                code_article_fils=row["article_eqpt_article_fils"], 
                quantite=row["art_et_art_fils_eqpt_quantite"]
                )
            session.add(nomenclature_item)
        session.commit()
    logger.info(f"{i} nomenclatures créées")


def import_data_image(engine, path_input_photo: str) -> None:

    logger.info("Import des images...")
    extensions_photo = {"jpeg", "jpg", "png"}
    pattern_code_art = r"[A-Z]{3}\d{4}\d{2}"

    liste_files = os.listdir(path_input_photo)
    liste_photos = [file for file in liste_files if os.path.isfile(os.path.join(path_input_photo, file)) and file.lower().split(".")[1] in extensions_photo]
    with Session(engine) as session:
        for i, photo in enumerate(liste_photos):
            with open(os.path.join(path_input_photo, photo), "rb") as f:
                found_patterns = re.search(pattern_code_art, photo)
                if found_patterns:
                    item_code = photo[found_patterns.start():found_patterns.end()]
                    image_bytes = f.read()
                    image = Image(code_article=item_code, image=resize_image(image_bytes))
                    session.add(image)
        session.commit()
    logger.info(f"{i} images importées")


def import_data_ig(engine, ig_dataframe: pl.DataFrame) -> None:
    with Session(engine) as session:
        for i, row in enumerate(ig_dataframe.iter_rows(named=True)):
            helios_ig = HeliosIG(code_ig=row["code_ig"], 
                    libelle_court_ig=row["libelle_court_ig"], 
                    libelle_long_ig=row["libelle_long_ig"],
                    adresse=row["adresse"], 
                    code_postal=row["code_postal"], 
                    commune=row["commune"], 
                    categorie=row["categorie"], 
                    libelle_entite=row["libelle_entite"], 
                    nature_infrastructure=row["nature_infrastructure"], 
                    etat_ig=row["etat_ig"], 
                    date_creation=row["date_creation_ig"], 
                    latitude=row["latitude"], 
                    longitude=row["longitude"], 
                    importance_site=row["importance_site_tdf"], 
                    classement_1u=row["classement_1_unite"], 
                    reserves_1u=row["reserves_1u"], 
                    zones2u=row["zone2u_site1ur_nom_helios_localisation_reserve"])
            session.add(helios_ig)
        session.commit()
    logger.info(f"{i} ig importés")



def import_data_ig_equipement(engine, helios_ig_equipement_df: pl.DataFrame) -> None:
    with Session(engine) as session:
        for i, row in enumerate(helios_ig_equipement_df.iter_rows(named=True)):
            helios_ig_equipement = HeliosIGEquip(code_ig=row["code_ig"], 
                                                 code_pop=row["code_pop_helios"], 
                                                 code_pno=row["pno_code_fonction_noeud"], 
                                                 code_sla=row["code_sla"], 
                                                 code_article=row["code_article"], 
                                                 nb_sites_active=row["nb_sites_active"], 
                                                 nb_sites_demontes=row["nb_sites_demontes"], 
                                                 nb_sites_desactives=row["nb_sites_desactivites"], 
                                                 nb_sites_en_cours_desinstallation=row["nb_sites_en_cours_desinstallation"], 
                                                 nb_sites_en_cours_installation=row["nb_sites_en_cours_installation"], 
                                                 nb_sites_en_cours_modification=row["nb_sites_en_cours_modification"], 
                                                 actif=row["actif"])
            session.add(helios_ig_equipement)
        session.commit()
    logger.info(f"{i} articles ig equipements importés")

def import_data_pop(engine, pop_df: pl.DataFrame) -> None:
    with Session(engine) as session:
        for i, row in enumerate(pop_df.iter_rows(named=True)):
            pop = Pop(code_pop=row["code_pop_helios"], 
                      libelle_pop=row["libelle_pop_helios"], 
                      statut_pop=row["statut_pop_helios"])
            session.add(pop)
        session.commit()
    logger.info(f"{i} pop importés")


def import_data_pno(engine, pno_df: pl.DataFrame) -> None:
    with Session(engine) as session:
        for i, row in enumerate(pno_df.iter_rows(named=True)):
            pno = Pno(code_pno=row["pno_code_fonction_noeud"], 
                      pno_description=row["pno_description_fonction"], 
                      pno_description_detaille=row["pno_description_detaillee_fonction"])
            session.add(pno)
        session.commit()
    logger.info(f"{i} pno importés")


def import_data_sla(engine, sla_df: pl.DataFrame) -> None:
    with Session(engine) as session:
        for i, row in enumerate(sla_df.iter_rows(named=True)):
            sla = Sla(code_sla=row["code_sla"], 
                      nom_sla=row["nom_sla"], 
                      description_detaille_sla=row["description_detaillee_du_sla"])
            session.add(sla)
        session.commit()
    logger.info(f"{i} sla importés")



def import_data_cco(engine, cco_df: pl.DataFrame) -> None:
    with Session(engine) as session:
        for i, row in enumerate(cco_df.iter_rows(named=True)):
            cco = Cco(code_cco=row["cco_code_contrat"], 
                      cco_description=row["cco_description"], 
                      cco_type=row["cco_type_contrat"])
            session.add(cco)
        session.commit()
    logger.info(f"{i} cco importés")


def import_data_referential_stores(engine, referential_stores_df: pl.DataFrame) -> None:
    with Session(engine) as session:
        for i, row in enumerate(referential_stores_df.iter_rows(named=True)):
            store = Store(
                code_magasin=row["code_magasin"], 
                code_tiers_daher=row["code_tiers_daher"],
                libelle_magasin=row["libelle_magasin"],
                statut=row["statut"], 
                type_de_depot=row["type_de_depot"],
                contact=row["contact"], 
                matricule_rh=row["matricule_rh"],
                equipe=row["equipe"], 
                region_etat_dr_emplacement=row["region_etat_dr_emplacement"],
                nom_responsable=row["nom_responsable"],
                prenom_responsable=row["prenom_responsable"], 
                mail_responsable=row["mail_responsable"],
                tel_contact=row["tel_contact"], 
                email_contact=row["email_contact"], 
                adresse_1=row["adresse1"],
                adresse_2=row["adresse2"],
                code_postal=row["code_postal"],
                ville=row["ville"], 
                pr_principal=row["pr_principal"], 
                pr_backup=row["pr_backup"],
                pr_hors_norme=row["pr_hors_norme"],
                code_ig=row["code_ig_du_tiers_emplacement"],
                code_transporteur_inf_35_kg=row["code_transporteur_inf_35_kg"],
                code_produit_inf_35_kg=row["code_produit_inf_35_kg"],
                code_transporteur_sup_35_kg=row["code_transporteur_sup_35_kg"],
                code_produit_sup_35_kg=row["code_produit_sup_35_kg"],
                latitude=row["latitude"],
                longitude=row["longitude"],
                date_creation=row["date_creation"],
                user_creation=row["user_creation"],
                mnemonique_technicien=row["mnemonique_tek"],
                amont_aval=row["amont_aval"],
                mag_18DE_95MG_18BB_44AA_MBRO=row["mag_18DE_95MG_18BB_44AA_MBRO"],
                mag_MMHS_MVEO_MTRA_MPER=row["mag_MMHS_MVEO_MTRA_MPER"]
                )
            session.add(store)
        session.commit()
    logger.info(f"{i} magasins importés")
            


def import_data_pe(engine, pe_dataframe: pl.DataFrame) -> None:
    logger.info("Import des code projets pe ...")
    with Session(engine) as session:
        for i, row in enumerate(pe_dataframe.iter_rows(named=True)):
            pe = PE(code_projet=row["projet_elementaire"], 
                    libelle_projet=row["libelle_pe"], 
                    description_projet=row["description_pe"], 
                    code_nature=row["nature_pe"], 
                    libelle_nature=row["libelle_nature_pe"], 
                    statut=row["statut_pe"], 
                    supprime=row["projet_pe_supprime_o_n"], 
                    code_projet_industrie=row["projet_industrie_information"], 
                    commercial_email=row["commercial_pe_email"], 
                    responsable=row["responsable_pe_nom_prenom"], 
                    responsable_email=row["responsable_pe_email"], 
                    code_ig=row["ig_principal_pe"], 
                    code_programme=row["code_programme_budgetaire_base_pe"], 
                    libelle_programe=row["libelle_programme_pe"], 
                    code_projet_budgetaire=row["code_projet_budgetaire_base_pe"], 
                    libelle_projet_budgetaire=row["lib_projet_budgetaire"])
            session.add(pe)
        session.commit()
    logger.info(f"{i} code projets élementaires importés")


def import_data_pj(engine, pj_dataframe: pl.DataFrame) -> None:
    logger.info("Import des données des projets industries ...")
    with Session(engine) as session:
        for i, row in enumerate(pj_dataframe.iter_rows(named=True)):
            pj = PJ(code_projet=row["projet_industrie"], 
                    libelle_projet=row["libelle_projet_industrie"], 
                    projet_retraite=row["pj_retraite"], 
                    description_projet=row["description_projet_industrie"], 
                    statut=row["statut_projet_industrie"], 
                    chef_projet=row["chef_projet_projet_industrie_nom_prenom"], 
                    chef_projet_email=row["chef_projet_projet_industrie_email"], 
                    libelle_programme=row["libelle_programme_pe"], 
                    responsable_programme_pe=row["responsable_programme_pe"], 
                    code_programme=row["code_programme_budgetaire_base_pe"], 
                    libelle_programme_budgetaire=row["lib_programme_budgetaire_base_pe"], 
                    code_projet_budgetaire=row["code_projet_budgetaire_base_pe"])
            session.add(pj)
        session.commit()

    logger.info(f"{i} codes projets industries importées.")


def import_data_stock(engine, stock_dataframe: pl.DataFrame) -> None:
    logger.info("Import des données stock temps réel ....")
    with Session(engine) as session:
        for i, row in enumerate(stock_dataframe.iter_rows(named=True)):
            stock = Stock(date_stock=row["date_stock"], 
                          code_magasin=row["code_magasin"], 
                          flag_stock_d_m=row["flag_stock_d_m"], 
                          emplacement=row["emplacement"], 
                          transit=row["transit"], 
                          code_article=row["code_article"], 
                          n_lot=row["n_lot"], 
                          n_serie=row["n_serie"], 
                          n_version=row["n_version"], 
                          qualite=row["qualite"], 
                          qte_stock=row["qte_stock"], 
                          date_reception=row["date_reception"], 
                          delai_anciennete=row["delai_anciennete"], 
                          code_projet=row["code_projet"], 
                          n_cde_dpm_dpi=row["n_cde_dpm_dpi"], 
                          n_ums=row["n_ums"], 
                          n_bt=row["n_bt"], 
                          ig_installation=row["ig_installation"], 
                          commentaire_operateur_tdf=row["commentaires_operateurs_tdf"])
            session.add(stock)
        session.commit()
    logger.info(f"{i} lignes de stock importées.")


def import_data_mvt(engine, mvt_dataframe: pl.DataFrame) -> None:
    logger.info("Import des mouvements de stock ...")
    with Session(engine) as session:
        for i, row in enumerate(mvt_dataframe.iter_rows(named=True)):
            mvt = Movement(n_mvt=row["n_mvt"], 
                           date_mvt=row["date_mvt"], 
                           code_motif_mvt=row["code_motif_mvt"],
                           lib_motif_mvt=row["lib_motif_mvt"], 
                           code_article=row["code_article"], 
                           n_lot=row["n_lot"], 
                           n_serie=row["n_serie"], 
                           qualite=row["qualite"], 
                           sens_mvt=row["sens_mvt"], 
                           magasin=row["magasin"], 
                           emplacement=row["emplacement"], 
                           magasin_destinataire=row["code_magasin_destinataire"],
                           n_cde=row["n_cde"], 
                           code_projet=row["code_projet"], 
                           n_cde_speed=row["n_cde_speed"], 
                           n_ligne_cde_speed=row["n_ligne_cde_speed"], 
                           n_cde_dpm_dpi=row["n_cde_dpm_dpi"], 
                           code_ig_intervention=row["code_ig_intervention"], 
                           flag_panne_sur_stock=row["flag_panne_sur_stock"], 
                           n_bt=row["n_bt"], 
                           n_rma=row["n_rma"], 
                           n_dossier_reparation=row["n_dossier_reparation"],
                           cause_retour_equipement=row["cause_retour_equipement"]
                           )
            session.add(mvt)
        session.commit()
    logger.info(f"{i} lignes de mouvements de stock importés.")



def import_data_transco_code_mag_oracle_speed(engine, dataframe: pl.DataFrame) -> None:
    logger.info("Importation des données des transcodifications des magasins Oracle Speed")
    with Session(engine) as session:
        for i, row in enumerate(dataframe.iter_rows(named=True)):
            transco_store_code = TranscoStoreOracleSpeed(
                code_magasin_oracle=row["code_magasin_oracle"], 
                libelle_magasin_oracle=row["libelle_magasin_oracle"], 
                code_magasin_daher=row["transcodification_pour_daher"], 
                code_magasin_speed=row["tiers_speed"])
            session.add(transco_store_code)
        session.commit()

    logger.info(f"{i} lignes de transcodifications importées")

