from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel, Field, Column
from sqlalchemy import LargeBinary


class ManufacturerItem(SQLModel, table=True):
    """Table d'association entre Article et Manufacturer"""
    id: Optional[int] = Field(default=None, primary_key=True)
    code_article: str = Field(foreign_key="item.code_article")
    nom_fabricant: Optional[str] = None 
    reference_article_fabricant: Optional[str] = None
    

class EquivalentItem(SQLModel, table=True):
    id: Optional[int]= Field(default=None, primary_key=True)
    code_article: str = Field(foreign_key="item.code_article")
    code_article_equivalent: Optional[str] 
    type_relation: Optional[str]


class Item(SQLModel, table=True):
    """Modèle pour les articles"""
    
    # Champs d'identification
    code_article: str = Field(unique=True, index=True, primary_key=True)
    proprietaire_article: str = Field(index=True)
    type_article: Optional[str] = Field(default="", index=True)
    
    # Libellés et descriptions
    libelle_court_article: str
    libelle_long_article: Optional[str] = None
    description_famille_d_achat: Optional[str] = None
    commentaire_technique: Optional[str] = None
    commentaire_logistique: Optional[str] = None
    
    # Statut et cycle de vie
    statut_abrege_article: Optional[str] = None
    cycle_de_vie_achat: Optional[str] = None
    cycle_de_vie_de_production_pim: Optional[str] = None
    
    # Catalogue et classification
    feuille_du_catalogue: Optional[str] = Field(default=None, index=True)
    description_de_la_feuille_du_catalogue: Optional[str] = None
    famille_d_achat_feuille_du_catalogue: Optional[str] = None
    catalogue_consommable: Optional[str] = None
    criticite_pim: Optional[str] = None
    famille_immobilisation: Optional[str] = None
    categorie_immobilisation: Optional[str] = None
    categorie_inv_accounting: Optional[str] = None
    
    # Caractéristiques techniques
    suivi_par_num_serie_oui_non: Optional[str] = None
    stocksecu_inv_oui_non: Optional[str] = None
    article_hors_normes: Optional[str] = None
    peremption: Optional[str] = None
    retour_production: Optional[str] = None
    est_oc_ou_ol: Optional[str] = None
    a_retrofiter: Optional[str] = None
    
    # Caractéristiques logistiques
    affretement: Optional[str] = None
    fragile: Optional[str] = None
    poids_article: Optional[float] = None
    volume_article: Optional[float] = None
    hauteur_article: Optional[float] = None
    longueur_article: Optional[float] = None
    largeur_article: Optional[float] = None
    matiere_dangereuse: Optional[str] = None
    md_code_onu: Optional[str] = None
    md_groupe_emballage: Optional[str] = None
    md_type_colis: Optional[str] = None
    
    # Prix et informations financières
    prix_achat_prev: Optional[float] = None
    pump: Optional[float] = None
    prix_eur_catalogue_article: Optional[float] = None
    compte_cg_achat: Optional[str] = None
    
    # Informations logistiques
    delai_approvisionnement: Optional[int] = None
    delai_de_reparation_contractuel: Optional[int] = None
    point_de_commande: Optional[int] = None
    quantite_a_commander: Optional[int] = None
    qte_cde_minimum_point_de_reappro: Optional[int] = None
    qte_minimum_ordre_de_commande: Optional[int] = None
    qte_maximum_ordre_de_commande: Optional[int] = None
    qte_min_de_l_article: Optional[int] = None
    qte_max_de_l_article: Optional[int] = None
    qte_cde_maximum_quantite_d_ordre_de_commande: Optional[int] = None
    
    # Réparation et maintenance
    lieu_de_reparation_pim: Optional[str] = None
    description_lieu_de_reparation: Optional[str] = None
    rma: Optional[str] = None
    role_responsable_et_equipement: Optional[str] = None
    mnemonique: Optional[str] = None
    
    # Métadonnées
    date_creation_article: Optional[datetime] = None
    nom_createur_article: Optional[str] = None
    date_derniere_modif_article: Optional[datetime] = None
    auteur_derniere_modif_article: Optional[str] = None


class Nomenclature(SQLModel, table=True):
    """Modèle pour les nomenclatures d'articles"""
    id : int | None = Field(default=None, primary_key=True)
    code_article_parent: str | None = Field(foreign_key="item.code_article")
    code_article_fils: str | None = Field(foreign_key="item.code_article")
    quantite: float | None 
 

class Image(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    code_article: str = Field(foreign_key="item.code_article")
    image: bytes = Field(sa_column=Column(LargeBinary))


class HeliosIG(SQLModel, table=True):
    code_ig: str = Field(primary_key=True)
    libelle_court_ig: str
    libelle_long_ig: Optional[str]
    adresse: Optional[str]
    code_postal: Optional[str]
    commune: Optional[str]
    categorie: Optional[str]
    libelle_entite: Optional[str]
    nature_infrastructure: Optional[str]
    etat_ig: Optional[str]
    date_creation: Optional[datetime]
    latitude: Optional[float]
    longitude: Optional[float]
    importance_site: Optional[str]
    classement_1u: Optional[str]
    reserves_1u: Optional[str]
    zone_2u: Optional[str]

class HeliosIGEquip(SQLModel, table=True):
    id: int = Field(primary_key=True)
    code_ig: str = Field(foreign_key="heliosig.code_ig")
    code_pop: Optional[str] = Field(foreign_key="pop.code_pop")
    code_pno: Optional[str] = Field(foreign_key="pno.code_pno")
    code_sla: Optional[str] = Field(foreign_key="sla.code_sla")
    code_article: Optional[str] = Field(foreign_key="item.code_article")
    nb_sites_active: Optional[int]
    nb_sites_demontes: Optional[int]
    nb_sites_desactives: Optional[int]
    nb_sites_en_cours_desinstallation: Optional[int]
    nb_sites_en_cours_installation: Optional[int]
    nb_sites_en_cours_modification: Optional[int]
    actif: Optional[bool]


class Pop(SQLModel, table=True):
    code_pop: str = Field(primary_key=True)
    libelle_pop: str
    statut_pop: str


class Pno(SQLModel, table=True):
    code_pno: str = Field(primary_key=True)
    pno_description: Optional[str]
    pno_description_detaille: Optional[str]


class Sla(SQLModel, table=True):
    code_sla: str = Field(primary_key=True)
    nom_sla: str
    description_detaille_sla: Optional[str]


class Cco(SQLModel, table=True):
    code_cco: str = Field(primary_key=True)
    cco_description: str
    cco_type: Optional[str]


class Store(SQLModel, table=True):
    code_magasin: Optional[str] = Field(primary_key=True)
    code_magasin_daher: Optional[str]
    nom_magasin: Optional[str]
    statut: int
    categorie_magasin: Optional[str]
    contact: Optional[str]
    matricule_rh: Optional[str]
    equipe: Optional[str]
    region: Optional[str]
    nom_responsable: Optional[str]
    prenom_responsable: Optional[str]
    mail_responsable: Optional[str]
    telephone: Optional[str]
    email: Optional[str]
    adresse_1: Optional[str]
    adresse_2: Optional[str]
    code_postal: Optional[str]
    ville: Optional[str]
    pr_principal: Optional[str] = Field(foreign_key="pudo.code_point_relais")
    pr_backup: Optional[str] = Field(foreign_key="pudo.code_point_relais")
    pr_hors_norme: Optional[str] = Field(foreign_key="pudo.code_point_relais")
    code_ig: Optional[str] = Field(foreign_key="heliosig.code_ig")
    code_transporteur_inf_35kg: Optional[str]
    code_produit_inf_35kg: Optional[str]
    code_transporteur_sup_35kg: Optional[str]
    code_produit_sup_35kg: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    date_creation: Optional[datetime]
    user_creation: Optional[str]
    mnemonique_technicien: Optional[str]
    amont_aval: Optional[str]
    mag_18DE_95MG_18BB_44AA_MBRO: Optional[bool]
    mag_MMHS_MVEO_MTRA_MPER: Optional[bool]


class AddressGps(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    adresse: str
    latitude: Optional[float]
    longitude: Optional[float]

class Pudo(SQLModel, table=True):
    code_point_relais: Optional[str] = Field(primary_key=True)
    nom_point_relais: Optional[str]
    code_transporteur: Optional[str]
    eligibilite: Optional[str]
    flag_hors_norme: Optional[int]
    flag_actif: Optional[int]
    adresse_1: Optional[str]
    adresse_2: Optional[str]
    adresse_3: Optional[str]
    adresse_4: Optional[str]
    code_postal: Optional[str]
    ville: Optional[str]
    categorie_de_point_relais: Optional[str]
    code_activite: Optional[str]


class Min_Max(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    code_magasin: Optional[str] = Field(foreign_key="store.code_magasin")
    code_article: Optional[str] = Field(foreign_key="item.code_article")
    flag_gestion: Optional[str]
    qte_min: Optional[int]
    qte_max: Optional[int]

class PE(SQLModel, table=True):
    code_projet: Optional[str] = Field(primary_key=True)
    libelle_projet: Optional[str]
    description_projet: Optional[str]
    code_nature: Optional[str]
    libelle_nature: Optional[str]
    statut: Optional[str]
    supprime: Optional[str]
    code_projet_industrie: Optional[str] = Field(foreign_key="pj.code_projet")
    commercial_email: Optional[str]
    responsable: Optional[str]
    responsable_email: Optional[str]
    code_ig: Optional[str] = Field(foreign_key="heliosig.code_ig")
    code_programme: Optional[str]
    libelle_programe: Optional[str]
    code_projet_budgetaire: Optional[str]
    libelle_projet_budgetaire: Optional[str]


class PJ(SQLModel, table=True):
    code_projet: Optional[str] = Field(primary_key=True)
    libelle_projet: Optional[str]
    projet_retraite: Optional[str]
    description_projet: Optional[str]
    statut: Optional[str]
    chef_projet: Optional[str]
    chef_projet_email: Optional[str]
    code_programme: Optional[str]
    libelle_programme: Optional[str]
    responsable_programme: Optional[str]
    code_programme_bugetaire: Optional[str]
    libelle_programme_budgetaire: Optional[str]
    code_projet_budgetaire: Optional[str]

class Stock(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    date_stock: Optional[datetime]
    code_magasin: Optional[str] = Field(foreign_key="store.code_magasin")
    flag_stock_d_m: Optional[str]
    emplacement: Optional[str]
    transit: Optional[str]
    code_article: Optional[str] = Field(foreign_key="item.code_article")
    n_lot: Optional[str]
    n_serie: Optional[str]
    n_version: Optional[str]
    qualite: Optional[str]
    qte_stock: Optional[str]
    date_reception: Optional[datetime]
    delai_anciennete: Optional[int]
    code_projet: Optional[str]
    n_cde_dpm_dpi: Optional[str]
    n_ums: Optional[str]
    n_bt: Optional[str]
    ig_installation: Optional[str]
    commentaire_operateur_tdf: Optional[str]


class Movement(SQLModel, table=True):
    n_mvt: Optional[int] = Field(default=None ,primary_key=True)
    date_mvt: Optional[datetime] = None
    code_motif_mvt: Optional[int] = None
    lib_motif_mvt: Optional[str] = None
    code_article: Optional[str] = Field(default=None, foreign_key="item.code_article")
    n_lot: Optional[str] = None
    n_serie: Optional[str] = None
    qualite: Optional[str] = None
    sens_mvt: Optional[str] = None
    magasin: Optional[str] = None
    emplacement: Optional[str] = None
    magasin_destinataire: Optional[str] = None
    n_cde: Optional[str] = None
    code_projet: Optional[str] = None
    n_cde_speed: Optional[str] = None
    n_ligne_cde_speed: Optional[str] = None
    n_cde_dpm_dpi: Optional[str] = None
    code_ig_intervention: Optional[str] = Field(default=None, foreign_key="heliosig.code_ig")
    n_bt: Optional[str] = None
    n_rma: Optional[str] = None
    flag_panne_sur_stock: Optional[str] = None
    n_dossier_reparation: Optional[int] = None
    cause_retour_equipement: Optional[str] = None
    

class TranscoStoreOracleSpeed(SQLModel, table=True):
    code_magasin_oracle: Optional[str] = Field(default=None, primary_key=True)
    libelle_magasin_oracle: Optional[str]
    code_magasin_daher: Optional[str]
    code_magasin_speed: Optional[str] = Field(default=None, foreign_key="store.code_magasin")





