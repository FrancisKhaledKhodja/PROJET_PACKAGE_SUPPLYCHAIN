# Package Supply Chain

Ce package Python est conçu pour gérer et analyser les données de la chaîne d'approvisionnement. Il fournit des outils pour le traitement des données de stock, des mouvements de marchandises, et la génération de rapports d'analyse.

## 🚀 Fonctionnalités

- Lecture et traitement de fichiers Excel et CSV
- Gestion des données de stock et historique
- Analyse des mouvements Oracle et de la vitesse de rotation
- Gestion des référentiels magasins
- Traitement des nomenclatures et des items
- Génération de rapports et statistiques

## 📋 Prérequis

- Python >= 3.13
- Environnement virtuel recommandé

## 📦 Installation des dépendances

```bash
pip install -r requirements.txt
```

Principales dépendances :
- polars-lts-cpu >= 1.31.0
- loguru >= 0.7.3
- python-dotenv >= 1.1.0
- fastexcel >= 0.14.0
- plotly >= 6.1.2

## 🔧 Configuration

1. Créez un fichier `.env` à la racine du projet
2. Configurez les variables d'environnement nécessaires

## 📁 Structure du projet

- `data_input/` : Dossier pour les fichiers d'entrée
- `data_output/` : Dossier pour les fichiers de sortie
- `package_supply_chain/` : Code source principal
  - `api/` : Endpoints et fonctions d'API
  - Modules de traitement des données
- `notebooks/` : Notebooks Jupyter pour l'analyse

## 🚀 Utilisation

```python
# Exemple d'utilisation du package
from package_supply_chain.stock import LoadStock
from package_supply_chain.helios import Helios

# Voir main.py pour des exemples d'utilisation complets
```

## 📊 Génération de rapports

Le package peut générer différents types de rapports :
- États des stocks
- Mouvements de marchandises
- Statistiques de production
- Analyses de priorité

## 📝 Logs

Les logs sont générés dans `application.log` avec différents niveaux de détail grâce à loguru.

## 🤝 Contribution

Pour contribuer au projet :
1. Créez une branche pour votre fonctionnalité
2. Committez vos changements
3. Ouvrez une Pull Request

## 📄 Licence

Ce projet est sous licence propriétaire. Tous droits réservés.