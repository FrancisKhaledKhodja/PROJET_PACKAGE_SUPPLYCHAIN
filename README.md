# Package Supply Chain

Ce package Python est conçu pour gérer et analyser les données de la chaîne d'approvisionnement. Il fournit des outils avancés pour le traitement des données de stock, l'analyse des mouvements, et la génération de rapports détaillés.

## 🚀 Fonctionnalités Principales

### Gestion des Données
- Lecture et traitement automatisé de fichiers Excel et CSV
- Conversion et stockage optimisé en format Parquet
- Gestion des données de stock en temps réel et historique
- Traitement des nomenclatures et référencement des items

### Analyses et Rapports
- Analyse des mouvements Oracle et vitesse de rotation
- Suivi des stocks et historique détaillé
- Statistiques de production et consommation
- Analyses de priorité et listes photos
- Traçabilité des retours de production
- Analyse des items M vers D
- Origine des items et traçabilité

### Gestion des Référentiels
- Gestion complète des référentiels magasins
- Suivi des équipements Helios
- Gestion des bâtiments et emplacements

## 📋 Prérequis

- Python >= 3.13
- Environnement virtuel recommandé

## 📦 Installation

Il est recommandé d'utiliser `uv` pour une installation rapide des dépendances :

1. Installation de uv :
```bash
pip install uv
```

2. Installation des dépendances avec uv :
```bash
uv pip install -r requirements.txt
```

Alternativement, vous pouvez utiliser pip classique :
```bash
pip install -r requirements.txt
```

### Dépendances Principales
- polars-lts-cpu >= 1.31.0 (Traitement de données haute performance)
- loguru >= 0.7.3 (Logging avancé)
- python-dotenv >= 1.1.0 (Gestion des variables d'environnement)
- fastexcel >= 0.14.0 (Lecture/écriture Excel optimisée)
- plotly >= 6.1.2 (Visualisation de données)

### Pourquoi uv ?
- Installation 10-100x plus rapide que pip
- Gestion optimisée des dépendances
- Compatible avec les fichiers requirements.txt standards

## 🔧 Configuration

1. Créez un fichier `.env` à la racine du projet
2. Configurez les variables d'environnement nécessaires :
   - Chemins des dossiers data_input et data_output
   - Paramètres de connexion aux bases de données
   - Autres configurations spécifiques

## 📁 Structure du Projet

```
project/
├── data_input/
│   └── QUOTIDIEN/      # Données quotidiennes
├── data_output/        # Rapports et analyses générés
├── package_supply_chain/
│   ├── api/           # Endpoints et fonctions d'API
│   ├── stock/         # Gestion des stocks
│   ├── helios/        # Interface Helios
│   └── ...           # Autres modules
├── notebooks/         # Notebooks d'analyse
└── main.py           # Point d'entrée principal
```

## 🚀 Utilisation

```python
from package_supply_chain.api.api_mvt import get_movements_oracle_and_speed
from package_supply_chain.api.api_stock import get_state_stocks
from package_supply_chain.api.api_priority_list_photo import get_priority_list_photo

# Exemple: Obtenir l'état des stocks
stocks = get_state_stocks(output_folder)

# Exemple: Analyser les mouvements
movements = get_movements_oracle_and_speed(output_folder)

# Voir main.py pour des exemples complets
```

## 📊 Types de Rapports Générés

- États des stocks et historique
- Mouvements Oracle et vitesse de rotation
- Statistiques de production et consommation
- Analyses de priorité et listes photos
- Rapports de traçabilité des items
- Analyses des retours de production
- États des référentiels magasins

## 📝 Logging

Le système de logging utilise loguru pour une traçabilité complète :
- Logs détaillés dans `application.log`
- Niveaux de log configurables
- Rotation automatique des fichiers de log

## 🤝 Contribution

1. Créez une branche pour votre fonctionnalité
2. Committez vos changements avec des messages descriptifs
3. Testez vos modifications
4. Ouvrez une Pull Request avec une description détaillée

## 📄 Licence

Ce projet est sous licence propriétaire. Tous droits réservés.