# AEP_HARMONISE_DB

Ce projet contient un script Python permettant de créer la structure de la base de données **AEP_HARMONISE** et d’intégrer les données dans PostgreSQL avec l’extension PostGIS.  
La base est conçue pour modéliser les réseaux d’adduction d’eau potable : captages, stations de traitement, réservoirs, conduites, points de distribution, ainsi que les volumes d’eau (brute, traitée, distribuée).

---

## 📂 Contenu
- `1_creation_base.py` : script Python qui crée les tables, types, contraintes et relations de la base.  
- `2_commune.py` : intégration des données sur la limite administrative **Commune**.  
- `README.md` : documentation du projet.  

---

## ⚙️ Prérequis
Avant d’utiliser le script, assurez-vous d’avoir installé :
- **PostgreSQL** ≥ 17  
- **PostGIS** ≥ 3  
- **Python** ≥ 3.8  
- La librairie Python `psycopg2`  

👉 Pour installer `psycopg2` :  
```bash
pip install psycopg2
