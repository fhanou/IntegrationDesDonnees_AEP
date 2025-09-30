import psycopg2
import psycopg2.extras
import json
import logging
import os
import re
from psycopg2 import sql

# --- CONFIGURATION ---

# Configuration de la base de données cible
DB_CONFIG_TARGET = {
    "database": "AEP_HARMONISE",
    "user": "postgres",
    "password": "*******",
    "host": "localhost",
    "port": "5432"
}

# Chemin vers le fichier GeoJSON des quartiers (utilisation de os.path.join pour la portabilité)
GEOJSON_PATH_QUARTIER = os.path.join("quartier_rhm.geojson")

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- FONCTIONS UTILITAIRES (inchangées) ---

def connect_db(config, name):
    """Établit une connexion à une base de données."""
    try:
        conn = psycopg2.connect(**config)
        logging.info(f"Connecté à la base de données '{name}' ({config['database']})")
        return conn
    except psycopg2.DatabaseError as e:
        logging.error(f"Erreur de connexion à '{name}': {e}")
        raise

def close_db(conn, name, cursor=None):
    """Ferme le curseur et la connexion."""
    if cursor:
        cursor.close()
    if conn:
        db_name = conn.dsn.split('dbname=')[1].split(' ')[0]
        conn.close()
        logging.info(f"Connexion à '{name}' ({db_name}) fermée.")

def load_geojson(file_path):
    """Charge et parse le fichier GeoJSON."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logging.info(f"Fichier GeoJSON chargé: {len(data['features'])} features trouvées dans {file_path}")
        return data
    except Exception as e:
        logging.error(f"Erreur lors du chargement du GeoJSON {file_path}: {e}")
        raise

def parse_numeric_value(value):
    """Parse une valeur numérique, gère les chaînes vides et les conversions."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        cleaned_value = str(value).strip()
        if not cleaned_value:
            return None
        cleaned_value = cleaned_value.replace(' ', '').replace(',', '.')
        cleaned_value = re.sub(r'[^\d\.\-]', '', cleaned_value)
        if cleaned_value and cleaned_value != '-':
            return float(cleaned_value) if '.' in cleaned_value else int(cleaned_value)
        return None
    except (ValueError, TypeError) as e:
        logging.warning(f"Impossible de convertir la valeur '{value}' en numérique: {e}")
        return None


# --- FONCTION DE MIGRATION POUR QUARTIER ---

def migrate_quartier_from_geojson(target_conn, geojson_data):
    """Migre les données du GeoJSON vers la table AEP_HARMONISE.quartier."""
    logging.info("--- Début Migration: quartier depuis GeoJSON ---")
    
    target_cursor = target_conn.cursor()
    processed_count = 0
    inserted_count = 0
    error_count = 0

    try:
        # Préparer la requête d'insertion
        # On insère les données et on laisse la BDD générer le 'id_quartier'
        sql_insert = sql.SQL("""
            INSERT INTO quartier (
                id_com, code_quartier, lib_quartier, area_km2, nb_habitant, geom
            ) VALUES (
                %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 29702)
            ) RETURNING id_quartier;
        """)

        # Itérer sur chaque feature du GeoJSON
        for feature in geojson_data['features']:
            processed_count += 1
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            
            # Utiliser un identifiant unique pour le logging
            feature_id = properties.get('code_quartier', f'feature_{processed_count}')

            try:
                # --- Transformations et validations des données ---
                
                # id_com: INTEGER NOT NULL
                id_com_val = properties.get('id_com')
                if id_com_val is None:
                    logging.error(f"Feature '{feature_id}': 'id_com' est NULL. Ligne ignorée.")
                    error_count += 1
                    continue
                
                # code_quartier: VARCHAR(50) UNIQUE NOT NULL
                code_quartier_val = properties.get('code_quartier')
                if code_quartier_val is None:
                    logging.error(f"Feature (id_com={id_com_val}): 'code_quartier' est NULL. Ligne ignorée.")
                    error_count += 1
                    continue
                code_quartier_val = str(code_quartier_val)[:50]

                # lib_quartier: VARCHAR(50)
                lib_quartier_val = properties.get('lib_quartier')
                if lib_quartier_val is not None:
                    lib_quartier_val = str(lib_quartier_val)[:50]

                # area_km2: NUMERIC
                area_km2_val = parse_numeric_value(properties.get('area_km2'))

                # nb_habitant: INTEGER
                nb_habitant_val = parse_numeric_value(properties.get('nb_habitant'))
                if nb_habitant_val is not None:
                    nb_habitant_val = int(nb_habitant_val)

                # geom: conversion de GeoJSON en geometry PostGIS
                geom_json = json.dumps(geometry) if geometry else None
                if geom_json is None:
                    logging.error(f"Feature '{feature_id}': Géométrie manquante. Ligne ignorée.")
                    error_count += 1
                    continue

                # Exécution de l'insertion
                target_cursor.execute(sql_insert, (
                    id_com_val,
                    code_quartier_val,
                    lib_quartier_val,
                    area_km2_val,
                    nb_habitant_val,
                    geom_json
                ))

                # Récupérer le nouvel ID généré
                new_id_quartier = target_cursor.fetchone()[0]
                inserted_count += 1
                logging.info(f"  -> Inséré: Feature code_quartier='{code_quartier_val}' -> Nouveau id_quartier={new_id_quartier}")

            except psycopg2.Error as e:
                # En cas d'erreur (ex: id_com non trouvé), on annule la transaction
                logging.error(f"Erreur PostgreSQL lors de l'insertion de la feature '{feature_id}': {e}")
                target_conn.rollback()
                error_count += 1
                # On arrête le script en cas d'erreur de BDD pour ne pas continuer avec des données potentiellement corrompues
                raise
            except Exception as ex:
                logging.error(f"Erreur Python inattendue lors du traitement de la feature '{feature_id}': {ex}")
                target_conn.rollback()
                error_count += 1
                raise
        
        # Si tout s'est bien passé, on valide toutes les insertions
        target_conn.commit()

    except Exception as e:
        logging.error(f"Erreur majeure pendant la migration des quartiers: {e}")
        # Assurer un rollback en cas d'erreur critique
        target_conn.rollback()
    finally:
        logging.info(f"--- Fin Migration: quartier depuis GeoJSON ---")
        logging.info(f"Statistiques: Features traitées={processed_count}, Insérées={inserted_count}, Erreurs={error_count}")
        if target_cursor:
            target_cursor.close()


# --- FONCTION PRINCIPALE ---

def main():
    """Orchestre la migration pour la table quartier depuis GeoJSON."""
    target_conn = None
    try:
        # Charger le fichier GeoJSON des quartiers
        geojson_data_quartier = load_geojson(GEOJSON_PATH_QUARTIER)
        
        # Connexion à la base de données cible
        target_conn = connect_db(DB_CONFIG_TARGET, "Cible HARMONISE")

        # Exécuter la migration pour les quartiers
        migrate_quartier_from_geojson(target_conn, geojson_data_quartier)

        logging.info("Migration de la table 'quartier' depuis GeoJSON terminée avec succès.")

    except FileNotFoundError:
        logging.error(f"ERREUR CRITIQUE: Le fichier GeoJSON '{GEOJSON_PATH_QUARTIER}' n'a pas été trouvé.")
    except Exception as e:
        logging.error(f"Une erreur critique est survenue lors du processus principal: {e}")
    finally:
        # Fermer la connexion
        if target_conn:
            close_db(target_conn, "Cible HARMONISE")

if __name__ == "__main__":
    main()