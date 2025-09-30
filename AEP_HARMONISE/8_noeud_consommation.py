import os
import json
import psycopg2
import logging
from psycopg2.extras import Json

# Configuration
DB_CONFIG = {
    "database": "AEP_HARMONISE",
    "user": "postgres",
    "password": "*******",
    "host": "localhost",
    "port": "5432"
}

GEOJSON_FILE = "noeud_consommation.geojson"
SRID = 29702

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_noeud_consommation.log'),
        logging.StreamHandler()
    ]
)

def connect_db(config):
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        logging.info("Connexion à la base de données réussie")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Échec de la connexion à la base de données: {e}")
        raise

def load_geojson(file_path):
    """Charge les données depuis le fichier GeoJSON"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if data.get('type') != 'FeatureCollection':
            raise ValueError("Le fichier GeoJSON doit être de type FeatureCollection")
            
        features = data.get('features', [])
        logging.info(f"Fichier GeoJSON chargé: {len(features)} features trouvées")
        return features
        
    except Exception as e:
        logging.error(f"Erreur lors du chargement du GeoJSON: {e}")
        raise

def transform_geometry(feature):
    """Transforme la géométrie GeoJSON en WKT"""
    try:
        geometry = feature.get('geometry')
        if not geometry or geometry.get('type') != 'Point':
            raise ValueError("Geometry manquante ou n'est pas un Point")
            
        coordinates = geometry.get('coordinates')
        if not coordinates or len(coordinates) != 2:
            raise ValueError("Coordonnées invalides")
            
        lon, lat = coordinates
        return f"POINT({lon} {lat})"
        
    except Exception as e:
        logging.error(f"Erreur de transformation de la géométrie: {e}")
        raise

def migrate_noeud_consommation():
    """Migre les données de noeud_consommation depuis le GeoJSON"""
    conn = None
    stats = {'total': 0, 'inserted': 0, 'errors': 0, 'skipped': 0}
    
    try:
        # Chemin complet du fichier GeoJSON
        script_dir = os.path.dirname(os.path.abspath(__file__))
        geojson_path = os.path.join(script_dir, GEOJSON_FILE)
        
        if not os.path.exists(geojson_path):
            raise FileNotFoundError(f"Fichier GeoJSON introuvable: {geojson_path}")
        
        # Chargement des données
        features = load_geojson(geojson_path)
        stats['total'] = len(features)
        
        if stats['total'] == 0:
            logging.warning("Aucune donnée à migrer dans le fichier GeoJSON")
            return stats
        
        # Connexion à la base
        conn = connect_db(DB_CONFIG)
        
        with conn.cursor() as cursor:
            for feature in features:
                try:
                    # Extraction des propriétés
                    properties = feature.get('properties', {})
                    libelle = properties.get('libelle')
                    troncon = properties.get('id_troncon') or None  # NULL si manquant
                    
                    # Validation des données obligatoires
                    if not libelle:
                        stats['skipped'] += 1
                        logging.warning(f"Feature ignorée (libelle manquant): {properties}")
                        continue
                    
                    # Préparation de la géométrie
                    geom_wkt = transform_geometry(feature)
                    
                    # Insertion dans la base (avec gestion NULL pour troncon)
                    cursor.execute("""
                        INSERT INTO noeud_consommation (libelle, troncon, geom)
                        VALUES (%s, %s, ST_Transform(ST_SetSRID(ST_GeomFromText(%s), 4326), %s))
                        RETURNING id_noeud_cons;
                    """, (libelle, troncon, geom_wkt, SRID))
                    
                    inserted_id = cursor.fetchone()[0]
                    stats['inserted'] += 1
                    logging.info(f"Noeud inséré - ID: {inserted_id}, Libellé: {libelle}")
                    
                except Exception as e:
                    stats['errors'] += 1
                    conn.rollback()  # Annule seulement la transaction courante
                    feature_id = properties.get('id', 'inconnu')
                    logging.error(f"Erreur sur la feature {feature_id}: {str(e)}")
                    continue
            
            conn.commit()
            logging.info(f"Migration terminée. Statistiques: Total={stats['total']}, Insérés={stats['inserted']}, Erreurs={stats['errors']}, Ignorés={stats['skipped']}")
            return stats
            
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"ERREUR GLOBALE: {str(e)}", exc_info=True)
        raise
    finally:
        if conn: conn.close()
        logging.info("Connexion à la base de données fermée")

if __name__ == "__main__":
    logging.info("Début de la migration des noeuds tronçons depuis GeoJSON")
    try:
        results = migrate_noeud_consommation()
        if results['errors'] == 0:
            logging.info("Migration terminée avec succès")
        else:
            logging.warning(f"Migration terminée avec {results['errors']} erreurs")
    except Exception as e:
        logging.critical(f"Échec critique de la migration: {str(e)}")