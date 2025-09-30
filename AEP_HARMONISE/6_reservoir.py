#  SCRIPT MIGRATION RESERVOIR 

import psycopg2
import psycopg2.extras
import logging
from psycopg2 import sql
import traceback

#  Configuration 
DB_CONFIG_TARGET = {
    "database": "AEP_HARMONISE",
    "user": "postgres",
    "password": "*******",
    "host": "localhost",
    "port": "5432"
}

DB_CONFIG_SOURCE_JIRAMA = {
    "database": "AEP_JIRAMA",
    "user": "postgres",
    "password": "*******",
    "host": "localhost",
    "port": "5432"
}

#  Logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_reservoir.log'),
        logging.StreamHandler()
    ]
)

#  Fonctions Utilitaires 
def connect_db(config, name):
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        logging.info(f"Connexion réussie à {name} ({config['database']})")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Échec connexion à {name}: {e}")
        raise

def close_db(conn, name):
    if conn:
        try:
            conn.close()
            logging.info(f"Connexion à {name} fermée")
        except psycopg2.Error as e:
            logging.error(f"Erreur fermeture {name}: {e}")

def find_quartier_id(target_cur, geom_point):
    """Trouve l'ID du quartier contenant le point du réservoir"""
    try:
        target_cur.execute("""
            SELECT id_quartier 
            FROM quartier 
            WHERE ST_Contains(geom, %s)
            LIMIT 1;
        """, (geom_point,))
        result = target_cur.fetchone()
        return result[0] if result else None
    except psycopg2.Error as e:
        logging.error(f"Erreur recherche quartier: {e}")
        return None

def convert_volume(volume_str):
    """Convertit le volume de type string (ex: '500 m3') en numeric"""
    if not volume_str:
        return None
    try:
        # Supprime ' m3' et convertit en float
        return float(volume_str.replace(' m3', '').strip())
    except (ValueError, TypeError):
        logging.warning(f"Conversion impossible du volume: {volume_str}")
        return None

#  Migration principale 
def migrate_reservoir():
    source_conn = target_conn = None
    stats = {'total': 0, 'success': 0, 'skipped': 0, 'errors': 0, 'no_quartier': 0}
    
    try:
        # Connexions
        source_conn = connect_db(DB_CONFIG_SOURCE_JIRAMA, "Source JIRAMA")
        target_conn = connect_db(DB_CONFIG_TARGET, "Cible HARMONISE")
        
        with source_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as source_cur, \
             target_conn.cursor() as target_cur, \
             target_conn.cursor() as lookup_cur:

            # 1. Récupération données source
            source_cur.execute("""
                SELECT id_reservoir, capacite, geom
                FROM "Reservoir"
                WHERE geom IS NOT NULL;
            """)
            stats['total'] = source_cur.rowcount
            logging.info(f"{stats['total']} réservoirs à migrer")

            # 2. Migration
            for row in source_cur:
                try:
                    # Vérification géométrie
                    if not row['geom']:
                        stats['skipped'] += 1
                        continue

                    # Recherche quartier parent
                    quartier_id = find_quartier_id(lookup_cur, row['geom'])
                    if not quartier_id:
                        stats['no_quartier'] += 1
                        logging.warning(f"Aucun quartier trouvé pour le réservoir {row['id_reservoir']}")
                        continue

                    # Conversion des données
                    libelle = row['id_reservoir'].upper()[:50]  # Conversion en majuscules et limitation à 50 caractères
                    volume_m3 = convert_volume(row['capacite'])

                    # Insertion
                    target_cur.execute("""
                        INSERT INTO reservoir (
                            libelle, materiel, volume_m3, 
                            geom, id_quartier
                        ) VALUES (%s, %s, %s, %s, %s)
                        RETURNING id_reservoir;
                    """, (
                        libelle,           # libelle (en majuscules)
                        None,              # materiel (non disponible dans la source)
                        volume_m3,         # volume converti
                        row['geom'],       # géométrie
                        quartier_id        # quartier
                    ))
                    
                    new_id = target_cur.fetchone()[0]
                    stats['success'] += 1
                    logging.info(f"Réservoir migré: {row['id_reservoir']} -> {new_id} (Quartier: {quartier_id}, Volume: {volume_m3}m3)")

                except Exception as e:
                    stats['errors'] += 1
                    logging.error(f"Erreur sur réservoir {row.get('id_reservoir')}: {str(e)}")
                    continue

            target_conn.commit()
            logging.info("Migration terminée. Stats: %s", stats)

    except Exception as e:
        if target_conn: target_conn.rollback()
        logging.error(f"ERREUR GLOBALE: {str(e)}", exc_info=True)
        raise
    finally:
        if source_conn: close_db(source_conn, "Source")
        if target_conn: close_db(target_conn, "Cible")

if __name__ == "__main__":
    logging.info("Début migration reservoir")
    try:
        migrate_reservoir()
        logging.info("Migration réussie")
    except Exception as e:
        logging.critical("Échec migration: %s", str(e))