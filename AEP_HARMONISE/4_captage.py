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
        logging.FileHandler('migration_captage.log'),
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

def format_libelle(original):
    """Convertit le libellé en majuscules et le nettoie"""
    if not original:
        return None
    
    # Convertir en majuscules et supprimer les espaces superflus
    formatted = ' '.join(str(original).upper().split())
    
    # Tronquer à 50 caractères si nécessaire
    return formatted[:50] if formatted else None

def find_quartier_id(target_cur, geom):
    """Trouve l'ID du quartier contenant le MultiPolygon"""
    try:
        target_cur.execute("""
            SELECT id_quartier 
            FROM quartier 
            WHERE ST_Contains(geom, %s)
            LIMIT 1;
        """, (geom,))
        result = target_cur.fetchone()
        return result[0] if result else None
    except psycopg2.Error as e:
        logging.error(f"Erreur recherche quartier: {e}")
        return None

#  Migration principale 
def migrate_captage():
    source_conn = target_conn = None
    stats = {'total': 0, 'success': 0, 'skipped': 0, 'errors': 0}
    captage_mapping = {}  # Pour stocker les anciens IDs vers nouveaux IDs

    try:
        # Connexions
        source_conn = connect_db(DB_CONFIG_SOURCE_JIRAMA, "Source JIRAMA")
        target_conn = connect_db(DB_CONFIG_TARGET, "Cible HARMONISE")
        
        with source_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as source_cur, \
             target_conn.cursor() as target_cur, \
             target_conn.cursor() as lookup_cur:

            # 1. Récupération données source
            source_cur.execute("""
                SELECT gid, id_capt, type, geom
                FROM captage
                WHERE geom IS NOT NULL;
            """)
            stats['total'] = source_cur.rowcount
            logging.info(f"{stats['total']} captages à migrer")

            # 2. Migration
            for row in source_cur:
                try:
                    # Vérification géométrie
                    if not row['geom']:
                        stats['skipped'] += 1
                        continue

                    # Recherche quartier
                    quartier_id = find_quartier_id(lookup_cur, row['geom'])
                    if not quartier_id:
                        stats['skipped'] += 1
                        continue

                    # Formatage du libellé
                    libelle_source = row['id_capt'] or f"CAPT_{row['gid']}"
                    libelle_final = format_libelle(libelle_source)
                    logging.debug(f"Libellé transformé: {libelle_source} -> {libelle_final}")

                    # Insertion
                    target_cur.execute("""
                        INSERT INTO captage (
                            libelle_capt, type_capt, debit_capt, 
                            date_mes, geom, id_quartier
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id_capt;
                    """, (
                        libelle_final,
                        (row['type'] or '')[:60],
                        None,  # debit_capt
                        None,  # date_mes
                        row['geom'],
                        quartier_id
                    ))
                    
                    new_id = target_cur.fetchone()[0]
                    captage_mapping[row['gid']] = new_id
                    stats['success'] += 1
                    logging.debug(f"Migré: {row['gid']} -> {new_id}")

                except Exception as e:
                    stats['errors'] += 1
                    logging.error(f"Erreur sur captage {row['gid']}: {str(e)}")
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
    logging.info("Début migration captage")
    try:
        migrate_captage()
        logging.info("Migration réussie")
    except Exception as e:
        logging.critical("Échec migration: %s", str(e))