#  SCRIPT REMPLISSAGE RESERVOIR_RESERVOIR 

import psycopg2
import logging
from psycopg2 import sql

#  Configuration 
DB_CONFIG = {
    "database": "AEP_HARMONISE",
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
        logging.FileHandler('remplissage_reservoir_reservoir.log'),
        logging.StreamHandler()
    ]
)

#  Fonctions Utilitaires 
def connect_db(config):
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        logging.info("Connexion à la base de données réussie")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Échec de connexion à la base de données: {e}")
        raise

def close_db(conn):
    if conn:
        try:
            conn.close()
            logging.info("Connexion à la base de données fermée")
        except psycopg2.Error as e:
            logging.error(f"Erreur lors de la fermeture de la connexion: {e}")

def get_reservoir_id(cur, libelle):
    """Récupère l'ID d'un réservoir par son libellé (insensible à la casse et aux espaces)"""
    try:
        cur.execute("""
            SELECT id_reservoir FROM reservoir 
            WHERE UPPER(TRIM(libelle)) = UPPER(TRIM(%s))
        """, (libelle,))
        result = cur.fetchone()
        return result[0] if result else None
    except psycopg2.Error as e:
        logging.error(f"Erreur lors de la recherche du réservoir {libelle}: {e}")
        return None

#  Remplissage des relations 
def fill_reservoir_reservoir_relations():
    conn = None
    try:
        conn = connect_db(DB_CONFIG)
        cur = conn.cursor()

        # Liste des relations à créer (source, destination)
        relations = [
            ("ROVA", "MORTHOMME"),
            ("MORTHOMME", "ILAINDASTRA"),
            ("ILAINDASTRA", "mahamanina")
        ]

        stats = {'total': len(relations), 'success': 0, 'errors': 0}

        for source_libelle, dest_libelle in relations:
            try:
                # Récupération des IDs
                source_id = get_reservoir_id(cur, source_libelle)
                dest_id = get_reservoir_id(cur, dest_libelle)

                if not source_id:
                    logging.error(f"Réservoir source non trouvé: {source_libelle}")
                    stats['errors'] += 1
                    continue

                if not dest_id:
                    logging.error(f"Réservoir destination non trouvé: {dest_libelle}")
                    stats['errors'] += 1
                    continue

                # Vérification si la relation existe déjà
                cur.execute("""
                    SELECT 1 FROM reservoir_reservoir 
                    WHERE id_reservoir_source = %s AND id_reservoir_destination = %s
                """, (source_id, dest_id))
                
                if cur.fetchone():
                    logging.info(f"Relation existe déjà: {source_libelle}({source_id}) -> {dest_libelle}({dest_id})")
                    stats['success'] += 1
                    continue

                # Insertion de la relation
                cur.execute("""
                    INSERT INTO reservoir_reservoir (
                        id_reservoir_source, 
                        id_reservoir_destination
                    ) VALUES (%s, %s)
                """, (source_id, dest_id))

                stats['success'] += 1
                logging.info(f"Relation créée: {source_libelle}({source_id}) -> {dest_libelle}({dest_id})")

            except psycopg2.Error as e:
                conn.rollback()
                stats['errors'] += 1
                logging.error(f"Erreur lors de la création de la relation {source_libelle}->{dest_libelle}: {e}")
                continue

        conn.commit()
        logging.info(f"Remplissage terminé. Statistiques: {stats}")

    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"ERREUR GLOBALE: {str(e)}", exc_info=True)
        raise
    finally:
        if conn: close_db(conn)

if __name__ == "__main__":
    logging.info("Début du remplissage des relations réservoir-réservoir")
    try:
        fill_reservoir_reservoir_relations()
        logging.info("Remplissage des relations terminé avec succès")
    except Exception as e:
        logging.critical(f"Échec du remplissage: {str(e)}")