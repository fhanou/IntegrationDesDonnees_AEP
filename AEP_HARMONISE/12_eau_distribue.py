import os
import csv
import psycopg2
import logging
from datetime import datetime

# Configuration de la base de données
DB_CONFIG = {
    "database": "AEP_HARMONISE",
    "user": "postgres",
    "password": "*******",
    "host": "localhost",
    "port": "5432"
}

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('import_eau_distribue.log'),
        logging.StreamHandler()
    ]
)

# Chemin du dossier contenant les fichiers CSV (répertoire du script)
DOSSIER_CSV = os.path.dirname(os.path.abspath(__file__))

def connect_db(config):
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        logging.info("Connexion à la base de données réussie")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Échec de la connexion à la base de données: {e}")
        raise

def get_point_dist_id(conn, ref_borne):
    """Trouve l'ID du point de distribution basé sur son ref borne"""
    if not ref_borne:
        return None
        
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id_point_dist 
                FROM point_de_distribution 
                WHERE ref_borne = %s
                LIMIT 1;
            """, (ref_borne.strip(),))
            result = cur.fetchone()
            return result[0] if result else None
    except psycopg2.Error as e:
        logging.error(f"Erreur lors de la recherche du point de distribution '{ref_borne}': {e}")
        return None

def import_csv_to_db():
    """Importe les données des fichiers CSV vers la table eau_distribue"""
    stats = {
        'total_files': 0,
        'total_rows': 0,
        'inserted': 0,
        'skipped': 0,
        'errors': 0,
        'points_not_found': 0,
        'null_dates': 0
    }
    
    conn = None
    try:
        # 1. Connexion à la base de données
        conn = connect_db(DB_CONFIG)
        
        # 2. Parcours des fichiers CSV dans le dossier
        for filename in os.listdir(DOSSIER_CSV):
            if not filename.lower().endswith('.csv'):
                continue
                
            filepath = os.path.join(DOSSIER_CSV, filename)
            stats['total_files'] += 1
            logging.info(f"Traitement du fichier: {filename}")
            
            with open(filepath, 'r', encoding='utf-8') as csvfile:
                csv_reader = csv.reader(csvfile)
                
                with conn.cursor() as cur:
                    for row_num, row in enumerate(csv_reader, 1):
                        try:
                            stats['total_rows'] += 1
                            
                            # Vérification du format de la ligne
                            if len(row) < 3:
                                stats['skipped'] += 1
                                logging.warning(f"{filename} ligne {row_num}: Format invalide (attendu: quantite,date,ref_borne)")
                                continue
                            
                            # Extraction des valeurs
                            quantite = row[0].strip()
                            date_str = row[1].strip() if len(row) > 1 else ''
                            ref_borne = row[2].strip() if len(row) > 2 else ''
                            
                            # Validation de la quantité
                            try:
                                quantite = float(quantite) if quantite else None
                            except ValueError:
                                stats['skipped'] += 1
                                logging.warning(f"{filename} ligne {row_num}: Quantité invalide '{quantite}'")
                                continue
                            
                            # Traitement de la date (peut être vide)
                            date = None
                            if date_str:
                                try:
                                    date = datetime.strptime(date_str, '%Y-%m-%d').date()
                                except ValueError:
                                    stats['skipped'] += 1
                                    logging.warning(f"{filename} ligne {row_num}: Date invalide '{date_str}' (format attendu: AAAA-MM-JJ)")
                                    continue
                            else:
                                stats['null_dates'] += 1
                                logging.info(f"{filename} ligne {row_num}: Date vide - sera enregistrée comme NULL")
                            
                            # Validation du Ref borne
                            if not ref_borne:
                                stats['skipped'] += 1
                                logging.warning(f"{filename} ligne {row_num}: Ref borne manquant")
                                continue
                            
                            # Recherche de l'ID du point de distribution
                            id_point_dist = get_point_dist_id(conn, ref_borne)
                            if not id_point_dist:
                                stats['points_not_found'] += 1
                                logging.warning(f"{filename} ligne {row_num}: Point de distribution '{ref_borne}' non trouvé")
                                continue
                            
                            # Insertion dans la base de données
                            cur.execute("""
                                INSERT INTO eau_distribue (
                                    quantite, date, id_point_dist
                                ) VALUES (
                                    %s, %s, %s
                                )
                            """, (quantite, date, id_point_dist))
                            
                            stats['inserted'] += 1
                            
                        except Exception as e:
                            stats['errors'] += 1
                            logging.error(f"{filename} ligne {row_num}: Erreur - {str(e)}")
                            conn.rollback()
                            continue
                    
                    conn.commit()
                    logging.info(f"Fichier {filename} traité - {row_num} lignes analysées")

        logging.info(f"Import terminé. Statistiques: {stats}")

    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"ERREUR GLOBALE: {str(e)}", exc_info=True)
        raise
    finally:
        if conn: conn.close()
        logging.info("Connexion à la base de données fermée")

if __name__ == "__main__":
    logging.info(f"Début de l'import depuis le dossier: {DOSSIER_CSV}")
    
    try:
        import_csv_to_db()
        logging.info("Import terminé avec succès")
    except Exception as e:
        logging.critical(f"Échec de l'import: {str(e)}")