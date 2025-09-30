# --- MIGRATION EAU_BRUTE DEPUIS FICHIERS CSV (CHEMIN COURANT) ---
# Mettre dans le dossier qui a le fichier .csv et executé le
import psycopg2
import csv
import os
import logging
from psycopg2 import sql

# --- Configuration ---
DB_CONFIG = {
    "database": "AEP_HARMONISE",
    "user": "postgres",
    "password": "*******",
    "host": "localhost",
    "port": "5432"
}

# Utiliser le répertoire du script comme dossier CSV
DOSSIER_CSV = os.path.dirname(os.path.abspath(__file__))

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_eau_traite.log'),
        logging.StreamHandler()
    ]
)

# --- Fonctions Utilitaires ---
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

def get_station_traitement_id(cur, nom_station_traitement):
    """Récupère l'ID du station_traitement par son nom (insensible à la casse)"""
    try:
        cur.execute("""
            SELECT id_station FROM station_traitement 
            WHERE UPPER(TRIM(libelle)) = UPPER(TRIM(%s))
            LIMIT 1;
        """, (nom_station_traitement,))
        result = cur.fetchone()
        return result[0] if result else None
    except psycopg2.Error as e:
        logging.error(f"Erreur recherche station_traitement {nom_station_traitement}: {e}")
        return None

def check_duplicate_data(cur, date, station_traitement_id):
    """Vérifie si une entrée existe déjà pour cette date et ce station_traitement"""
    try:
        cur.execute("""
            SELECT 1 FROM eau_traite 
            WHERE date = %s AND id_station = %s
            LIMIT 1;
        """, (date, station_traitement_id))
        return cur.fetchone() is not None
    except psycopg2.Error as e:
        logging.error(f"Erreur vérification doublon: {e}")
        return False

def process_csv_file(conn, file_path):
    """Traite un fichier CSV et insère les données dans la base"""
    cursor = conn.cursor()
    stats = {
        'total': 0, 
        'success': 0, 
        'errors': 0, 
        'no_station_traitement': 0,
        'skipped_empty': 0,
        'null_quantite': 0,
        'null_date': 0,
        'duplicates': 0  # Nouveau compteur pour les doublons
    }
    
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            csvreader = csv.reader(csvfile)
            
            for row in csvreader:
                try:
                    # Vérifier si la ligne a au moins 3 colonnes
                    if len(row) < 3:
                        stats['errors'] += 1
                        continue
                    
                    quantite_str, date_str, nom_station_traitement = row
                    
                    # Nettoyage des valeurs
                    quantite_str = quantite_str.strip()
                    date_str = date_str.strip()
                    nom_station_traitement = nom_station_traitement.strip()
                    
                    # Cas 1: Les deux champs sont vides → ignorer la ligne
                    if not quantite_str and not date_str:
                        stats['skipped_empty'] += 1
                        continue
                    
                    stats['total'] += 1
                    
                    # Conversion des valeurs
                    quantite = None
                    if quantite_str:
                        try:
                            quantite = float(quantite_str.replace(',', '.'))
                        except ValueError:
                            stats['errors'] += 1
                            continue
                    else:
                        stats['null_quantite'] += 1
                    
                    # Validation de la date
                    date = None
                    if date_str:
                        if len(date_str) == 10 and date_str.count('-') == 2:
                            date = date_str
                        else:
                            stats['errors'] += 1
                            continue
                    else:
                        stats['null_date'] += 1
                    
                    # Recherche du station_traitement (obligatoire)
                    if not nom_station_traitement:
                        stats['errors'] += 1
                        continue
                        
                    station_traitement_id = get_station_traitement_id(cursor, nom_station_traitement)
                    if not station_traitement_id:
                        stats['no_station_traitement'] += 1
                        continue
                    
                    # Vérification des doublons avant insertion
                    if check_duplicate_data(cursor, date, station_traitement_id):
                        stats['duplicates'] += 1
                        logging.debug(f"Doublon ignoré: station_traitement {station_traitement_id}, date {date}")
                        continue
                    
                    # Insertion dans la base
                    cursor.execute("""
                        INSERT INTO eau_traite (quantite, date, id_station)
                        VALUES (%s, %s, %s)
                    """, (quantite, date, station_traitement_id))
                    
                    stats['success'] += 1
                    
                except Exception as e:
                    stats['errors'] += 1
                    logging.error(f"Erreur traitement ligne: {row} - {str(e)}")
                    continue
        
        conn.commit()
        logging.info(
            f"Fichier {os.path.basename(file_path)} traité. "
            f"Total: {stats['total']}, Succès: {stats['success']}\n"
            f"Détail: "
            f"Lignes ignorées (vides): {stats['skipped_empty']}, "
            f"Quantités NULL: {stats['null_quantite']}, "
            f"Dates NULL: {stats['null_date']}, "
            f"Erreurs: {stats['errors']}, "
            f"Captages non trouvés: {stats['no_station_traitement']}, "
            f"Doublons ignorés: {stats['duplicates']}"  # Ajout du compteur de doublons
        )
        return stats
        
    except Exception as e:
        conn.rollback()
        logging.error(f"ERREUR fichier {file_path}: {e}")
        raise
        
# --- Migration principale ---
def migrate_eau_traite():
    conn = None
    global_stats = {
        'total': 0, 
        'success': 0, 
        'errors': 0, 
        'no_station_traitement': 0,
        'duplicates': 0  # Ajout du compteur global de doublons
    }
    
    try:
        conn = connect_db(DB_CONFIG)
        
        # Parcourir tous les fichiers CSV du dossier
        for filename in os.listdir(DOSSIER_CSV):
            if filename.endswith('.csv'):
                file_path = os.path.join(DOSSIER_CSV, filename)
                logging.info(f"Traitement du fichier {filename}...")
                
                try:
                    file_stats = process_csv_file(conn, file_path)
                    # Mise à jour des statistiques globales
                    for key in file_stats:
                        if key in global_stats:
                            global_stats[key] += file_stats[key]
                
                except Exception as e:
                    logging.error(f"Échec traitement fichier {filename}: {e}")
                    global_stats['errors'] += 1
                    continue
        
        logging.info(f"Migration terminée. Statistiques globales: {global_stats}")
        
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"ERREUR GLOBALE: {str(e)}", exc_info=True)
        raise
    finally:
        if conn: close_db(conn)

if __name__ == "__main__":
    logging.info("Début migration des données eau_traite")
    try:
        migrate_eau_traite()
        logging.info("Migration réussie")
    except Exception as e:
        logging.critical(f"Échec migration: {str(e)}")