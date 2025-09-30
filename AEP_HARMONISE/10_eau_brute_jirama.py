# --- MIGRATION EAU_BRUTE DEPUIS FICHIERS CSV (CHEMIN COURANT) ---

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
        logging.FileHandler('migration_eau_brute.log'),
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

def get_captage_id(cur, nom_captage):
    """Récupère l'ID du captage par son nom (insensible à la casse)
       Cas spécial : si nom_captage == 'VATOSOLA', on cherche d'abord 'BARRAGE 1 - VATOSOLA'
    """
    try:
        if nom_captage.strip().upper() == "VATOSOLA":
            # Recherche prioritaire 'BARRAGE 1 - VATOSOLA'
            cur.execute("""
                SELECT id_capt FROM captage 
                WHERE UPPER(TRIM(libelle_capt)) = UPPER(TRIM(%s))
                LIMIT 1;
            """, ("BARRAGE 1 - VATOSOLA",))
            result = cur.fetchone()
            if result:
                return result[0]

            # Sinon recherche simple 'VATOSOLA'
            cur.execute("""
                SELECT id_capt FROM captage 
                WHERE UPPER(TRIM(libelle_capt)) = UPPER(TRIM(%s))
                LIMIT 1;
            """, (nom_captage,))
            result = cur.fetchone()
            return result[0] if result else None

        else:
            # Cas normal 
            cur.execute("""
                SELECT id_capt FROM captage 
                WHERE UPPER(TRIM(libelle_capt)) = UPPER(TRIM(%s))
                LIMIT 1;
            """, (nom_captage,))
            result = cur.fetchone()
            return result[0] if result else None

    except psycopg2.Error as e:
        logging.error(f"Erreur recherche captage {nom_captage}: {e}")
        return None


def check_duplicate_data(cur, date, captage_id):
    """Vérifie si une entrée existe déjà pour cette date et ce captage"""
    try:
        cur.execute("""
            SELECT 1 FROM eau_brute 
            WHERE date = %s AND id_capt = %s
            LIMIT 1;
        """, (date, captage_id))
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
        'no_captage': 0,
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
                    
                    quantite_str, date_str, nom_captage = row
                    
                    # Nettoyage des valeurs
                    quantite_str = quantite_str.strip()
                    date_str = date_str.strip()
                    nom_captage = nom_captage.strip()
                    
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
                    
                    # Recherche du captage (obligatoire)
                    if not nom_captage:
                        stats['errors'] += 1
                        continue
                        
                    captage_id = get_captage_id(cursor, nom_captage)
                    if not captage_id:
                        stats['no_captage'] += 1
                        continue
                    
                    # Vérification des doublons avant insertion
                    if check_duplicate_data(cursor, date, captage_id):
                        stats['duplicates'] += 1
                        logging.debug(f"Doublon ignoré: captage {captage_id}, date {date}")
                        continue
                    
                    # Insertion dans la base
                    cursor.execute("""
                        INSERT INTO eau_brute (quantite, date, id_capt)
                        VALUES (%s, %s, %s)
                    """, (quantite, date, captage_id))
                    
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
            f"Captages non trouvés: {stats['no_captage']}, "
            f"Doublons ignorés: {stats['duplicates']}"  # Ajout du compteur de doublons
        )
        return stats
        
    except Exception as e:
        conn.rollback()
        logging.error(f"ERREUR fichier {file_path}: {e}")
        raise
        
# --- Migration principale ---
def migrate_eau_brute():
    conn = None
    global_stats = {
        'total': 0, 
        'success': 0, 
        'errors': 0, 
        'no_captage': 0,
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
    logging.info("Début migration des données eau_brute")
    try:
        migrate_eau_brute()
        logging.info("Migration réussie")
    except Exception as e:
        logging.critical(f"Échec migration: {str(e)}")