import os
import pandas as pd
import psycopg2
import logging
from datetime import datetime
from typing import Dict, Optional

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
        logging.FileHandler('import_excel_points_distribution.log'),
        logging.StreamHandler()
    ]
)

# Chemin du dossier contenant les fichiers Excel (répertoire du script)
DOSSIER_EXCEL = os.path.dirname(os.path.abspath(__file__))

def connect_db(config):
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        logging.info("Connexion à la base de données réussie")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Échec de la connexion à la base de données: {e}")
        raise

def load_excel_mapping(xlsx_path: str) -> Dict[str, str]:
    """
    Charge le fichier Excel et retourne un mapping ref_borne, troncon
    """
    logging.info(f"Lecture du fichier de mapping: {xlsx_path}")
    try:
        df = pd.read_excel(xlsx_path, dtype=str)
        df.columns = [c.strip().lower() for c in df.columns]
        
        col_borne = next((c for c in df.columns if "ref_borne" in c), None)
        col_troncon = next((c for c in df.columns if "tronçon" in c or "troncon" in c), None)
        
        if not col_borne or not col_troncon:
            logging.warning("Colonnes non détectées automatiquement, utilisation des deux premières colonnes")
            col_borne = df.columns[0]
            col_troncon = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        
        mapping = {}
        for _, row in df.iterrows():
            num = row.get(col_borne)
            att = row.get(col_troncon)
            if pd.isna(num) or pd.isna(att):
                continue
            mapping[str(num).strip()] = str(att).strip()
        
        logging.info(f"{len(mapping)} mappings chargés")
        return mapping
    
    except Exception as e:
        logging.error(f"Erreur lors de la lecture du fichier de mapping: {e}")
        return {}

def find_noeud_cons_id(conn, troncon: str) -> Optional[int]:
    """
    Trouve l'ID du noeud_consommation correspondant au tronçon
    """
    if not troncon or pd.isna(troncon):
        return None
    
    try:
        with conn.cursor() as cur:
            normalized = troncon.replace(" - ", "->")
            cur.execute("""
                SELECT id_noeud_cons FROM noeud_consommation 
                WHERE troncon = %s LIMIT 1
            """, (normalized,))
            result = cur.fetchone()
            if result:
                return result[0]
            
            if " - " in troncon:
                parts = [p.strip() for p in troncon.split(" - ")]
                if len(parts) == 2:
                    for variant in [f"{parts[0]}->{parts[1]}", f"{parts[1]}->{parts[0]}"]:
                        cur.execute("""
                            SELECT id_noeud_cons FROM noeud_consommation 
                            WHERE troncon = %s LIMIT 1
                        """, (variant,))
                        result = cur.fetchone()
                        if result:
                            return result[0]
            return None
            
    except Exception as e:
        logging.error(f"Erreur lors de la recherche du noeud_consommation: {e}")
        return None

def get_quartier_id(conn, quartier_name):
    """Trouve l'ID du quartier en ajoutant 'FKT ' devant le nom"""
    if not quartier_name or pd.isna(quartier_name):
        return None
        
    try:
        quartier_name = str(quartier_name).strip()
        search_name = f"FKT {quartier_name}"
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id_quartier 
                FROM quartier 
                WHERE lib_quartier ILIKE %s
                LIMIT 1;
            """, (search_name,))
            result = cur.fetchone()
            return result[0] if result else None
    except psycopg2.Error as e:
        logging.error(f"Erreur lors de la recherche du quartier '{quartier_name}': {e}")
        return None

def process_excel_file(excel_file, conn, mapping_data):
    """Traite un fichier Excel et importe les données"""
    stats = {
        'total': 0,
        'inserted': 0,
        'skipped': 0,
        'errors': 0,
        'quartier_not_found': 0,
        'noeud_cons_found': 0,
        'noeud_cons_not_found': 0
    }
    
    try:
        df = pd.read_excel(excel_file)
        stats['total'] = len(df)
        logging.info(f"Fichier {os.path.basename(excel_file)} chargé: {stats['total']} enregistrements trouvés")

        with conn.cursor() as cur:
            for index, row in df.iterrows():
                try:
                    ref_borne = str(row['Ref_borne']).strip() if not pd.isna(row['Ref_borne']) else None
                    if not ref_borne:
                        stats['skipped'] += 1
                        logging.warning(f"Ligne {index+2}: Ref borne - ignorée")
                        continue

                    quartier_name = str(row['Cartier']).strip() if not pd.isna(row['Cartier']) else None
                    id_quartier = get_quartier_id(conn, quartier_name)
                    
                    if quartier_name and not id_quartier:
                        stats['quartier_not_found'] += 1
                        logging.warning(f"Ligne {index+2}: Quartier '{quartier_name}' non trouvé dans la base")

                    id_noeud_cons = None
                    if ref_borne in mapping_data:
                        troncon = mapping_data[ref_borne]
                        id_noeud_cons = find_noeud_cons_id(conn, troncon)
                        if id_noeud_cons:
                            stats['noeud_cons_found'] += 1
                        else:
                            stats['noeud_cons_not_found'] += 1

                    # 🔹 Déterminer le type à partir du fichier Excel
                    type_borne = str(row['Type']).strip().upper() if not pd.isna(row.get('Type')) else "BORNE PARTICULIER"
                    if type_borne not in ["BORNE FONTAINE", "BORNE PARTICULIER"]:
                        logging.warning(f"Ligne {index+2}: Type inconnu '{type_borne}', remplacé par 'BORNE PARTICULIER'")
                        type_borne = "BORNE PARTICULIER"

                    # 🔹 Insertion dans la base avec le bon type
                    cur.execute("""
                        INSERT INTO point_de_distribution (
                            type, geom, ref_borne, population,
                            id_quartier, id_noeud_cons
                        ) VALUES (
                            %s, NULL, %s, NULL, %s, %s
                        )
                    """, (
                        type_borne,
                        ref_borne, 
                        id_quartier,
                        id_noeud_cons
                    ))

                    stats['inserted'] += 1
                    if (index + 1) % 100 == 0:
                        conn.commit()
                        logging.info(f"{index+1} lignes traitées...")

                except Exception as e:
                    stats['errors'] += 1
                    logging.error(f"Erreur ligne {index+2}: {str(e)}")
                    conn.rollback()
                    continue

            conn.commit()
            logging.info(f"Fichier {os.path.basename(excel_file)} traité. Stats: {stats}")
            return stats

    except Exception as e:
        conn.rollback()
        logging.error(f"ERREUR lors du traitement du fichier {excel_file}: {str(e)}", exc_info=True)
        raise

def find_mapping_file(directory: str) -> Optional[str]:
    """Trouve le fichier Excel de mapping dans le répertoire"""
    for filename in os.listdir(directory):
        if filename.lower().endswith(('.xlsx', '.xls')):
            try:
                df = pd.read_excel(os.path.join(directory, filename), nrows=1)
                cols = [c.strip().lower() for c in df.columns]
                if any("ref_borne" in c or "borne" in c for c in cols) and \
                   any("tronçon" in c or "troncon" in c for c in cols):
                    return os.path.join(directory, filename)
            except:
                continue
    return None

def import_excel_files():
    """Importe tous les fichiers Excel du dossier"""
    global_stats = {
        'total_files': 0,
        'total_rows': 0,
        'total_inserted': 0,
        'total_skipped': 0,
        'total_errors': 0,
        'total_quartier_not_found': 0,
        'total_noeud_cons_found': 0,
        'total_noeud_cons_not_found': 0
    }
    
    conn = None
    try:
        conn = connect_db(DB_CONFIG)
        
        mapping_file = find_mapping_file(DOSSIER_EXCEL)
        if not mapping_file:
            logging.warning("Aucun fichier de mapping trouvé dans le répertoire")
            mapping_data = {}
        else:
            mapping_data = load_excel_mapping(mapping_file)
        
        for filename in os.listdir(DOSSIER_EXCEL):
            if not filename.lower().endswith(('.xlsx', '.xls')) or filename == os.path.basename(mapping_file):
                continue
                
            filepath = os.path.join(DOSSIER_EXCEL, filename)
            global_stats['total_files'] += 1
            logging.info(f"\nDébut du traitement du fichier: {filename}")
            
            try:
                stats = process_excel_file(filepath, conn, mapping_data)
                global_stats['total_rows'] += stats['total']
                global_stats['total_inserted'] += stats['inserted']
                global_stats['total_skipped'] += stats['skipped']
                global_stats['total_errors'] += stats['errors']
                global_stats['total_quartier_not_found'] += stats['quartier_not_found']
                global_stats['total_noeud_cons_found'] += stats['noeud_cons_found']
                global_stats['total_noeud_cons_not_found'] += stats['noeud_cons_not_found']
                
            except Exception as e:
                global_stats['total_errors'] += 1
                logging.error(f"Échec du traitement du fichier {filename}: {str(e)}")
                continue

        logging.info(f"\nImport global terminé. Statistiques globales: {global_stats}")

    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"ERREUR GLOBALE: {str(e)}", exc_info=True)
        raise
    finally:
        if conn: conn.close()
        logging.info("Connexion à la base de données fermée")

if __name__ == "__main__":
    logging.info(f"Début de l'import depuis le dossier: {DOSSIER_EXCEL}")
    
    try:
        import_excel_files()
        logging.info("Import terminé avec succès")
    except Exception as e:
        logging.critical(f"Échec de l'import: {str(e)}")
