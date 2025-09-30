import psycopg2
import psycopg2.extras # Pour DictCursor
import logging
from psycopg2 import sql

#Configuration
# Base de données CIBLE
DB_CONFIG_TARGET = {
    "database": "AEP_HARMONISE",
    "user": "postgres",
    "password": "*******",
    "host": "localhost",
    "port": "5432"
}

# Base de données SOURCE
DB_CONFIG_SOURCE_EAURIZON = {
    "database": "AEP_EAURIZON",
    "user": "postgres",
    "password": "*******",
    "host": "localhost",
    "port": "5432"
}

#Logging
# Configuration du logging pour afficher les informations
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#Fonctions Utilitaires
def connect_db(config, name):
    """Établit une connexion à une base de données."""
    try:
        conn = psycopg2.connect(**config)
        # Autocommit False par défaut, c'est ce qu'on veut pour contrôler la transaction
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

#Fonction de Migration pour Commune

# Dictionnaire pour stocker les mappings (si nécessaire pour les étapes futures)
# Clé: Identifiant unique de la source (gid ici), Valeur: Nouvel id_com dans la cible
id_mapping_commune = {}

def migrate_commune(source_conn, target_conn):
    """Migre les données de AEP_EAURIZON.commune vers AEP_HARMONISE.commune."""
    logging.info("--- Début Migration: commune ---")
    # Utiliser DictCursor pour accéder aux colonnes par leur nom
    source_cursor = source_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    target_cursor = target_conn.cursor()

    processed_count = 0
    inserted_count = 0
    error_count = 0

    try:
        # 1. Sélectionner les données de la table source
        source_cursor.execute("""
            SELECT
                gid,        -- Pour référence/mapping
                cod_dist,
                cod_com,    -- double precision
                lib_com,
                cat_com,
                area_km2,
                nom_maire,
                densite,    -- double precision (pour nb_habitant)
                geom        -- geometry
            FROM commune;
        """)
        rows = source_cursor.fetchall()
        logging.info(f"Trouvé {len(rows)} lignes dans AEP_EAURIZON.commune.")

        # 2. Préparer la requête d'insertion pour la table cible
        # Utilisation de psycopg2.sql pour une construction sûre
        sql_insert = sql.SQL("""
            INSERT INTO commune (
                code_dist, code_com, lib_com, cat_com, area_km2,
                nom_maire, nb_habitant, geom
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING id_com; -- Récupérer le nouvel ID généré
        """) 

        # 3. Itérer sur chaque ligne source, transformer et insérer
        for row in rows:
            processed_count += 1
            try:
                #Transformations et validations
                # code_dist: VARCHAR(20) <- character varying(254)
                code_dist_val = row['cod_dist']
                if code_dist_val and len(code_dist_val) > 20:
                    logging.warning(f"Source gid={row['gid']}: 'cod_dist' ('{code_dist_val}') tronqué à 20 caractères.")
                    code_dist_val = code_dist_val[:20]

                # code_com: VARCHAR(10) NOT NULL <- double precision
                code_com_val = None
                if row['cod_com'] is not None:
                     # Convertit le float en string. Gère les '.0' si présents.
                     code_com_str = str(row['cod_com'])
                     if code_com_str.endswith('.0'):
                         code_com_str = code_com_str[:-2]

                     if len(code_com_str) > 10:
                         logging.warning(f"Source gid={row['gid']}: 'cod_com' ('{code_com_str}') tronqué à 10 caractères.")
                         code_com_val = code_com_str[:10]
                     else:
                         code_com_val = code_com_str
                else:
                    logging.error(f"Source gid={row['gid']}: 'cod_com' est NULL. Ligne ignorée car la cible est NOT NULL.")
                    error_count += 1
                    continue 

                # lib_com: VARCHAR(50) <- character varying(254)
                lib_com_val = row['lib_com']
                if lib_com_val and len(lib_com_val) > 50:
                    logging.warning(f"Source gid={row['gid']}, code_com={code_com_val}: 'lib_com' ('{lib_com_val}') tronqué à 50 caractères.")
                    lib_com_val = lib_com_val[:50]

                # cat_com: VARCHAR(30) <- character varying(20)
                cat_com_val = row['cat_com']
                if cat_com_val and len(cat_com_val) > 30:
                     logging.warning(f"Source gid={row['gid']}: 'cat_com' ('{cat_com_val}') tronqué à 30 caractères.")
                     cat_com_val = cat_com_val[:30]


                # area_km2: NUMERIC <- numeric
                area_km2_val = row['area_km2'] 

                # nom_maire: VARCHAR(50) <- character varying(254)
                nom_maire_val = row['nom_maire']
                if nom_maire_val and len(nom_maire_val) > 50:
                    logging.warning(f"Source gid={row['gid']}: 'nom_maire' ('{nom_maire_val}') tronqué à 50 caractères.")
                    nom_maire_val = nom_maire_val[:50]

                # nb_habitant: INTEGER <- double precision (densite)
                nb_habitant_val = None
                if row['densite'] is not None:
                    try:
                        # Conversion simple en entier (tronque la partie décimale)
                        nb_habitant_val = int(row['densite'])
                    except (ValueError, TypeError):
                        logging.warning(f"Source gid={row['gid']}: Impossible de convertir 'densite' ({row['densite']}) en INTEGER. Mis à NULL.")
                        nb_habitant_val = None
                # Si row['densite'] est None, nb_habitant_val reste None

                # geom: geometry(MultiPolygon, 29702) <- geometry(MultiPolygon, 29702)
                geom_val = row['geom'] 

                #Exécution de l'insertion
                target_cursor.execute(sql_insert, (
                    code_dist_val,
                    code_com_val,
                    lib_com_val,
                    cat_com_val,
                    area_km2_val,
                    nom_maire_val,
                    nb_habitant_val,
                    geom_val
                ))

                # Récupérer le nouvel ID et loguer
                new_id_com = target_cursor.fetchone()[0]
                inserted_count += 1
                logging.info(f"  -> Inséré: Source gid={row['gid']} (code_com='{code_com_val}', lib_com='{lib_com_val}') -> Nouveau id_com={new_id_com}")

                # Stocker le mapping si nécessaire pour les tables dépendantes
                id_mapping_commune[row['gid']] = new_id_com

            except psycopg2.Error as e:
                logging.error(f"Erreur lors de l'insertion de la ligne source gid={row['gid']}: {e}")
                error_count += 1
            except Exception as ex:
                logging.error(f"Erreur Python inattendue lors du traitement de la ligne source gid={row['gid']}: {ex}")
                error_count += 1

        # 4. Valider ou annuler la transaction en fonction des erreurs
        if error_count == 0:
            target_conn.commit()
            logging.info("Transaction validée (commit).")
        else:
            target_conn.rollback()
            logging.warning(f"Transaction annulée (rollback) car {error_count} erreur(s) se sont produites lors du traitement des lignes.")
            # target_conn.commit()
            # logging.warning(f"Transaction validée (commit), mais {error_count} erreur(s) se sont produites et ont été ignorées.")


    except psycopg2.Error as e:
        logging.error(f"Erreur majeure de base de données pendant la migration commune: {e}")
        target_conn.rollback()
    except Exception as e:
        logging.error(f"Erreur Python inattendue pendant la migration commune: {e}")
        target_conn.rollback()
    finally:
        logging.info(f"--- Fin Migration: commune ---")
        logging.info(f"Statistiques: Lignes traitées={processed_count}, Insérées={inserted_count}, Erreurs={error_count}")
        if source_cursor:
            source_cursor.close()
        if target_cursor:
            target_cursor.close()


#Fonction Principale
def main():
    """Orchestre la migration pour la table commune."""
    source_conn = None
    target_conn = None

    try:
        # Connexion aux bases de données
        source_conn = connect_db(DB_CONFIG_SOURCE_EAURIZON, "Source EAURIZON")
        target_conn = connect_db(DB_CONFIG_TARGET, "Cible HARMONISE")

        # Exécuter la migration pour la table commune
        migrate_commune(source_conn, target_conn)

        logging.info("Migration de la table 'commune' terminée.")

    except Exception as e:
        logging.error(f"Une erreur critique est survenue dans le processus principal: {e}")
    finally:
        # Fermer toutes les connexions
        if source_conn:
            close_db(source_conn, "Source EAURIZON")
        if target_conn:
            close_db(target_conn, "Cible HARMONISE")

if __name__ == "__main__":
    main()