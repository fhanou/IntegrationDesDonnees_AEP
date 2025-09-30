import psycopg2
from psycopg2 import sql

DB_CONFIG = {
    "database": "AEP_HARMONISE",
    "user": "postgres",
    "password": "*******",
    "host": "localhost",
    "port": "5432"
}

SQL_COMMANDS = [
    """
    CREATE EXTENSION IF NOT EXISTS postgis;
    """,
    """
    DROP TABLE IF EXISTS eau_distribue CASCADE;
    DROP TABLE IF EXISTS eau_traite CASCADE;
    DROP TABLE IF EXISTS eau_brute CASCADE;
    DROP TABLE IF EXISTS conduite_noeud CASCADE;
    DROP TABLE IF EXISTS noeud CASCADE;
    DROP TABLE IF EXISTS noeud_consommation CASCADE;
    DROP TABLE IF EXISTS conduite_de_distribution CASCADE;
    DROP TABLE IF EXISTS conduite_adduction CASCADE;
    DROP TABLE IF EXISTS conduite_amenee CASCADE;
    DROP TABLE IF EXISTS point_de_distribution CASCADE;
    DROP TABLE IF EXISTS reservoir_reservoir CASCADE;
    DROP TABLE IF EXISTS reservoir CASCADE;
    DROP TABLE IF EXISTS station_traitement CASCADE;
    DROP TABLE IF EXISTS captage CASCADE;
    DROP TABLE IF EXISTS quartier CASCADE;
    DROP TABLE IF EXISTS commune CASCADE;
    DROP TYPE IF EXISTS type_point_distr CASCADE;
    """,
    """
    CREATE TYPE type_point_distr AS ENUM (
        'BORNE FONTAINE',
        'BORNE PARTICULIER',
        'BORNE INSTITUT PUBLIQUE'
    );
    """,
    """
    CREATE TABLE commune (
        id_com SERIAL PRIMARY KEY,
        code_dist VARCHAR(20),
        code_com VARCHAR(10) NOT NULL,
        lib_com VARCHAR(50),
        cat_com VARCHAR(30),
        area_km2 NUMERIC,
        nom_maire VARCHAR(50),
        nb_habitant INTEGER,
        geom geometry(MultiPolygon, 29702)
    );
    """,
    """
    CREATE TABLE quartier (
        id_quartier SERIAL PRIMARY KEY,
        id_com INTEGER NOT NULL,
        code_quartier VARCHAR(50) UNIQUE NOT NULL,
        lib_quartier VARCHAR(50),
        area_km2 NUMERIC,
        nb_habitant INTEGER,
        geom geometry(MultiPolygon, 29702)
    );
    """,
    """
    CREATE TABLE captage (
        id_capt SERIAL PRIMARY KEY,
        libelle_capt VARCHAR(50),
        type_capt VARCHAR(60),
        debit_capt NUMERIC,
        date_mes DATE,
        geom geometry(MultiPolygon, 29702),
        id_quartier INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE station_traitement (
        id_station SERIAL PRIMARY KEY,
        libelle VARCHAR(50),
        elevation NUMERIC,
        decanteurs NUMERIC,
        filtres NUMERIC,
        capacite NUMERIC,
        geom geometry(Point, 29702),
        id_quartier INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE reservoir (
        id_reservoir SERIAL PRIMARY KEY,
        libelle VARCHAR(50), 
        materiel VARCHAR(50),
        volume_m3 NUMERIC,
        geom geometry(Point, 29702),
        id_quartier INTEGER NOT NULL
    );
    """,
    """
    -- Table pour relations entre réservoirs
    CREATE TABLE reservoir_reservoir (
        id_reservoir_source INTEGER NOT NULL,
        id_reservoir_destination INTEGER NOT NULL,
        PRIMARY KEY (id_reservoir_source, id_reservoir_destination),
        CHECK (id_reservoir_source <> id_reservoir_destination)
    );
    """,
    """
    CREATE TABLE noeud_consommation (
        id_noeud_cons SERIAL PRIMARY KEY,
        libelle VARCHAR(50), 
        troncon VARCHAR(15), 
        geom geometry(Point, 29702)
    );
    """,
    """
    CREATE TABLE point_de_distribution (
        id_point_dist SERIAL PRIMARY KEY,
        type type_point_distr,
        ref_borne VARCHAR(15),
        population NUMERIC,
        id_quartier INTEGER,
        id_noeud_cons INTEGER,
        geom geometry(Point, 29702)
    );
    """,
    """
    -- Table pour relation ManyToMany entre captage et station_traitement
    CREATE TABLE conduite_amenee (
        id_conduit_ame SERIAL PRIMARY KEY,
        id_capt INTEGER NOT NULL,
        id_station INTEGER NOT NULL,
        type VARCHAR(15),
        materiel VARCHAR(15),
        diametre NUMERIC,
        longueur NUMERIC,
        geom geometry(Point, 29702)
    );
    """,
    """
    -- Table pour relation ManyToMany entre station_traitement, reservoir et captage
    CREATE TABLE conduite_adduction (
        id_conduit_add SERIAL PRIMARY KEY,
        id_reservoir INTEGER NOT NULL,
        id_capt INTEGER,
        id_station INTEGER,
        libelle VARCHAR(50), 
        materiel VARCHAR(15), 
        longueur NUMERIC,
        dn NUMERIC,
        rugosite NUMERIC,
        geom geometry(Point, 29702)
    );
    """,
    """
    -- Table pour relation ManyToMany entre point_de_distribution et (reservoir, station_traitement, captage)
    CREATE TABLE conduite_de_distribution (
        id_conduite_dist SERIAL PRIMARY KEY,
        id_reservoir INTEGER,
        id_station INTEGER,
        id_capt INTEGER,
        id_point_dist INTEGER NOT NULL,
        libelle VARCHAR(50), 
        materiel VARCHAR(15), 
        longueur NUMERIC,
        dn NUMERIC,
        rugosite NUMERIC,
        geom geometry(MultiLineStringZ, 29702)
    );
    """,
    """
    -- Nouvelle table noeud
    CREATE TABLE noeud (
        id_noeud SERIAL PRIMARY KEY,
        libelle VARCHAR(50), 
        elevation NUMERIC,
        demande NUMERIC,
        geom geometry(Point, 29702)
    );
    """,
    """
    -- Table de jonction pour relations ManyToMany entre conduites et noeuds
    CREATE TABLE conduite_noeud (
        id_conduite_dist INTEGER,
        id_conduit_add INTEGER,
        id_noeud INTEGER NOT NULL,
        PRIMARY KEY (id_conduite_dist, id_conduit_add, id_noeud)
    );
    """,
    """
    CREATE TABLE eau_brute (
        id_prod_eb SERIAL PRIMARY KEY,
        quantite NUMERIC,
        date DATE,
        id_capt INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE eau_traite (
        id_prod_et SERIAL PRIMARY KEY,
        quantite NUMERIC,
        date DATE,
        id_station INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE eau_distribue (
        id_distr_ep SERIAL PRIMARY KEY,
        quantite NUMERIC,
        date DATE,
        id_point_dist INTEGER
    );
    """,
    """
    ALTER TABLE quartier
        ADD CONSTRAINT fk_quartier_commune
        FOREIGN KEY (id_com) REFERENCES commune (id_com)
        ON DELETE RESTRICT ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE captage
        ADD CONSTRAINT fk_captage_quartier
        FOREIGN KEY (id_quartier) REFERENCES quartier (id_quartier)
        ON DELETE RESTRICT ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE station_traitement
        ADD CONSTRAINT fk_station_quartier
        FOREIGN KEY (id_quartier) REFERENCES quartier (id_quartier)
        ON DELETE RESTRICT ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE reservoir
        ADD CONSTRAINT fk_reservoir_quartier
        FOREIGN KEY (id_quartier) REFERENCES quartier (id_quartier)
        ON DELETE RESTRICT ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE reservoir_reservoir
        ADD CONSTRAINT fk_reservreserv_source
        FOREIGN KEY (id_reservoir_source) REFERENCES reservoir (id_reservoir)
        ON DELETE CASCADE ON UPDATE CASCADE,
        ADD CONSTRAINT fk_reservreserv_dest
        FOREIGN KEY (id_reservoir_destination) REFERENCES reservoir (id_reservoir)
        ON DELETE CASCADE ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE point_de_distribution
        ADD CONSTRAINT fk_pointdist_quartier
        FOREIGN KEY (id_quartier) REFERENCES quartier (id_quartier)
        ON DELETE RESTRICT ON UPDATE CASCADE,
        ADD CONSTRAINT fk_pointdist_noeudcons
        FOREIGN KEY (id_noeud_cons) REFERENCES noeud_consommation (id_noeud_cons)
        ON DELETE SET NULL ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE conduite_amenee
        ADD CONSTRAINT fk_conduiteame_captage
        FOREIGN KEY (id_capt) REFERENCES captage (id_capt)
        ON DELETE CASCADE ON UPDATE CASCADE,
        ADD CONSTRAINT fk_conduiteame_station
        FOREIGN KEY (id_station) REFERENCES station_traitement (id_station)
        ON DELETE CASCADE ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE conduite_adduction
        ADD CONSTRAINT fk_conduiteadd_reservoir
        FOREIGN KEY (id_reservoir) REFERENCES reservoir (id_reservoir)
        ON DELETE CASCADE ON UPDATE CASCADE,
        ADD CONSTRAINT fk_conduiteadd_captage
        FOREIGN KEY (id_capt) REFERENCES captage (id_capt)
        ON DELETE CASCADE ON UPDATE CASCADE,
        ADD CONSTRAINT fk_conduiteadd_station
        FOREIGN KEY (id_station) REFERENCES station_traitement (id_station)
        ON DELETE CASCADE ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE conduite_de_distribution
        ADD CONSTRAINT fk_conduitedist_reservoir
        FOREIGN KEY (id_reservoir) REFERENCES reservoir (id_reservoir)
        ON DELETE SET NULL ON UPDATE CASCADE,
        ADD CONSTRAINT fk_conduitedist_station
        FOREIGN KEY (id_station) REFERENCES station_traitement (id_station)
        ON DELETE SET NULL ON UPDATE CASCADE,
        ADD CONSTRAINT fk_conduitedist_captage
        FOREIGN KEY (id_capt) REFERENCES captage (id_capt)
        ON DELETE SET NULL ON UPDATE CASCADE,
        ADD CONSTRAINT fk_conduitedist_pointdist
        FOREIGN KEY (id_point_dist) REFERENCES point_de_distribution (id_point_dist)
        ON DELETE CASCADE ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE conduite_noeud
        ADD CONSTRAINT fk_conduitenoeud_dist
        FOREIGN KEY (id_conduite_dist) REFERENCES conduite_de_distribution (id_conduite_dist)
        ON DELETE CASCADE ON UPDATE CASCADE,
        ADD CONSTRAINT fk_conduitenoeud_add
        FOREIGN KEY (id_conduit_add) REFERENCES conduite_adduction (id_conduit_add)
        ON DELETE CASCADE ON UPDATE CASCADE,
        ADD CONSTRAINT fk_conduitenoeud_noeud
        FOREIGN KEY (id_noeud) REFERENCES noeud (id_noeud)
        ON DELETE CASCADE ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE eau_brute
        ADD CONSTRAINT fk_prodeb_captage
        FOREIGN KEY (id_capt) REFERENCES captage (id_capt)
        ON DELETE CASCADE ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE eau_traite
        ADD CONSTRAINT fk_prodet_station
        FOREIGN KEY (id_station) REFERENCES station_traitement (id_station)
        ON DELETE CASCADE ON UPDATE CASCADE;
    """,
    """
    ALTER TABLE eau_distribue
        ADD CONSTRAINT fk_distep_pointdist
        FOREIGN KEY (id_point_dist) REFERENCES point_de_distribution (id_point_dist)
        ON DELETE CASCADE ON UPDATE CASCADE;
    """
]

def create_database_schema():
    """Crée la structure de la base de données AEP_HARMONISE."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        for i, command in enumerate(SQL_COMMANDS):
            print(f"Exécution de la commande SQL #{i+1}...")
            cursor.execute(command)
            
        conn.commit()
        print("Schéma créé avec succès avec les nouvelles relations.")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erreur: {error}")
        if conn: conn.rollback()
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    create_database_schema()