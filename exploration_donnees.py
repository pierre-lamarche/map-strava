import os
import duckdb

connexion = duckdb.connect()

chemin_parquet = "/home/pierre/Documents/strava/data/parquet"

liste_fichiers = sorted(f for f in os.listdir(chemin_parquet) if f.endswith(".parquet"))

for fichier in liste_fichiers:
    nom_vue = fichier.removesuffix(".parquet")
    connexion.execute(
        f'CREATE OR REPLACE VIEW "{nom_vue}" AS SELECT * FROM read_parquet(\'{os.path.join(chemin_parquet, fichier)}\')'
    )

connexion.sql(r"SELECT view_name FROM duckdb_views() WHERE schema_name = 'main' AND view_name ~ '^\d+$' ORDER BY view_name")

%connection_show connexion
connexion.close()
