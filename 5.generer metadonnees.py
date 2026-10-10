"""
Génère un parquet de métadonnées à la racine de data/sspcloud/
contenant, pour chaque activité :

    id         -> identifiant de l'activité
    nom        -> nom de l'activité (activities.csv du zip d'export)
    type       -> type d'activité (Vélo, Course à pied, etc.)
    fichier    -> nom du parquet de géométrie (data/sspcloud/geoparquet)
    distance_m -> distance totale (mètres, haversine)
    duree      -> durée de l'activité (temps)
    date       -> début d'activité
"""

import os
import zipfile
import numpy as np
import pandas as pd
import geopandas as gpd

CHEMIN_GEO = "/home/pierre/Documents/strava/data/sspcloud/geoparquet"
CHEMIN_SOURCE = "/home/pierre/Documents/strava/data/parquet"
CHEMIN_ZIP = "/home/pierre/Documents/strava/data/export_121802745.zip"
CHEMIN_RACINE = "/home/pierre/Documents/strava/data/sspcloud"
CHEMETD = os.path.join(CHEMIN_RACINE, "metadonnees.parquet")

os.makedirs(CHEMIN_RACINE, exist_ok=True)


def noms_activites(chemin_zip: str) -> pd.DataFrame:
    """Récupère id et nom des activités depuis activities.csv dans le zip.

    L'id utile est celui contenu dans la colonne "Nom du fichier"
    (ex. activities/21289896024.fit.gz -> 21289896024),
    qui correspond aux noms de fichiers parquet.
    """
    with zipfile.ZipFile(chemin_zip) as z:
        with z.open("activities.csv") as f:
            df = pd.read_csv(f, usecols=["Activity Name", "Activity Type", "Filename"])
    df.columns = ["nom", "type", "fichier_zip"]
    df["id"] = df["fichier_zip"].str.extract(r"(\d+)")
    return df.set_index("id")[["nom", "type"]]


noms = noms_activites(CHEMIN_ZIP)

liste_fichiers = sorted(f for f in os.listdir(CHEMIN_GEO) if f.endswith(".parquet"))


def distance_haversine(coords: np.ndarray) -> float:
    """Distance totale d'une trace, coords = (n, 2) en degrés (lon, lat)."""
    lon_rad = np.radians(coords[:, 0])
    lat_rad = np.radians(coords[:, 1])
    dlon = np.diff(lon_rad)
    dlat = np.diff(lat_rad)
    a = np.sin(dlat / 2) ** 2 + np.cos(lat_rad[:-1]) * np.cos(lat_rad[1:]) * np.sin(dlon / 2) ** 2
    return float(6371_000 * (2 * np.arcsin(np.sqrt(a))).sum())


lignes = []
for fichier in liste_fichiers:
    gdf = gpd.read_parquet(os.path.join(CHEMIN_GEO, fichier))
    coords = np.array(gdf.geometry.iloc[0].coords)

    source = os.path.join(CHEMIN_SOURCE, fichier)
    if os.path.exists(source):
        time = pd.read_parquet(source, columns=["time"])["time"]
        duree = time.max() - time.min()
        date = time.min()
    else:
        duree = pd.NaT
        date = gdf["time"].iloc[0]

    act_id = str(gdf["id"].iloc[0])
    row = noms.loc[act_id] if act_id in noms.index else None
    lignes.append(
        {
            "id": act_id,
            "nom": row["nom"] if row is not None else None,
            "type": row["type"] if row is not None else None,
            "fichier": fichier,
            "distance_m": distance_haversine(coords),
            "duree": duree,
            "date": date,
        }
    )

metadonnees = pd.DataFrame(lignes).sort_values("date").reset_index(drop=True)
metadonnees.to_parquet(CHEMETD, index=False)

print(f"Métadonnées écrites : {CHEMETD}")
print(f"  {len(metadonnees)} activités")
print(metadonnees.head(3).to_string(index=False))
