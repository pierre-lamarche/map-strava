"""
Transforme chaque parquet d'activité Strava en un parquet GeoArrow où toute
l'activité est une seule ligne:

    id        -> identifiant de l'activité
    time      -> début d'activité (MIN(time))
    geometry  -> LINESTRING des points (longitude, latitude) ordonnés par temps,
                 encodé en WKB (extension geoarrow.wkb, CRS 4326)
"""

import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

CHEMIN_ENTREE = "/home/pierre/Documents/strava/data/parquet"
CHEMIN_SORTIE = "/home/pierre/Documents/strava/data/sspcloud/geoparquet"

os.makedirs(CHEMIN_SORTIE, exist_ok=True)

liste_fichiers = sorted(f for f in os.listdir(CHEMIN_ENTREE) if f.endswith(".parquet"))

for fichier in liste_fichiers:
    nom_vue = fichier.removesuffix(".parquet")
    chemin_sortie = os.path.join(CHEMIN_SORTIE, fichier)

    df = pd.read_parquet(os.path.join(CHEMIN_ENTREE, fichier)).sort_values("time")

    pts = df[["longitude", "latitude"]].dropna()
    if len(pts) < 2:
        print(f"{fichier}: ignoré (< 2 points valides)")
        continue

    geom = LineString(list(zip(pts["longitude"], pts["latitude"])))

    gdf = gpd.GeoDataFrame(
        {"id": [df["id"].iloc[0]], "time": [df["time"].min()]},
        geometry=[geom],
        crs=4326,
    )
    gdf.to_parquet(chemin_sortie)

print(f"Done: {len(liste_fichiers)} fichiers parcourus, écrits dans {CHEMIN_SORTIE}")
