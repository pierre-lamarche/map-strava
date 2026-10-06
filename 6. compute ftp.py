"""Lister les activités utilisables pour le calcul du FTP.

Parcourt le dossier de données parquet et retient les fichiers dont les
colonnes `power` et `heart_rate` existent et ne sont pas entièrement
manquantes (chacune doit contenir au moins une valeur non-nulle).

Les fichiers dérivés de GPX, qui ne contiennent pas ces colonnes, sont
exclus. La liste `noms_activites` est prête à être réutilisée pour la
suite du calcul du FTP.

Écrit ensuite une table agrégée sur fenêtres fixes de 30 s
(`power_hr_window30.parquet`) : distance chemin, moyennes power / FC /
température, coordonnées et altitude de début et de fin, et gradient
d'altitude de la fenêtre (Δaltitude / distance).

Enfin, une table « best efforts » (`best_efforts.parquet`) : pour chaque
activité, la meilleure puissance moyenne sur des durées de 1, 2, 5, 10,
20, 30, 40 min et 1 h, calculée par sommes glissantes (rolling, méthode
des sommes partielles).
"""

import os

import numpy as np
import pyarrow.parquet as pq
from tqdm import tqdm
import pandas as pd

CHEMIN_SOURCE = "/home/pierre/Documents/strava/data/parquet"
COLONNES = ["id", "time", "power", "heart_rate", "cadence", "weight", "temperature", "enhanced_altitude", "longitude", "latitude"]
CHEMIN_SORTIE = "/home/pierre/Documents/strava/data/power_hr.parquet"
CHEMIN_SORTIE_FENETRES = "/home/pierre/Documents/strava/data/power_hr_window30.parquet"
FENETRE_S = 30
CHEMIN_SORTIE_BEST = "/home/pierre/Documents/strava/data/best_efforts.parquet"
DURATIONS_S = [(60, "best_1min"), (120, "best_2min"), (300, "best_5min"),
               (600, "best_10min"), (1200, "best_20min"), (1800, "best_30min"),
               (2400, "best_40min"), (3600, "best_60min")]


def compter_non_manquants(chemin):
    """Retourner (n_non_nulls_power, n_non_nulls_hr) ou None si les
    colonnes `power` et `heart_rate` n'existent pas dans le fichier."""
    pf = pq.ParquetFile(chemin)
    colonnes = pf.schema_arrow.names
    if "power" not in colonnes or "heart_rate" not in colonnes:
        return None
    table = pf.read(columns=["power", "heart_rate"])
    n_power = len(table.column("power").drop_null())
    n_hr = len(table.column("heart_rate").drop_null())
    return n_power, n_hr


def haversine_m(lat1, lon1, lat2, lon2):
    """Distance en mètres entre deux paires de coordonnées (vectorisé)."""
    r_ = 6_371_000.0
    p1 = np.radians(lat1)
    p2 = np.radians(lat2)
    dp = np.radians(lat2 - lat1)
    dl = np.radians(lon2 - lon1)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * r_ * np.arcsin(np.sqrt(a))


def fenetres_30s(df, dist_1s):
    """Agrégats sur fenêtres fixes non chevauchantes de 30 s, par activité.

    `df` doit être trié par (id, time). `dist_1s` est la distance (m) entre
    chaque point et le précédent dans la même activité (NaN pour le 1er).
    Retourne un DataFrame à une ligne par (activité, fenêtre).
    """
    # Fenêtres ancrées sur la grille unix (secondes // 30 * 30), stables
    # d'un run à l'autre et comparables entre activités.
    # Soustraction d'époque (indépendante de la précision stockée en ns/us/ms).
    epoch_s = (
        df["time"] - pd.Timestamp("1970-01-01", tz="UTC")
    ) // pd.Timedelta(seconds=1)
    time_start = pd.to_datetime(
        epoch_s // FENETRE_S * FENETRE_S, unit="s", utc=True
    ).dt.tz_convert(df["time"].dt.tz)

    out = (
        df.assign(dist_1s=dist_1s, time_start=time_start)
          .groupby(["id", "time_start"], sort=False)
          .agg(
              n              = ("time", "size"),
              distance       = ("dist_1s", "sum"),
              power          = ("power", "mean"),
              heart_rate     = ("heart_rate", "mean"),
              temperature    = ("temperature", "mean"),
              lat_first      = ("latitude", "first"),
              lon_first      = ("longitude", "first"),
              altitude_first = ("enhanced_altitude", "first"),
              lat_last       = ("latitude", "last"),
              lon_last       = ("longitude", "last"),
              altitude_last  = ("enhanced_altitude", "last"),
          )
          .reset_index()
    )

    out["delta_altitude"] = out["altitude_last"] - out["altitude_first"]
    out["gradient"] = np.where(
        out["distance"] > 0,
        out["delta_altitude"] / out["distance"],
        np.nan,
    )
    out["pente_pct"] = out["gradient"] * 100
    out["vitesse_m_s"] = np.where(
        out["n"] > 1, out["distance"] / (out["n"] - 1), np.nan
    )
    return out


def best_efforts(df):
    """Meilleure puissance moyenne (W) sur T secondes, par activité.

    `df` doit être trié par (id, time), cadence 1 s (T lignes = T s).
    Une fenêtre compte s'il contient T échantillons valides (rolling avec
    min_periods = T, les NaN n'étant pas comptés comme valides). Retourne
    un DataFrame indexé par id, une colonne par durée ; NaN si l'activité
    est trop courte pour la durée.
    """
    g = df.groupby("id", sort=False)
    out = pd.DataFrame(index=df["id"].drop_duplicates())
    for duree_s, nom in DURATIONS_S:
        moy = g["power"].transform(
            lambda s: s.rolling(duree_s, min_periods=duree_s).mean()
        )
        out[nom] = moy.groupby(df["id"]).max()
    return out


def main():
    fichiers = sorted(
        f for f in os.listdir(CHEMIN_SOURCE) if f.endswith(".parquet")
    )

    noms_activites = []
    tranches = []
    for fichier in tqdm(fichiers, desc="Vérification power/heart_rate"):
        chemin = os.path.join(CHEMIN_SOURCE, fichier)
        resultats = compter_non_manquants(chemin)
        if resultats is None:
            continue
        n_power, n_hr = resultats
        if n_power >= 1 and n_hr >= 1:
            noms_activites.append(fichier)
            print(f"  {fichier}: power={n_power}, heart_rate={n_hr}")
            pf = pq.ParquetFile(chemin)
            colonnes_dispo = [c for c in COLONNES if c in pf.schema_arrow.names]
            table = pq.read_table(chemin, columns=colonnes_dispo)
            df = table.to_pandas()
            tranches.append(df)
            print(f"  {fichier}: {len(df)} lignes")

    print(f"\n{len(noms_activites)} fichier(s) sur {len(fichiers)} "
          f"ont power et heart_rate non entièrement manquants.")
    resultat = pd.concat(tranches, ignore_index=True)

    # ---- Gradient d'altitude (pente locale) par activité ----
    # Ordre chronologique indispensable avant diff()/shift() au sein de chaque id
    resultat = resultat.sort_values(["id", "time"]).reset_index(drop=True)

    g = resultat.groupby("id", sort=False)
    dh = g["enhanced_altitude"].diff()
    lat_prev = g["latitude"].shift(1)
    lon_prev = g["longitude"].shift(1)

    dist = haversine_m(
        resultat["latitude"],
        resultat["longitude"],
        lat_prev,
        lon_prev,
    )

    gradient = dh / dist
    gradient[dist <= 0] = np.nan  # points superposes -> pente non definie
    resultat["gradient"] = gradient

    resultat.to_parquet(CHEMIN_SORTIE, index=False)
    print(f"\nFichier écrit : {CHEMIN_SORTIE}")
    print(f"Total : {len(resultat)} lignes, "
          f"{resultat['id'].nunique()} activités, "
          f"colonnes : {list(resultat.columns)}")

    # ---- Table agrégée sur fenêtres de 30 s ----
    fenetres = fenetres_30s(resultat, dist)
    fenetres.to_parquet(CHEMIN_SORTIE_FENETRES, index=False)
    print(f"\nFichier écrit : {CHEMIN_SORTIE_FENETRES}")
    print(f"Fenêtres : {len(fenetres)} lignes, "
          f"{fenetres['id'].nunique()} activités, "
          f"colonnes : {list(fenetres.columns)}")

    # ---- Best efforts par activité (1/2/5/10/20/30/40/60 min) ----
    best = best_efforts(resultat).reset_index()
    best.to_parquet(CHEMIN_SORTIE_BEST, index=False)
    print(f"\nFichier écrit : {CHEMIN_SORTIE_BEST}")
    print(best.to_string(index=False,
                         formatters={c: "{:.0f}".format
                                      for c in best.columns[1:]}))


if __name__ == "__main__":
    main()
