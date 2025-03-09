import geopandas as gpd 
import folium
import pandas as pd
from shapely import LineString
import os
import time

chemin = "/home/pierre/Documents/strava/data/parquet/"

liste_fichiers = os.listdir(chemin)
df = pd.concat([pd.read_parquet(f"{chemin}/{fichier}")[['id', 'longitude', 'latitude', 'time', 'fit']] for fichier in liste_fichiers])
df = df[df[['longitude', 'latitude']].notnull().all(1)]

df['date'] = pd.to_datetime(df['time'], unit='s').dt.date.apply(lambda x: int(time.mktime(x.timetuple())))

gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), 
                       crs = "EPSG:4326")
gdf_ag = gdf.groupby('id')['geometry'].apply(lambda x: LineString(x))
gdf_ag = gpd.GeoDataFrame(gdf_ag, geometry='geometry', crs="EPSG:4326")
gdf_ag['id'] = gdf_ag.index
gdf_ag =gdf_ag.reset_index(drop=True)

gdf_ag = pd.merge(gdf_ag, df[['id', 'date']].drop_duplicates(), on='id', how='left').sort_values(by=['date'])

tiles = 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}'
attr = 'Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ'
location = gdf_ag['geometry'].centroid[0]
ma_carte = folium.Map(tiles=tiles, attr=attr, location=[location.y, location.x], zoom_start=12)

for line in gdf_ag['geometry']:
    folium.PolyLine(locations=[[coord[1], coord[0]] for coord in line.coords], weight=2, color='blue', smoothing=1, opacity=0.5).add_to(ma_carte)

ma_carte.save("macarte.html")
