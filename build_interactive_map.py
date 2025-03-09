import os
import folium
import geopandas as gpd
import json
import pandas as pd
from shapely.geometry import mapping
from folium import FeatureGroup, LayerControl
from folium.plugins import FeatureGroupSubGroup
from shapely import LineString

chemin = "/home/pierre/Documents/strava/data/parquet/"

liste_fichiers = os.listdir(chemin)
df = pd.concat([pd.read_parquet(f"{chemin}/{fichier}")[['id', 'longitude', 'latitude', 'time', 'fit']] for fichier in liste_fichiers])
df = df[df[['longitude', 'latitude']].notnull().all(1)]

df['date'] = pd.to_datetime(df['time'], unit='s').dt.date.apply(lambda x: x.strftime("%Y-%m-%d"))

gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), 
                       crs = "EPSG:4326")
gdf_ag = gdf.groupby('id')['geometry'].apply(lambda x: LineString(x))
gdf_ag = gpd.GeoDataFrame(gdf_ag, geometry='geometry', crs="EPSG:4326")
gdf_ag['id'] = gdf_ag.index
gdf_ag =gdf_ag.reset_index(drop=True)

gdf_ag = pd.merge(gdf_ag, df[['id', 'date']].drop_duplicates(), on='id', how='left').sort_values(by=['date'])

# 📌 Création de la carte Folium

tiles = 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}'
attr = 'Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ'
location = gdf_ag['geometry'].centroid[0]
ma_carte = folium.Map(tiles=tiles, attr=attr, location=[location.y, location.x], zoom_start=12)

# 📌 Ajout d'un FeatureGroup global (permet d'activer/désactiver les lignes)
main_group = FeatureGroup(name="Lignes Temporelles").add_to(ma_carte)

# 📌 Génération des sous-groupes dynamiques en fonction des dates
date_groups = {}
for _, row in gdf_ag.iterrows():
    date_str = row["date"]
    if date_str not in date_groups:
        date_groups[date_str] = FeatureGroupSubGroup(main_group, name=f"Lignes {date_str}")
        date_groups[date_str].add_to(ma_carte)  # Ajout à la carte

    # Ajout de la ligne au bon groupe temporel
    folium.GeoJson(
        mapping(row["geometry"]),
        style_function=lambda feature: {
            "color": "blue",
            "weight": 2,
            "opacity": 0.5
        }
    ).add_to(date_groups[date_str])

# 📌 Ajout du contrôle des couches
LayerControl().add_to(ma_carte)
date_list = list(date_groups.keys())  # Liste des dates

# 📌 Ajout du slider avec JavaScript pour afficher dynamiquement les bonnes dates
slider_js = f"""
<script>
    let dates = {json.dumps(date_list)};
    let cumulativeMode = false; // 📌 Variable pour le mode cumulatif
    let lastValue = 0;  // 📌 Stocke la dernière position du slider

    function updateLayers(value) {{
        let selectedDate = dates[value];
        document.getElementById("date-display").textContent = selectedDate;

        let layers = document.querySelectorAll(".leaflet-control-layers-selector");
        let parentGroup = null;

        layers.forEach(layer => {{
            let label = layer.nextSibling.textContent.trim();  // Récupère le nom de la couche
            let layerDate = label.match(/\\d{{4}}-\\d{{2}}-\\d{{2}}/);

            if (label.includes("Lignes Temporelles")) {{
                parentGroup = layer;  // 📌 On stocke le FeatureGroup parent
            }}

            if (cumulativeMode) {{
                if (layerDate) {{
                    let layerDateStr = layerDate[0];

                    if (layerDateStr <= selectedDate) {{
                        // 📌 Si la date est inférieure ou égale à la date sélectionnée, afficher la couche
                        if (!layer.checked) layer.click();
                    }} else if (lastValue > value) {{
                        // 📌 Si on revient en arrière dans le temps, masquer les couches trop récentes
                        if (layer.checked) layer.click();
                    }}
                }}
            }} else {{
                // 📌 Mode normal : Afficher uniquement la couche de la date sélectionnée
                if (label.includes(selectedDate)) {{
                    if (!layer.checked) layer.click();
                }} else {{
                    if (layer.checked) layer.click();
                }}
            }}
        }});

        // 📌 Activation automatique du FeatureGroup parent si non sélectionné
        if (parentGroup && !parentGroup.checked) {{
            parentGroup.click();
        }}

        // 📌 Mise à jour de la dernière valeur du slider
        lastValue = value;
    }}

    // 📌 Activation / Désactivation du mode cumulatif
    function toggleCumulativeMode() {{
        cumulativeMode = document.getElementById("cumulative-checkbox").checked;
        lastValue = document.getElementById("date-slider").value;  // 📌 Réinitialise la dernière valeur du slider
        updateLayers(lastValue);
    }}

    document.addEventListener("DOMContentLoaded", function() {{
        document.getElementById("date-slider").addEventListener("input", function() {{
            updateLayers(this.value);
        }});

        document.getElementById("date-display").textContent = dates[0];

        document.getElementById("cumulative-checkbox").addEventListener("change", toggleCumulativeMode);

        // 📌 Sélectionne la première date par défaut
        setTimeout(() => updateLayers(0), 1000);
    }});
</script>

<input id="date-slider" type="range" min="0" max="{len(date_list) - 1}" value="0" step="1" 
    style="width: 100%;" />
<p>Date sélectionnée: <span id="date-display"></span></p>

<!-- 📌 Ajout d'une case à cocher pour activer/désactiver le mode cumulatif -->
<input type="checkbox" id="cumulative-checkbox" /> Mode cumulatif
"""

# 📌 Injection du slider dans la carte avec les dates disponibles
ma_carte.get_root().html.add_child(folium.Element(slider_js))

ma_carte.save("macarte_with_slider.html")
