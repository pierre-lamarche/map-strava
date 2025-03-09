from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from pytz import timezone
import pyarrow as pa
import pyarrow.parquet as pq

with open("/home/pierre/Documents/strava/data/Soleil_et_vent_d_hiver.gpx", "r") as f:
    soup = BeautifulSoup(f.read(), features="xml")

donnees = pd.DataFrame.from_dict(
    [
        {
            "id": "000000001",
            "longitude": float(point["lon"]),
            "latitude": float(point["lat"]),
            "time": datetime.strptime(point.find("time").text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = timezone('Europe/Paris')),
            "heart_rate": int(point.find("hr").text),
            "fit": False
        }
        for point in soup.find_all("trkpt")
    ]
)

pq.write_table(
    pa.Table.from_pandas(donnees),
    "/home/pierre/Documents/strava/data/parquet/000000001.parquet",
)
