import zipfile
import gzip
import re
import fitdecode
import gpxpy
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
from pytz import timezone
import os
from tqdm import tqdm


with zipfile.ZipFile(
    "/home/pierre/Documents/strava/data/export_121802745.zip", "r"
) as zf:
    list_activities = [
        file for file in zf.namelist() if re.match(r"^activities/.+", file)
    ]

    for activity in tqdm(list_activities):
        nameFile = re.match(r".*./(.*?)\..*$", activity).group(1)
        if re.match(r".*\.gpx$", activity):
            with zf.open(activity, "r") as f:
                parsed_activity = gpxpy.parse(f)
                points = [
                    p
                    for t in parsed_activity.tracks
                    for s in t.segments
                    for p in s.points
                ]
                data = [
                    {
                        "id": nameFile,
                        "longitude": point.longitude,
                        "latitude": point.latitude,
                        "elevation": point.elevation,
                        "time": point.time.replace(tzinfo=timezone("Europe/Paris")),
                        "fit": False,
                    }
                    for point in points
                ]
        if re.match(r".*\.fit.gz$", activity):
            with zf.open(activity, "r") as f:
                with gzip.open(f, "rb") as gf:
                    with fitdecode.FitReader(gf) as fit_file:
                        data = [
                            {
                                "id": nameFile,
                                "longitude": (
                                    frame.get_value("position_long") / ((2**32) / 360)
                                    if frame.has_field("position_long")
                                    else None
                                ),
                                "latitude": (
                                    frame.get_value("position_lat") / ((2**32) / 360)
                                    if frame.has_field("position_lat")
                                    else None
                                ),
                                "time": (
                                    frame.get_value("timestamp")
                                    .astimezone(timezone("Europe/Paris"))
                                    if frame.has_field("timestamp")
                                    else None
                                ),
                                "temperature": (
                                    frame.get_value("temperature")
                                    if frame.has_field("temperature")
                                    else None
                                ),
                                "speed": (
                                    frame.get_value("speed")*3.6
                                    if frame.has_field("speed")
                                    else None
                                ),
                                "altitude": (
                                    frame.get_value("altitude")
                                    if frame.has_field("altitude")
                                    else None
                                ),
                                "enhanced_speed": (
                                    frame.get_value("enhanced_speed")*3.6
                                    if frame.has_field("enhanced_speed")
                                    else None
                                ),
                                "enhanced_altitude": (
                                    frame.get_value("enhanced_altitude")
                                    if frame.has_field("enhanced_altitude")
                                    else None
                                ),
                                "heart_rate": (
                                    frame.get_value("heart_rate")
                                    if frame.has_field("heart_rate")
                                    else None
                                ),
                                "fit": True,
                            }
                            for frame in fit_file
                            if isinstance(frame, fitdecode.records.FitDataMessage)
                            and frame.name == "record"
                        ]
        df = pd.DataFrame(data)
        # df_g = gpd.GeoDataFrame(
        #    df, geometry=gpd.points_from_xy(df.longitude, df.latitude)
        # ).set_crs(4326)

        # df_g.to_parquet(f"/home/pierre/Documents/strava/data/test/{nameFile}.parquet")
        if not os.path.isdir("/home/pierre/Documents/strava/data/parquet"):
            os.makedirs("/home/pierre/Documents/strava/data/parquet")
        pq.write_table(
            pa.Table.from_pandas(df),
            f"/home/pierre/Documents/strava/data/parquet/{nameFile}.parquet",
        )

    with zf.open("activities.csv", "r") as f:
        df = pd.read_csv(f)

    if not os.path.isdir("/home/pierre/Documents/strava/data/geoparquet"):
        os.makedirs("/home/pierre/Documents/strava/data/geoparquet")

    pq.write_table(
        pa.Table.from_pandas(df),
        "/home/pierre/Documents/strava/data/geoparquet/metadonnees.parquet",
    )
