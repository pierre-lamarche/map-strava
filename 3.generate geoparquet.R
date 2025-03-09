library(arrow)
library(tidyverse)
library(sfarrow)
library(sf)
library(glue)

liste_fichiers <- list.files("/home/pierre/Documents/strava/data/parquet",
    full.names = TRUE
)

if (!dir.exists("/home/pierre/Documents/strava/data/geoparquet")) {
    dir.create("/home/pierre/Documents/strava/data/geoparquet")
}

for (fichier in liste_fichiers) {
    nomFichier <- str_extract(fichier, ".*/(\\d+)\\.parquet", group = 1)
    donnees <- read_parquet(fichier)

    if (!any(donnees$fit)) {
        donnees <- donnees %>%
            mutate(
                lat = latitude * pi / 180,
                long = longitude * pi / 180,
                pas = time - lag(time),
                dlat = lat - lag(lat),
                dlon = long - lag(long),
                a = sin(dlat / 2)^2 + cos(lag(lat)) * cos(lat) * sin(dlon / 2)^2,
                c = 2 * atan2(sqrt(a), sqrt(1 - a)),
                distance = 6371.0 * c,
                vitesse = distance * 3600,
                pourcentage = (elevation - lag(elevation)) / (distance * 10)
            ) %>%
            select(id, time, vitesse, pourcentage, longitude, latitude, elevation) %>%
            st_as_sf(coords = c("longitude", "latitude"), crs = 4326)
    } else {
        donnees <- donnees %>%
            mutate(
                lat = latitude * pi / 180,
                long = longitude * pi / 180,
                pas = time - lag(time),
                dlat = lat - lag(lat),
                dlon = long - lag(long),
                a = sin(dlat / 2)^2 + cos(lag(lat)) * cos(lat) * sin(dlon / 2)^2,
                c = 2 * atan2(sqrt(a), sqrt(1 - a)),
                distance = 6371.0 * c,
                vitesse = distance * 3600,
                pourcentage = (altitude - lag(altitude)) / (distance * 10)
            ) %>%
            filter(!is.na(longitude)) %>%
            select(id, time, vitesse, speed, pourcentage, longitude, latitude, altitude, temperature, heart_rate) %>%
            st_as_sf(coords = c("longitude", "latitude"), crs = 4326)
    }
    st_write_parquet(donnees, glue("/home/pierre/Documents/strava/data/geoparquet/{nomFichier}.parquet"))
}


liste_fichiers <- list.files("/home/pierre/Documents/strava/data/geoparquet/", pattern = "\\d+\\.parquet", full.names = TRUE)
metadonnees <- read_parquet("/home/pierre/Documents/strava/data/geoparquet/metadonnees.parquet") %>% 
  select(`ID de l'activité`, `Date de l'activité`, `Nom de l'activité`, `Nom du fichier`) %>% 
  mutate(id = str_extract(`Nom du fichier`, "activities/(\\d+)\\.(gpx|fit\\.gz)$", group = 1))
d <- Reduce(bind_rows, lapply(liste_fichiers, st_read_parquet)) %>% 
  arrange(time) %>% 
  mutate(jour = format(time, "%Y-%m-%d")) %>% 
  mutate(tour = dense_rank(jour)) %>% 
  left_join(metadonnees, by = "id")

attr(d$time, "tzone") <- "Europe/Paris"

plot(d[, "geometry"], type = "l")

library(leaflet)

d_ag <- d |> 
    group_by(id) |> 
    summarise(geometry = st_combine(geometry)) |> 
    st_cast("LINESTRING")

leaflet(d_ag) %>%
    addPolylines(weight = 2, col = "blue") %>%
    #addProviderTiles("OpenTopoMap")
    addProviderTiles("Esri.WorldGrayCanvas")
