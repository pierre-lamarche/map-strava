library(XML)
library(tidyverse)
library(pracma)
library(sf)
library(sfarrow)

gpx_parsed <- htmlTreeParse(file = "~/Documents/strava/data/Le_Croisic_Pornichet_entre_deux_temp_tes_et_Halloween.gpx", useInternalNodes = TRUE)

coords <- xpathSApply(doc = gpx_parsed, path = "//trkpt", fun = xmlAttrs)
elevation <- xpathSApply(doc = gpx_parsed, path = "//trkpt/ele", fun = xmlValue)
temps <- xpathSApply(doc = gpx_parsed, path = "//trkpt/time", fun = xmlValue)

donnees <- tibble(temps = as.POSIXct(strptime(temps, format = "%Y-%m-%dT%H:%M:%SZ")) + 7200,
                  lon = as.numeric(coords["lon", ])*pi/180,
                  lat = as.numeric(coords["lat", ])*pi/180,
                  elevation = as.numeric(elevation))

donnees <- donnees |>
  mutate(pas = temps - lag(temps),
         dlat = lat - lag(lat),
         dlon = lon - lag(lon),
         a = sin(dlat/2)^2 + cos(lag(lat)) * cos(lat) * sin(dlon/2)^2,
         c = 2 * atan2(sqrt(a), sqrt(1-a)),
         distance = 6371.0 * c,
         vitesse = distance * 3600,
         pourcentage = (elevation - lag(elevation))/(distance*10)) |>
  filter(row_number() > 5)

mean(kernapply(donnees$vitesse, kernel("daniell", 5)))

donnees_spatiales <- st_as_sf(donnees,
                              coords = c("lon", "lat"),
                              crs = 4326)

plot(donnees_spatiales[, c("vitesse", "geometry")])

plot(kernapply(donnees$vitesse, kernel("daniell", 5)), type = "l")

st_write_parquet(donnees_spatiales, "~/Documents/strava/data/Le_Croisic_Pornichet_entre_deux_temp_tes_et_Halloween.parquet")

liste_fichiers <- list.files("/home/pierre/Documents/strava/data", pattern = ".parquet", full.names = TRUE)
d <- Reduce(bind_rows, lapply(liste_fichiers, st_read_parquet))
  
plot(d[, c("vitesse", "geometry")], type = "l")
