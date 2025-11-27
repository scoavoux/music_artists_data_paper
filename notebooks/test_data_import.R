
source("./R/load_data.R")


## parquet
artists_data <- load_s3("records_w3/items/artists_data.snappy.parquet")
head(artists_data)

## csv
names_aliases <- load_s3("musicbrainz/mbid_name_alias.csv")
head(names_aliases)

## partitioned parquet
streams_short <- load_partitioned_s3(prefix = "records_w3/streams/streams_short",
                                       cols = c("is_listened", "media_id"))
head(streams_short)



## ------------------------------------------------------------

## check mbid_deezerid vs mbid_deezerid_pair

mbid_deezerid <- load_s3("musicbrainz/mbid_deezerid.csv") # in artists_country
mbid_deezerid_pair <- load_s3("musicbrainz/mbid_deezerid_pair.csv") # in senscritique_mb_deezer_id

head(mbid_deezerid) # 248K, cols artist_id and mbid
head(mbid_deezerid_pair) # 94K, cols deezer_id, mbid, mbname


## ------------------------------------------------------------

## check area_country

area_country <- read.csv("data/area_country.csv")
area_country









