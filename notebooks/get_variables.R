library(purrr)
library(dplyr)
library(tidyr)
library(tibble)
library(openxlsx)
library(readxl)

source("./R/load_data.R")


datalist <- list(
  "records_w3/items/songs.snappy.parquet",
  "records_w3/items/song.snappy.parquet",
  "records_w3/radio/radio_plays_with_artist_id.csv",
  "records_w3/artists_genre_weight.csv",
  "records_w3/RECORDS_hashed_user_group.parquet", # !!
  "records_w3/artists_songs_languages.csv",
  "records_w3/survey/pcs_openrefine1.csv",
  "records_w3/items/artists_data.snappy.parquet",
  
  "musicbrainz/mbid_name_alias.csv",
  "musicbrainz/mbid_area.csv",
  "musicbrainz/area_names.csv",
  "musicbrainz/area_types.csv",
  "musicbrainz/mbid_deezerid.csv",
  "musicbrainz/mbid_wikidataid_pair.csv",
  "musicbrainz/mbid_deezerid_pair.csv",
  "musicbrainz/mbid_spotifyid_pair.csv",
  "musicbrainz/mbz_gender.csv",
  
  "senscritique/albums_tags.csv",
  "senscritique/tags_meaning.csv",
  "senscritique/contacts_albums_link.csv",
  "senscritique/contacts.csv",
  "senscritique/contacts_tracks.csv",
  "senscritique/ratings.csv",
  "senscritique/senscritique_id_deezer_id_pairing.csv",
  "senscritique/senscritique_deezer_id_pairing_2.csv",
  "senscritique/senscritique_deezer_id_pairing_3.csv",
  "senscritique/senscritique_deezer_id_pairing_4.csv",
  
  "french_media/telerama_raw.csv",
  "french_media/lemonde/lemonde-2010.csv",

  "PCS2020/isco_isei.csv"
  )


col_list <- map(datalist, load_file_info)
names(col_list) <- datalist

long_df <- map_df(names(col_list), function(file) {
  tibble(
    file = file,
    variable = col_list[[file]]
  )
})

wide_df <- long_df %>%
  mutate(has_var = variable) %>%  
  pivot_wider(
    names_from = file,
    values_from = has_var
  )

write.xlsx(wide_df, "variable_list.xlsx")


################################################
################################################

streams_short <- load_partitioned_data(prefix = "records_w3/streams/streams_short/")
streams_long <- load_partitioned_data(prefix = "records_w3/streams/streams_long/")

names(streams_short)
names(streams_long)

################################################
################################################

datalist2 <- list(
"data/regex_fixes.csv",
"data/artists_to_remove.csv",
"data/genres_from_deezer_albums.csv",
"data/manual_search.csv",
"data/area_country.csv",
"data/country_rank.csv"
)

lapply(datalist2, load_data_file)

################################################
################################################

s3 <- initialize_s3()

s3$download_file(Bucket = "scoavoux", 
                 Key = "PCS2020/L66_Matrice_codification_PCS2020_collecte_2023.xlsx", 
                 Filename = "data/temp/L66_Matrice_codification_PCS2020_collecte_2023.xlsx")

pcs <- read_excel("data/temp/L66_Matrice_codification_PCS2020_collecte_2023.xlsx", 
                  sheet = 2, 
                  skip=8)
names(pcs)

s3$download_file(Bucket = "scoavoux", 
                 Key = "PCS2020/L72_Matrice_codification_ISCO_collecte_2023.xlsx", 
                 Filename = "data/temp/L72_Matrice_codification_ISCO_collecte_2023.xlsx")

pcs2 <- read_excel("data/temp/L72_Matrice_codification_ISCO_collecte_2023.xlsx", 
                   sheet = 2, 
                   skip=8)


################################################
################################################

# did le monde and libération in big list

load_file_info("french_media/lefigaro-complet-v0.csv") # ??
load_file_info("french_media/liberation-complet-v2.csv") # ??






