source("./R/load_data.R")

### records
load_data_file(path = "data/",
          file = "regex_fixes.csv")


library(purrr)
library(dplyr)
library(tidyr)
library(tibble)

build_variable_matrix <- function(dataset_table) {
  
  # dataset_table must contain: path, file
  
  results <- pmap(dataset_table, function(file) {
    vars <- get_variables(file)
    tibble(dataset = file, variable = vars)
  }) %>% bind_rows()
  
  # Wide matrix: each dataset becomes a column
  wide <- results %>%
    mutate(row = row_number()) %>%  # temporary to help spreading uneven sizes
    select(dataset, variable) %>%
    group_by(dataset) %>%
    mutate(var_index = row_number()) %>%
    ungroup() %>%
    pivot_wider(
      names_from = dataset,
      values_from = variable
    )
  
  return(wide)
}


var_matrix <- build_variable_matrix(dataset_list)

write.csv(var_matrix, "all_dataset_variables.csv", row.names = FALSE)







"records_w3/items/songs.snappy.parquet"
"records_w3/items/song.snappy.parquet"


"records_w3/streams/streams_short"
"records_w3/radio/radio_plays_with_artist_id.csv"
"records_w3/artists_genre_weight.csv"
"data/temp/RECORDS_hashed_user_group.parquet"
"records_w3/artists_songs_languages.csv"
"records_w3/survey/pcs_openrefine1.csv"

"records_w3/items/artists_data.snappy.parquet"


"musicbrainz/mbid_name_alias.csv"
"musicbrainz/mbid_area.csv"
"musicbrainz/area_names.csv"
"musicbrainz/area_types.csv"
"musicbrainz/mbid_deezerid.csv"
"musicbrainz/mbid_wikidataid_pair.csv"
"musicbrainz/mbid_deezerid_pair.csv"
"musicbrainz/mbid_spotifyid_pair.csv"
"musicbrainz/mbz_gender.csv"
"musicbrainz/mbid_deezerid.csv"


"senscritique/albums_tags.csv"
"senscritique/tags_meaning.csv"
"senscritique/contacts_albums_link.csv"

"senscritique/contacts.csv"
"senscritique/contacts_tracks.csv"
"senscritique/ratings.csv"



"PCS2020/isco_isei.csv"


"senscritique/senscritique_id_deezer_id_pairing.csv"
"senscritique/senscritique_deezer_id_pairing_2.csv"
"senscritique/senscritique_deezer_id_pairing_3.csv"
"senscritique/senscritique_deezer_id_pairing_4.csv"



data/regex_fixes.csv
data/artists_to_remove.csv
data/genres_from_deezer_albums.csv

data/temp/L66_Matrice_codification_PCS2020_collecte_2023.xlsx

PCS2020/L72_Matrice_codification_ISCO_collecte_2023.xlsx


data/manual_search.csv

streams_long

# french_media/music_review_BERT_tags.csv

# french_media/telerama_raw.csv
# french_media/lefigaro-complet-v0.csv
# french_media/lemonde/lemonde-20XX.csv
# french_media/liberation-complet-v2.csv
# french_media/music_reviews_manual_annotations.csv

data/area_country.csv
data/country_rank.csv







































