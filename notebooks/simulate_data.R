library(dplyr)
library(stringr)
library(arrow)


set.seed(12345)

tar_load(df_complete)
df <- df_complete


# -------------------------------------------

# 1. SELECT ARTISTS TO INCLUDE
artist_sample <- df %>% 
  slice_head(n = 50) %>% 
  mutate(dz_artist_id = as.integer(dz_artist_id),
         sc_artist_id = as.integer(sc_artist_id)) %>% 
  select(dz_artist_id, dz_name, mbz_artist_id, sc_artist_id)

# -------------------------------------------


# ----------------------- dz_songs

# for old and new, subset to relevant songs
tar_load(dz_songs_new)

dz_songs_new_sample <- dz_songs_new %>% 
  rename(artists_ids = dz_artist_feat_id,
         artist_id = dz_artist_id) %>%  
  mutate(artist_id = as.integer(artist_id)) %>% 
  inner_join(artist_sample, by = c(artist_id = "dz_artist_id")) 

tar_load(dz_songs_old)

dz_songs_old_sample <- dz_songs_old %>% 
  rename(artists_ids = dz_artist_feat_id,
         artist_id = dz_artist_id) %>%  
  mutate(artist_id = as.integer(artist_id)) %>% 
  inner_join(artist_sample, by = c(artist_id = "dz_artist_id"))


write_s3(dz_songs_old_sample, "replication_data/songs.snappy.parquet")
write_s3(dz_songs_new_sample, "replication_data/song.snappy.parquet")


### RECOMPUTE ARTIST SAMPLE
artist_sample_2 <- df %>% 
  mutate(dz_artist_id = as.integer(dz_artist_id),
         sc_artist_id = as.integer(sc_artist_id)) %>% 
  inner_join(dz_songs_new_sample, by = c(dz_artist_id = "artist_id")) %>% 
  select(dz_artist_id, 
         dz_name = "dz_name.x", 
         mbz_artist_id = "mbz_artist_id.x", 
         sc_artist_id = "sc_artist_id.x")

artist_sample_2

# ---------------------- dz_users

# dz_users: create fake hashes with 

users <- load_s3("records_w3/survey/RECORDS_hashed_user_group.parquet")

users <- users %>% 
  slice_head(n = 1000) %>% 
  mutate(hashed_id = c(1:1000),
         is_respondent = sample(c(TRUE, FALSE), 
                                1000, 
                                prob = c(0.5,0.5),
                                replace = T),
         is_in_control_group = ifelse(is_respondent == FALSE, TRUE, FALSE))

write_s3(users, "replication_data/RECORDS_hashed_user_group.parquet")



# ---------------- dz_stream_data + respondent_streams

# create fake streams dataset from scratch with
# fake user hashes and relevant real right song_ids
# same for respondents. use dz_users

song_id <- dz_songs_new_sample$song_id
length(song_id)

n <- 20000
streams_long <- tibble(is_listened = 1,
                       ts_listen = round(rnorm(n, mean = 1735191661, sd = 50000)),
                       song_id = sample(rep(song_id, length.out = n)), # recycle song_id
                       listening_time = abs(round(rnorm(n, mean = 150, sd = 100))),
                       hashed_id = sample(rep(1:1000, length.out = n)))


n <- 1000
streams_short <- tibble(is_listened = 1,
                       ts_listen = round(rnorm(n, mean = 1645191661, sd = 50000)),
                       media_id = sample(rep(song_id, length.out = n)), # recycle song_id
                       media_type = "song",
                       listening_time = abs(round(rnorm(n, mean = 150, sd = 100))),
                       hashed_id = sample(rep(1:1000, length.out = T)))



# write datasets to partitioned parquet
write_dataset(
  streams_short,
  path = "data/streams/streams_short",
  format = "parquet",
  existing_data_behavior = "overwrite"
)

write_dataset(
  streams_long,
  path = "data/streams/streams_long",
  format = "parquet",
  existing_data_behavior = "overwrite"
)



# ------------------ survey_raw

# survey: sample random users, replace user hashes with
# the ones from fake streams and apply some shuffling

survey_raw <- load_s3("records_w3/survey/RECORDS_Wave3_apr_june_23_responses_corrected.csv") 
  
survey_raw <- survey_raw %>% 
  slice_sample(n = 100) %>% 
  mutate(across(all_of(colnames(survey_raw)), sample)) %>% 
  mutate(hashed_id = as.character(1:100)) %>% 
  select(hashed_id, Progress, E_birth_year, E_gender,
         E_situation_prof, E_FR_prof_femme, E_FR_prof_homme,
         E_FR_prof_retr_femme, E_FR_prof_retr_homme,
         E_statut_pub_priv, E_taille_entreprise,
         E_position_pub, E_position_priv, E_encadre, 
         E_diploma, country)

write_s3(survey_raw, "replication_data/RECORDS_Wave3_apr_june_23_responses_corrected.csv")


# --------------------------- senscritique

sc <- load_s3("senscritique/contacts.csv")

sc <- sc %>% 
  tibble() %>% 
  mutate(across(all_of(c("collection_count", "product_count")), sample))

write_s3(sc, "replication_data/contacts.csv")

# --------------------------- sc_ratings

co_alb_link <- load_s3("senscritique/contacts_albums_link.csv")
co_alb_link <- co_alb_link %>% 
  mutate(across(all_of(colnames(co_alb_link)), sample)) %>% 
  inner_join(artist_sample, by = c(contact_id = "sc_artist_id")) %>% 
  slice_sample(n = 5000)
write_s3(co_alb_link, "replication_data/contacts_albums_link.csv")

ratings <- load_s3("senscritique/ratings.csv")
ratings <- ratings %>% 
  mutate(across(all_of(colnames(ratings)), sample)) %>% 
  inner_join(co_alb_link, by = "product_id") %>% 
  slice_sample(n = 5000)
write_s3(ratings, "replication_data/ratings.csv")

tracks <- load_s3("senscritique/tracks.csv")
tracks <- tracks %>% 
  mutate(across(all_of(colnames(tracks)), sample)) %>% 
  slice_sample(n = 5000)
write_s3(tracks, "replication_data/tracks.csv")

co_tracks_link <- load_s3("senscritique/contact_tracks_link.csv")
co_tracks_link <- co_tracks_link %>% 
  mutate(across(all_of(colnames(co_tracks_link)), sample)) %>% 
  inner_join(artist_sample, by = c(contact_id = "sc_artist_id")) %>% 
  slice_sample(n = 5000)
write_s3(co_tracks_link, "replication_data/contact_tracks_link.csv")


# ---------------------------------- radio

radio <- load_s3("records_w3/radio/radio_plays_with_artist_id.csv")

radio <- radio %>% 
  inner_join(artist_sample, by = c(artist_id = "dz_artist_id")) %>% 
  slice_sample(n = 10000) %>% 
  mutate(across(all_of(colnames(radio)), sample))

write_s3(radio, "replication_data/radio_plays_with_artist_id.csv")

# ----------------------------------- mbz_deezer

mbz_deezer <- load_s3("musicbrainz/musicbrainz_urls.csv")
mbz_deezer <- mbz_deezer %>% 
  inner_join(artist_sample, by = c(artist_name = "dz_name"))
write_s3(mbz_deezer, "replication_data/musicbrainz_urls.csv")


# ---------------------------------- dz_names

artists_data <- load_s3("records_w3/items/artists_data.snappy.parquet")
artists_data <- artists_data %>% 
  inner_join(artist_sample, by = c(name = "dz_name")) %>% 
  select(artist_id, name, main_genre, dz_artist_id)
write_s3(artists_data, "replication_data/artists_data.snappy.parquet")

new_artists_names <- load_s3("interim/new_artists_names_from_api.csv")
new_artists_names <- new_artists_names %>% 
  inner_join(artist_sample, by = c(name = "dz_name"))
write_s3(new_artists_names, "replication_data/new_artists_names_from_api.csv")

# ---------------------------------- mbz_releases

mbz_releases <- load_s3("musicbrainz/musicbrainz_releases.csv")
mbz_releases <- mbz_releases %>% 
  inner_join(artist_sample, by = c(mbid = "mbz_artist_id")) %>% 
  slice_sample(n = 5000)
write_s3(mbz_releases, "replication_data/musicbrainz_releases.csv")


# ---------------------------- mbz genre

mbz_genre_album <- load_s3("musicbrainz/musicbrainz_artist_releasegroup_genre.csv")
mbz_genre_album <- mbz_genre_album %>% 
  inner_join(artist_sample, by = c(artist_mbid = "mbz_artist_id")) %>% 
  slice_sample(n = 5000)
write_s3(mbz_genre_album, "replication_data/musicbrainz_artist_releasegroup_genre.csv")


mbz_genre_artist <- load_s3("musicbrainz/musicbrainz_artist_genre.csv")
mbz_genre_artist <- mbz_genre_artist %>% 
  inner_join(artist_sample, by = c(artist_mbid = "mbz_artist_id")) %>% 
  slice_sample(n = 5000)
write_s3(mbz_genre_artist, "replication_data/musicbrainz_artist_genre.csv")


# ---------------------------

mbz_area <- load_s3("musicbrainz/musicbrainz_area.csv")
mbz_area <- mbz_area %>% 
  inner_join(artist_sample, by = c(mbid = "mbz_artist_id"))
write_s3(mbz_area, "replication_data/musicbrainz_area.csv")


# ------------------------------

dz_genre_album <- load_s3("interim/prod/genres_from_albums.parquet")
dz_genre_album <- dz_genre_album %>% 
  mutate(artist_id = as.integer(artist_id)) %>% 
  inner_join(artist_sample, by = c(artist_id = "dz_artist_id")) %>% 
  mutate(artist_id = as.character(artist_id)) %>% 
  slice_sample(n = 5000)
write_s3(dz_genre_album, "replication_data/genres_from_albums.parquet")

# ---------------------------------- 

wiki <- load_s3("interim/prod/wiki_labels.csv")
wiki <- wiki %>% 
  inner_join(artist_sample, by = c(label = "dz_name")) %>% 
  distinct(dz_artist_id, .keep_all = T)
write_s3(wiki, "replication_data/wiki_labels.csv")


# --------------- EXPORT TO LOCAL 2905

wiki_ids <- load_s3("interim/prod/wiki_ids.csv")
wiki_ids <- wiki_ids %>% 
  inner_join(artist_sample, by = c(musicBrainzID = "mbz_artist_id")) %>% 
  select(-c(dz_artist_id, dz_name, sc_artist_id))
write_s3(wiki_ids, "replication_data/wiki_ids.csv")


# ------------------

mbz_end_date <- load_s3("musicbrainz/musicbrainz_artist_end_date.csv")
mbz_end_date <- mbz_end_date %>% 
  slice_sample(n = 5000)
write_s3(mbz_end_date, "replication_data/musicbrainz_artist_end_date.csv")


# -------------------

mbz_gender <- load_s3("musicbrainz/musicbrainz_artist_gender.csv")
mbz_gender <- mbz_gender %>% 
  inner_join(artist_sample, by = c(gid = "mbz_artist_id")) %>% 
  select(gid, gender)
write_s3(mbz_gender, "replication_data/musicbrainz_artist_gender.csv")


# --------------------

gpt_gender <- load_s3("interim/prod/gpt_gender.csv")
gpt_gender <- gpt_gender %>% 
  inner_join(artist_sample, by = c(artist_id = "dz_artist_id")) %>% 
  select(artist_id, name, gender)
write_s3(gpt_gender, "replication_data/gpt_gender.csv")


# ---------------------

artists_songs_languages <- load_s3("interim/prod/artists_songs_languages.csv")
artists_songs_languages <- artists_songs_languages %>% 
  inner_join(artist_sample, by = c(art_id = "dz_artist_id"))
write_s3(artists_songs_languages, "replication_data/artists_songs_languages.csv")



# -------------- PRESS

press_ents <- load_s3("interim/press/extracted_ents_2105.csv")
press_ents <- press_ents %>% 
  arrange(desc(name_count)) %>% 
  slice_head(n = 5000)
write_s3(press_ents, "replication_data/extracted_ents_2105.csv")



# --------------- favorites

dz_favorites <- load_s3("records_w3/favorites/RECORDS_hashed_user_favorites.parquet")

n <- 5000
dz_favorites <- dz_favorites %>% 
  inner_join(dz_songs_new_sample, by = c(item_id = "song_id")) %>% 
  slice_sample(n = 5000) %>% 
  mutate(hashed_id = sample(rep(1:100, length.out = n))) %>% 
  select(item_id, item_type, hashed_id)

write_parquet(dz_favorites, "data/records_w3/favorites/RECORDS_hashed_user_favorites.parquet")


# --------------- artists_pop (for n_followers)
artists_pop <- load_s3("records_w3/artists_pop.csv")

artists_pop <- artists_pop %>% 
  inner_join(artist_sample, by = c(artist_id = "dz_artist_id")) %>% 
  select(artist_id, nb_fans)

write.csv2(artists_pop, "data/records_w3/items/artists_pop.csv")


# -------------- gender sample machin
gender_sample <- load_s3("interim/dict/gender_sample_expert_annotated.csv")

write.csv2(gender_sample, "data/interim/dict/gender_sample_expert_annotated.csv")













