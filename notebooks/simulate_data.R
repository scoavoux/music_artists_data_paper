library(dplyr)
library(stringr)
library(arrow)


set.seed(12345)

tar_load(df)


# -------------------------------------------
# -------------------------------------------


######## 1. SELECT ARTISTS TO INCLUDE
artist_sample <- df %>% 
  slice_head(n = 1000) %>% 
  mutate(dz_artist_id = as.integer(dz_artist_id)) %>% 
  select(dz_artist_id, dz_name)


# -------------------------------------------
# -------------------------------------------

######## 2. dz_songs

# for old and new, subset to relevant songs
tar_load(dz_songs_new)

dz_songs_new_sample <- dz_songs_new %>% 
  rename(artists_ids = dz_artist_feat_id,
         artist_id = dz_artist_id) %>%  
  inner_join(artist_sample, by = c(artist_id = "dz_artist_id"))

tar_load(dz_songs_old)

dz_songs_old_sample <- dz_songs_old %>% 
  rename(artists_ids = dz_artist_feat_id,
         artist_id = dz_artist_id) %>%  
  inner_join(artist_sample, by = c(artist_id = "dz_artist_id"))


write_s3(dz_songs_old_sample, "replication_data/songs_old.parquet")
write_s3(dz_songs_new_sample, "replication_data/songs_new.parquet")


# -------------------------------------------
# -------------------------------------------

# dz_users: create fake hashes with 

users <- load_s3("records_w3/RECORDS_hashed_user_group.parquet")

users <- users %>% 
  slice_head(n = 1000) %>% 
  mutate(hashed_id = c(1:1000),
         is_respondent = sample(c(TRUE, FALSE), 
                                1000, 
                                prob = c(0.5,0.5),
                                replace = T),
         is_in_control_group = ifelse(is_respondent == FALSE, TRUE, FALSE))

write_s3(users, "replication_data/hashed_user_group.parquet")



# -------------------------------------------
# -------------------------------------------

# streams: create fake streams dataset from scratch with
# fake user hashes and relevant real right song_ids
# same for respondents. use dz_users

song_id <- dz_songs_new_sample$song_id
length(song_id)

n <- 100000
streams_long <- tibble(is_listened = 1,
                       ts_listen = round(rnorm(n, mean = 1735191661, sd = 50000)),
                       song_id = sample(rep(song_id, length.out = n)), # recycle song_id
                       listening_time = abs(round(rnorm(n, mean = 150, sd = 100))),
                       hashed_id = sample(rep(1:1000, length.out = n)))


n <- 5000
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



# ----------------------------------------------

# survey: sample random users, replace user hashes with
# the ones from fake streams and apply some shuffling

tar_load(survey_raw)

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

write_s3(survey_raw, "replication_data/survey_raw_sim.csv")


# ----------------------------------------------

sc <- load_s3("senscritique/contacts.csv")

sc <- sc %>% 
  tibble() %>% 
  filter(collection_count > 50) %>% 
  mutate(across(all_of(c("collection_count", "product_count")), sample))

write_s3(sc, "replication_data/contacts_sim.csv")


ratings <- load_s3("senscritique/ratings.csv")
ratings <- ratings %>% 
  mutate(across(all_of(colnames(ratings)), sample))
write_s3(ratings, "replication_data/ratings_sim.csv")

co_alb_link <- load_s3("senscritique/contacts_albums_link.csv")
co_alb_link <- co_alb_link %>% 
  mutate(across(all_of(colnames(co_alb_link)), sample))
write_s3(co_alb_link, "replication_data/contacts_albums_link_sim.csv")

tracks <- load_s3("senscritique/tracks.csv")
tracks <- tracks %>% 
  mutate(across(all_of(colnames(tracks)), sample))
write_s3(tracks, "replication_data/tracks_sim.csv")

co_tracks_link <- load_s3("senscritique/contact_tracks_link.csv")
co_tracks_link <- co_tracks_link %>% 
  mutate(across(all_of(colnames(co_tracks_link)), sample))
write_s3(co_tracks_link, "replication_data/contact_tracks_link_sim.csv")


# ----------------------------------

radio <- load_s3("records_w3/radio/radio_plays_with_artist_id.csv")

radio <- radio %>% 
  inner_join(artist_sample, by = c(artist_id = "dz_artist_id")) %>% 
  slice_sample(n = 10000) %>% 
  mutate(across(all_of(colnames(radio)), sample))

write_s3(radio, "replication_data/radio_sim.csv")

# -----------------------------------

mbz_deezer <- load_s3("musicbrainz/musicbrainz_urls.csv")

mbz_deezer <- mbz_deezer %>% 
  inner_join(artist_sample, by = c(artist_name = "dz_name"))

write_s3(mbz_deezer, "replication_data/musicbrainz_urls_sim.csv")
























