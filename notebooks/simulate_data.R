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
  select(dz_artist_id)


# -------------------------------------------
# -------------------------------------------

######## 2. dz_songs

# for old and new, subset to relevant songs
tar_load(dz_songs_new)

dz_songs_new_sample <- dz_songs_new %>% 
  #slice_sample(n = 10000) %>% # adjust
  rename(artists_ids = dz_artist_feat_id,
         artist_id = dz_artist_id) %>%  
  inner_join(artist_sample, by = c(artist_id = "dz_artist_id"))

tar_load(dz_songs_old)

dz_songs_old_sample <- dz_songs_old %>% 
  slice_sample(n = 10000) %>% # adjust
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
  mutate(hashed_id = c(1:1000))

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
                       ts_listen = round(rnorm(n, mean = 1735191661, sd = 10000000)),
                       song_id = sample(rep(song_id, length.out = n)), # recycle song_id
                       listening_time = abs(round(rnorm(n, mean = 150, sd = 100))),
                       hashed_id = sample(rep(1:1000, length.out = n)))


n <- 5000
streams_short <- tibble(is_listened = 1,
                       ts_listen = round(rnorm(n, mean = 1645191661, sd = 10000000)),
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
  mutate(hashed_id = 1:nrow(survey_raw))























