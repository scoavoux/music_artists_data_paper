

set.seed(12345)

tar_load(df)

######## 1. SELECT ARTISTS TO INCLUDE
artist_sample <- df %>% 
  slice_head(n = 10000) %>% 
  select(dz_artist_id)






######## 2. dz_songs

# for old and new, subset to relevant songs
tar_load(dz_songs_new)

dz_songs_new <- dz_songs_new %>% 
  mutate(dz_artist_id = as.character(dz_artist_id)) %>% 
  inner_join(artist_sample, by = "dz_artist_id")

tar_load(dz_songs_old)

dz_songs_old <- dz_songs_old %>% 
  mutate(dz_artist_id = as.character(dz_artist_id)) %>% 
  inner_join(artist_sample, by = "dz_artist_id")


# dz_users: create fake hashes with 
tar_load(dz_users)

dz_users <- dz_users %>% 
  slice_head(n = 1000) %>% 
  mutate(hashed_id = c(1:1000))


# streams: create fake streams dataset from scratch with
# fake user hashes and relevant real right song_ids

streams_long <- tibble(is_listened,
                       ts_listen,
                       song_id,
                       listening_time,
                       hashed_id) # input hashes from dz_users

streams_short <- tibble(is_listened,
                       ts_listen,
                       media_id,
                       media_type,
                       listening_time,
                       hashed_id) # input hashes from dz_users


# survey: sample random users, replace user hashes with
# the ones from fake streams
tar_load(survey_raw)

survey_raw



#####

# senscritique ratings: select sampled artists, 
# make synthetic sc_artist_ids and shuffle raw ratings

# press data: ??

# radio plays: ??






















