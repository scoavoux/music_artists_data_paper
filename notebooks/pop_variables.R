tar_load(dz_songs)
options(arrow.skip_nul = TRUE)

options(scipen = 99)

library(tidyr)

# -------------------
song_artist_weights <- dz_songs %>% 
  select(song_id, dz_artist_feat_id, w_feat)

# ----------------
# load user data
users <- load_s3("records_w3/RECORDS_hashed_user_group.parquet")

users <- users %>% 
  mutate(is_respondent = ifelse(is_respondent == TRUE, 1, 0),
         is_control = ifelse(is_in_control_group == TRUE, 1, 0)) %>% 
  filter(is_respondent != 0 | is_control != 0) %>% # remove users who are neither
  select(hashed_id, is_respondent, is_control)


# ---------------------------------------------------------------
# STREAMS
path_long <- arrow::open_dataset(
  source = arrow::s3_bucket("scoavoux",
                              endpoint_override = "minio.lab.sspcloud.fr")
  $path("records_w3/streams/streams_long")
)

streams_long <- path_long %>%
  select(hashed_id,
         is_listened,
         ts_listen,
         listening_time,
         song_id
         ) %>%
  mutate(year = year(as_datetime(ts_listen)),
         lt = ifelse(listening_time < 0, 0, listening_time)) %>%
  filter(is_listened == 1,
         year %in% c(2023, 2024),
         song_id > 0) %>%
  inner_join(song_artist_weights, by = "song_id") %>% 
  inner_join(users, by = c("hashed_id")) %>%
  select(hashed_id, song_id, dz_artist_feat_id, is_respondent, w_feat)


path_short <- arrow::open_dataset(
  source = arrow::s3_bucket("scoavoux",
                            endpoint_override = "minio.lab.sspcloud.fr")
  $path("records_w3/streams/streams_short")
)

streams_short <- path_short %>% 
  select(hashed_id, 
         is_listened, 
         ts_listen, 
         listening_time, 
         media_type, 
         song_id = "media_id"
  ) %>% 
  mutate(year = year(as_datetime(ts_listen)),
         lt = ifelse(listening_time < 0, 0, listening_time)) %>% 
  filter(media_type == "song", 
    is_listened == 1, 
    year == 2022,
    song_id > 0) %>% 
  inner_join(song_artist_weights, by = "song_id") %>% 
  inner_join(users, by = c("hashed_id")) %>%
  select(hashed_id, song_id, dz_artist_feat_id, is_respondent, w_feat)


streams <- union_all(streams_short, streams_long)


# make n_plays at group-song level
plays <- streams %>%
  group_by(song_id, dz_artist_feat_id, 
           is_respondent, w_feat) %>%
  summarise(n_plays = n(),
            .groups = "drop") %>% 
  mutate(n_plays = n_plays * w_feat) %>% 
  select(song_id, dz_artist_feat_id, is_respondent, n_plays)


# make n_users at group-artist levels
artist_user_counts <- streams_short %>%
  group_by(dz_artist_feat_id, is_respondent) %>%
  summarise(
    n_users = n_distinct(hashed_id),
    .groups = "drop"
  ) %>% 
  select(dz_artist_feat_id, is_respondent, n_users)


# ---------------------------------------------------------------
# ARTIST POPULARITY
artist_popularity <- plays %>%
  
  # aggregate dz_stream_count to group
  group_by(dz_artist_feat_id, is_respondent) %>%
  summarise(
    n_plays = sum(n_plays, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  
  # add dz_stream_user_count (agg by group already)
  inner_join(
    artist_user_counts,
    by = c("dz_artist_feat_id", "is_respondent")
  ) %>%
  
  collect()


head(artist_popularity)


# reshape to wide
artist_popularity_wide <- artist_popularity %>% 
  mutate(group = ifelse(is_respondent == 1, "respondent", "control")) %>%
  select(-is_respondent) %>%
  pivot_wider(
    names_from = group,
    values_from = c(n_plays, n_users),
    names_glue = "{.value}_{group}"
  ) %>% 
  rename(n_plays = "n_plays_control",
         n_users = "n_users_control")
  
# for sanity checks
artist_popularity_wide <- artist_popularity_wide %>% 
  mutate(
    n_plays_share = (n_plays / sum(n_plays, na.rm = T)) * 100,
    n_plays_share_respondent = (n_plays_respondent / sum(n_plays_respondent, na.rm = T)) * 100
  )

all_final <- all_final %>% 
  left_join(artist_popularity_wide, by = c(dz_artist_id = "dz_artist_feat_id")) %>% 
  arrange(desc(n_plays)) %>% 
  select(dz_name, dz_stream_share, n_plays_share)


missing_artists <- all_final %>% 
  filter(is.na(n_plays))
print_stream_share(missing_artists) # 0.2% stream share missing

missing_artists_resp <- all_final %>% 
  filter(is.na(n_plays_respondent))
print_stream_share(missing_artists_resp) # 0.2% stream share missing

# Jul and Ninho as big outliers, else kind of okay-ish
all_final_clean <- all_final %>% 
  filter(!is.na(n_plays_share)) %>% 
  mutate(diff = abs(log(n_plays_share) / log(dz_stream_share))) %>% 
  arrange(desc(diff))

head(streams)


all_final_clean %>% 
  filter(dz_name == "The Beatles")
















