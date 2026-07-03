
tar_load(dz_users)

t <- df %>% 
  slice_head(n = 1000) %>% 
  select(dz_artist_id)

tar_load(dz_songs)

dz_songs <- dz_songs %>% 
  inner_join(t, by = "dz_artist_id")


song_artist_weights <- dz_songs %>%
  select(song_id, dz_artist_id, w_feat)

# ------------------------------------------------------------------
# LOAD LONG STREAMS
# ------------------------------------------------------------------

  
  path_long <- arrow::open_dataset(
    source = arrow::s3_bucket(
      "scoavoux",
      endpoint_override = "minio.lab.sspcloud.fr"
    )$path("records_w3/streams/streams_long")
  )
  

# ------------------------------------------------------------------
# LOAD SHORT STREAMS
# ------------------------------------------------------------------

  
  path_short <- arrow::open_dataset(
    source = arrow::s3_bucket(
      "scoavoux",
      endpoint_override = "minio.lab.sspcloud.fr"
    )$path("records_w3/streams/streams_short")
  )
  

# ------------------------------------------------------------------
# QUERY LONG
# ------------------------------------------------------------------

streams_long <- path_long %>%
  select(
    hashed_id,
    is_listened,
    ts_listen,
    listening_time,
    song_id
  ) %>%
  mutate(
    year = year(as_datetime(ts_listen)),
    lt = ifelse(listening_time < 0, 0, listening_time)
  ) %>%
  filter(
    is_listened == 1,
    year %in% c(2023, 2024),
    song_id > 0
  ) %>%
  inner_join(song_artist_weights, by = "song_id") %>%
  inner_join(dz_users, by = c("hashed_id")) %>%
  select(
    hashed_id,
    song_id,
    dz_artist_id,
    is_respondent,
    w_feat
  )

# ------------------------------------------------------------------
# QUERY SHORT
# ------------------------------------------------------------------

streams_short <- path_short %>%
  select(
    hashed_id,
    is_listened,
    ts_listen,
    listening_time,
    media_type,
    song_id = "media_id"
  ) %>%
  mutate(
    year = year(as_datetime(ts_listen)),
    lt = ifelse(listening_time < 0, 0, listening_time)
  ) %>%
  filter(
    media_type == "song",
    is_listened == 1,
    year == 2022,
    song_id > 0
  ) %>%
  inner_join(song_artist_weights, by = "song_id") %>%
  inner_join(dz_users, by = c("hashed_id")) %>%
  select(
    hashed_id,
    song_id,
    dz_artist_id,
    is_respondent,
    w_feat
  )

streams_query <- union_all(streams_short, streams_long)

########## ---------------------------------
########## ---------------------------------
########## ---------------------------------
########## ---------------------------------
########## ---------------------------------
########## ---------------------------------
########## ---------------------------------

streams <- query_raw_streams(path_long = "records_w3/streams/streams_long", 
                             path_short = "records_w3/streams/streams_short", 
                             dz_songs=dz_songs, 
                             dz_users=dz_users)

# ---------- N_PLAYS AND N_USERS

# n_plays at song level
plays <- streams %>%
  group_by(song_id, dz_artist_id, is_respondent, w_feat) %>%
  summarise(
    n_plays_raw = sum(w_feat / w_feat),   # unweighted count
    n_plays = sum(w_feat),                # weighted count
    .groups = "drop"
  ) %>%
  select(song_id, dz_artist_id, is_respondent,
         n_plays_raw, n_plays)

# n_users at song level
song_user_counts <- streams %>%
  group_by(song_id, dz_artist_id, is_respondent) %>%
  summarise(
    n_users = n_distinct(hashed_id),
    .groups = "drop"
  )

# ---------------------------------------------------------------
# BIND TO SONG POPULARITY
song_popularity <- plays %>%
  inner_join(
    song_user_counts,
    by = c("song_id", "dz_artist_id", "is_respondent")
  ) %>%
  collect()

# reshape to wide
song_popularity_wide <- song_popularity %>%
  mutate(group = ifelse(is_respondent == 1, "respondent", "control")) %>%
  select(-is_respondent) %>%
  pivot_wider(
    names_from = group,
    values_from = c(n_plays_raw, n_plays, n_users),
    names_glue = "{.value}_{group}"
  ) %>%
  rename(
    n_plays_raw = n_plays_raw_control,
    n_plays = n_plays_control,
    n_users = n_users_control
  ) %>%
  filter(!is.na(n_plays))


song_popularity <- song_popularity_wide %>% 
  group_by(dz_artist_id) %>% 
  mutate(n_plays_share = n_plays_raw / sum(n_plays_raw)) %>% 
  ungroup()


library(ineq)

artist_concentration <- song_popularity %>%
  group_by(dz_artist_id) %>%
  arrange(desc(n_plays_raw), .by_group = TRUE) %>%
  mutate(rank = row_number()) %>%
  summarise(
    
    # size of catalogue
    n_songs = n(),
    
    # cumulative share of top songs
    top1_share  = sum(n_plays_share[rank <= 1]),
    top5_share  = sum(n_plays_share[rank <= 5]),
    top10_share = sum(n_plays_share[rank <= 10]),
    
    # h-index
    h_index = max(c(0, which(n_plays_raw >= rank))),
    
    # Shannon effective diversity (your function)
    shannon_effective = prod(n_plays_share^n_plays_share)^(-1),
    
    # gini coefficient
    gini = ineq(n_plays_raw, type = "Gini"),
    
    # songs needed to reach 80% of n_plays
    songs_for_80 = {
      cs <- cumsum(n_plays_share)
      which(cs >= 0.8)[1]
    },
    
    
    .groups = "drop"
  )

names <- df %>% 
  select(dz_artist_id, dz_name)


artist_concentration <- names %>% 
  inner_join(artist_concentration, by = "dz_artist_id")

write.csv2(artist_concentration, "data/song_concentration_1000.csv")





