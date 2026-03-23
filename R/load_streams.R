
# queries for long and short streams 
# no collect() yet
query_raw_streams <- function(path_long = "records_w3/streams/streams_long", 
                              path_short = "records_w3/streams/streams_short", 
                              dz_songs=dz_songs, dz_users=dz_users){
  
  song_artist_weights <- dz_songs %>% 
    select(song_id, dz_artist_feat_id, w_feat)
  
  # ---------------------------------------------------------------
  # LOAD STREAMS
  path_long <- arrow::open_dataset(
    source = arrow::s3_bucket("scoavoux",
                              endpoint_override = "minio.lab.sspcloud.fr")
    $path(path_long)
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
    inner_join(dz_users, by = c("hashed_id")) %>%
    select(hashed_id, song_id, dz_artist_feat_id, is_respondent, w_feat)
  
  
  path_short <- arrow::open_dataset(
    source = arrow::s3_bucket("scoavoux",
                              endpoint_override = "minio.lab.sspcloud.fr")
    $path(path_short)
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
    inner_join(dz_users, by = c("hashed_id")) %>%
    select(hashed_id, song_id, dz_artist_feat_id, is_respondent, w_feat)
  
  
  streams_query <- union_all(streams_short, streams_long)
  
  return(streams_query)
  
}



#### create streams df with cols song_id and f_n_plays
#### remove songs with no listens, negative song_ids
make_stream_popularity <- function(dz_songs, dz_users){
  
  streams <- query_raw_streams(path_long = "records_w3/streams/streams_long", 
                               path_short = "records_w3/streams/streams_short", 
                               dz_songs=dz_songs, dz_users=dz_users)
  
  # ---------- N_PLAYS AND N_USERS
  # make n_plays at group-song level
  plays <- streams %>%
    group_by(song_id, dz_artist_feat_id, 
             is_respondent, w_feat) %>%
    summarise(n_plays = sum(w_feat),
              .groups = "drop") %>% 
    select(song_id, dz_artist_feat_id, is_respondent, n_plays)
  
  
  # make n_users at group-artist levels
  artist_user_counts <- streams %>%
    group_by(dz_artist_feat_id, is_respondent) %>%
    summarise(
      n_users = n_distinct(hashed_id),
      .groups = "drop"
    ) %>% 
    select(dz_artist_feat_id, is_respondent, n_users)
  
  
  # ---------------------------------------------------------------
  # BIND TO ARTIST POPULARITY
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
           n_users = "n_users_control",
           dz_artist_id = "dz_artist_feat_id") %>% 
    
    # filter out artists with no plays in control group
    filter(!is.na(n_plays)) 
  
  return(artist_popularity_wide)
}



# make user_artist data for respondents
# needed for user demographics per artist
# separate function because user-song level calculations
# break R if applied to all users
make_respondent_plays <- function(dz_songs, dz_users){
  
  streams <- query_raw_streams(path_long = "records_w3/streams/streams_long", 
                               path_short = "records_w3/streams/streams_short", 
                               dz_songs=dz_songs, dz_users=dz_users)
  
  respondent_plays <- streams %>%
    filter(is_respondent == 1) %>% 
    group_by(hashed_id, song_id, dz_artist_feat_id, w_feat) %>%
    summarise(n_plays = sum(w_feat),
              .groups = "drop") %>% 

    group_by(hashed_id, dz_artist_feat_id) %>%
    summarise(
      n_plays = sum(n_plays, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    
    collect()
  
  return(respondent_plays)
}














