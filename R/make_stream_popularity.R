
# queries for long and short streams 
# no collect() yet
query_raw_streams <- function(
    path_long = "records_w3/streams/streams_long",
    path_short = "records_w3/streams/streams_short",
    dz_songs = dz_songs,
    dz_users = dz_users,
    simulation = SIMULATION,
    local_dir = LOCAL_DATA_DIR
) {
  
  song_artist_weights <- dz_songs %>%
    mutate(song_title_norm = str_normalize_titles(song_title)) %>%
    select(song_id, song_title_norm, dz_artist_id, w_feat)
  
  # ------------------------------------------------------------------
  # LOAD LONG STREAMS
  # ------------------------------------------------------------------
  
  if (simulation) {
    
    path_long <- arrow::open_dataset(
      file.path(local_dir, path_long)
    )
    
  } else {
    
    path_long <- arrow::open_dataset(
      source = arrow::s3_bucket(
        "scoavoux",
        endpoint_override = "minio.lab.sspcloud.fr"
      )$path(path_long)
    )
    
  }
  
  # ------------------------------------------------------------------
  # LOAD SHORT STREAMS
  # ------------------------------------------------------------------
  
  if (simulation) {
    
    path_short <- arrow::open_dataset(
      file.path(local_dir, path_short)
    )
    
  } else {
    
    path_short <- arrow::open_dataset(
      source = arrow::s3_bucket(
        "scoavoux",
        endpoint_override = "minio.lab.sspcloud.fr"
      )$path(path_short)
    )
    
  }
  
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
      song_title_norm,
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
      song_title_norm,
      dz_artist_id,
      is_respondent,
      w_feat
    )
  
  streams_query <- union_all(streams_short, streams_long)
  
  return(streams_query)
}



#### create streams df with cols song_id and n_plays
#### remove songs with no listens, negative song_ids
make_stream_popularity <- function(dz_songs, dz_users){
  
  streams <- query_raw_streams(path_long = "records_w3/streams/streams_long", 
                               path_short = "records_w3/streams/streams_short", 
                               dz_songs = dz_songs, 
                               dz_users = dz_users)
  
  # ---------- N_PLAYS AND N_USERS
  # make n_plays at group-song level
  plays <- streams %>%
    group_by(song_id, dz_artist_id, 
             is_respondent, w_feat) %>%
    summarise(n_plays_raw = sum(w_feat / w_feat), # cancel weights
              n_plays = sum(w_feat),
              .groups = "drop") %>% 
    select(song_id, dz_artist_id, is_respondent, 
           n_plays_raw, n_plays)
  
  
  # make n_users at group-artist levels
  artist_user_counts <- streams %>%
    group_by(dz_artist_id, is_respondent) %>%
    summarise(
      n_users = n_distinct(hashed_id),
      .groups = "drop"
    ) %>% 
    select(dz_artist_id, is_respondent, n_users)
  
  
  # ---------------------------------------------------------------
  # BIND TO ARTIST POPULARITY
  artist_popularity <- plays %>%
    
    # aggregate dz_stream_count to group
    group_by(dz_artist_id, is_respondent) %>%
    summarise(
      n_plays_raw = sum(n_plays_raw),
      n_plays = sum(n_plays, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    
    # add dz_stream_user_count (agg by group already)
    inner_join(
      artist_user_counts,
      by = c("dz_artist_id", "is_respondent")
    ) %>%
    
    collect()
  
  # reshape to wide
  artist_popularity_wide <- artist_popularity %>% 
    mutate(group = ifelse(is_respondent == 1, "respondent", "control")) %>%
    select(-is_respondent) %>%
    pivot_wider(
      names_from = group,
      values_from = c(n_plays_raw, n_plays, n_users),
      names_glue = "{.value}_{group}"
    ) %>% 
    rename(
      n_plays_raw = "n_plays_raw_control",
      n_plays = "n_plays_control"
      ) %>% 
    
    # filter out artists with no plays in control group
    filter(!is.na(n_plays)) 
  
  return(artist_popularity_wide)
}



# make user_artist data for respondents --> is needed for user demographics per artist
# separate function because user-song level calculations break R if applied to all users
make_respondent_plays <- function(dz_songs, dz_users){
  
  streams <- query_raw_streams(path_long = "records_w3/streams/streams_long", 
                               path_short = "records_w3/streams/streams_short", 
                               dz_songs=dz_songs, dz_users=dz_users)
  
  respondent_plays <- streams %>%
    filter(is_respondent == 1) %>% 
    group_by(hashed_id, song_id, dz_artist_id, w_feat) %>%
    summarise(n_plays_raw = sum(w_feat / w_feat),
              n_plays = sum(w_feat),
              .groups = "drop") %>% 
    
    group_by(hashed_id, dz_artist_id) %>%
    summarise(
      n_plays_raw = sum(w_feat / w_feat),
      n_plays = sum(n_plays, na.rm = TRUE),
      .groups = "drop"
    )
  
  n_users <- respondent_plays %>%
    group_by(dz_artist_id) %>%
    summarise(
      n_users_respondent = n(),
      .groups = "drop"
    )
  
  respondent_plays <- respondent_plays %>%
    left_join(n_users, by = "dz_artist_id") %>%
    collect()
  
  return(respondent_plays)
}

# using streams and songs, make different metrics on
# inequality in popularity within an artist's catalogue
make_song_diversity <- function(dz_songs, 
                                dz_users, 
                                path_long="records_w3/streams/streams_long", 
                                path_short="records_w3/streams/streams_short"){
  
  # ---------------------------------------------------------------
  # STREAMS (CONTROL GROUP ONLY)
  # ---------------------------------------------------------------
  
  streams <- query_raw_streams(
    path_long = path_long,
    path_short = path_short,
    dz_songs = dz_songs,
    dz_users = dz_users
  ) %>%
    filter(is_respondent == 0)
  
  # ---------------------------------------------------------------
  # SONG-LEVEL POPULARITY
  # ---------------------------------------------------------------
  
  isrc <- load_s3("records_w3/items/song_ids_isrc_matched.csv")
  isrc <- isrc %>% 
    distinct() # rm perfect duplicates
  
  streams <- streams %>% 
    left_join(isrc, by = "song_id")
  
  song_popularity <- streams %>%
    group_by(isrc, dz_artist_id) %>%
    summarise(
      n_plays_raw = n(),
      n_plays = sum(w_feat),
      n_users = n_distinct(hashed_id),
      .groups = "drop"
    ) %>%
    collect()
  
  # ---------------------------------------------------------------
  # SONG SHARE WITHIN ARTIST
  # ---------------------------------------------------------------
  
  song_popularity <- song_popularity %>%
    group_by(dz_artist_id) %>%
    mutate(
      n_plays_share = n_plays_raw / sum(n_plays_raw)
    ) %>%
    ungroup()
  
  # ---------------------------------------------------------------
  # ARTIST CONCENTRATION / INEQUALITY
  # ---------------------------------------------------------------
  
  song_diversity <- song_popularity %>%
    group_by(dz_artist_id) %>%
    arrange(desc(n_plays_raw), .by_group = TRUE) %>%
    mutate(
      rank = row_number(),
      cum_share = cumsum(n_plays_share)
    ) %>%
    summarise(
      
      # concentration
      div_top1_share = first(n_plays_share),
      div_top5_share = sum(head(n_plays_share, 5)),
      div_top10_share = sum(head(n_plays_share, 10)),
      
      # h-index
      div_h_index = {
        x <- sort(n_users, decreasing = TRUE)
        max(c(0, which(x >= seq_along(x))))
      },
      
      # Shannon effective diversity
      div_shannon_effective = prod(n_plays_share^n_plays_share)^(-1),
      
      # Songs required for 80% of listening
      div_songs_for_80 = which(cum_share >= .8)[1],
      
      .groups = "drop"
    )
  
  return(song_diversity)
  
}



# df_complete <- df_complete %>% 
#   select(dz_artist_id, 
#          dz_name,
#          n_plays,
#          starts_with("div_"))
# 
# write.csv2(df_complete, "data/df_div_song_title.csv")
# 




















