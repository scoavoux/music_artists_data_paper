tar_load(dz_songs)
options(arrow.skip_nul = TRUE)

# select only relevant cols in dz_songs
song_artist_weights <- dz_songs %>%
  select(song_id, dz_artist_feat_id, w_feat)

# ----------------
# load user data
users <- load_s3("records_w3/RECORDS_hashed_user_group.parquet")

users <- users %>% 
  mutate(is_respondent = ifelse(is_respondent == TRUE, 1, 0),
         is_control = ifelse(is_in_control_group == TRUE, 1, 0)) %>% 
  select(hashed_id, is_respondent, is_control)



## HEY HEY HEY
## SEPARATE STREAMS COMPLETELY BY CONTROL/RESPONDENT
## THEN JUST NEED TO MAKE THE W_FEAT THING AND
## WE CAN AGGREGATE BY ARTIST





# --------------------------------------------------
# --------------------------------------------------

data_cloud <- arrow::open_dataset(
  source = arrow::s3_bucket(
    "scoavoux",
    endpoint_override = "minio.lab.sspcloud.fr"
  )$path("records_w3/streams/streams_short"),
  partitioning = arrow::schema(REGION = arrow::utf8())
)

months <- sprintf("2022-%02d", 1:12)

results <- list()

for (m in months) {
  
  cat("Processing", m, "\n")
  
  query <- data_cloud %>%
    select(
      hashed_id,
      ts_listen,
      listening_time,
      media_type,
      song_id = media_id
    ) %>%
    mutate(
      month = format(as_datetime(ts_listen), "%Y-%m"),
      lt = pmax(listening_time, 0)
    ) %>%
    filter(
      media_type == "song",
      month == m
    ) %>%
    inner_join(song_artist_weights, by = "song_id") %>%
    group_by(hashed_id, song_id, dz_artist_feat_id, w_feat) %>%
    summarise(
      n_play = n(),
      l_play = sum(lt),
      .groups = "drop"
    ) %>%
    collect()
  
  # apply weights locally (cheap now)
  query$n_play <- query$n_play * query$w_feat
  query$l_play <- query$l_play * query$w_feat
  
  results[[m]] <- query
}

streams_small <- bind_rows(results)

streams_artist <- streams_small %>%
  group_by(hashed_id, dz_artist_feat_id) %>%
  summarise(
    n_play = sum(n_play),
    l_play = sum(l_play),
    .groups = "drop"
  )




# -----------------------------------------------------

years  <- 1970:2098
months <- 1:12

results <- list()
i <- 1

data_cloud <- arrow::open_dataset(
  source = arrow::s3_bucket(
    "scoavoux",
    endpoint_override = "minio.lab.sspcloud.fr"
  )$path("records_w3/streams/streams_long")
)

for (y in years) {
  #for (m in months) {
    
    cat("Processing", y, #m, 
        "\n")
    
    query <- data_cloud %>%
      select(
        hashed_id,
        ts_listen,
        listening_time,
        #media_type,
        song_id
      ) %>%
      mutate(
        year = format(as_datetime(ts_listen), "%Y"),
        # month = format(as_datetime(ts_listen), "%Y-%m"),
        lt = pmax(listening_time, 0)
      ) %>%
      filter(
        year == y,
        #month == m,
      ) %>%
      inner_join(song_artist_weights, by = "song_id") %>%
      
      # collapse streams first
      group_by(hashed_id, song_id, dz_artist_feat_id, w_feat) %>%
      summarise(
        n_play = n(),
        l_play = sum(lt),
        .groups = "drop"
      ) %>%
      
      # apply weights
      mutate(
        n_play = n_play * w_feat,
        l_play = l_play * w_feat
      ) %>%
      
      # collapse to artist level
      group_by(hashed_id, dz_artist_feat_id) %>%
      summarise(
        n_play = sum(n_play),
        l_play = sum(l_play),
        .groups = "drop"
      ) %>%
      
      collect()
    
    results[[i]] <- query
    i <- i + 1
  #}
}

streams_all <- bind_rows(results)

streams_all <- streams_all %>%
  group_by(hashed_id, dz_artist_feat_id) %>%
  summarise(
    n_play = sum(n_play),
    l_play = sum(l_play),
    .groups = "drop"
  )



























