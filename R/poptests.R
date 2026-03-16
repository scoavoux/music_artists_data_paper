

load_stream_dataset <- function(){
  
  bucket <- arrow::s3_bucket(
    "scoavoux",
    endpoint_override = "minio.lab.sspcloud.fr"
  )
  
  streams_short <- arrow::open_dataset(
    bucket$path("records_w3/streams/streams_short"),
    partitioning = arrow::schema(REGION = arrow::utf8())
  ) %>%
    select(
      hashed_id,
      song_id = media_id,
      listening_time,
      ts_listen,
      is_listened,
      media_type
    ) %>%
    filter(
      media_type == "song",
      is_listened == 1,
      song_id > 0,
      ts_listen >= 1640995200,
      ts_listen < 1672531200
    )
  
  streams_long <- arrow::open_dataset(
    bucket$path("records_w3/streams/streams_long"),
    partitioning = arrow::schema(REGION = arrow::utf8())
  ) %>%
    select(
      hashed_id,
      song_id,
      listening_time,
      ts_listen,
      is_listened
    ) %>%
    filter(
      is_listened == 1,
      song_id > 0,
      ts_listen < 1640995200
    )
  
  stream_data <- union_all(streams_short, streams_long)
  
  stream_data %>%
    mutate(
      lt = if_else(listening_time < 0, 0, listening_time)
    ) %>%
    group_by(hashed_id, song_id) %>%
    summarize(
      n_play = n(),
      l_play = sum(lt),
      .groups = "drop"
    ) %>%
    collect()
  
  return(stream_data)
}














