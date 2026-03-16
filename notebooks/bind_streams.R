library(arrow)
library(dplyr)
library(lubridate)

bucket <- arrow::s3_bucket(
  "scoavoux",
  endpoint_override = "minio.lab.sspcloud.fr"
)

options(arrow.use_threads = TRUE)

# -------------------------
# STREAMS_SHORT

streams_short <- open_dataset(
  bucket$path("records_w3/streams/streams_short"),
  partitioning = schema(REGION = utf8())
) %>%
  transmute(
    hashed_id,
    song_id = media_id,
    listening_time,
    ts_listen,
    is_listened,
    media_type
  )

# -------------------------
# STREAMS_LONG

streams_long <- open_dataset(
  bucket$path("records_w3/streams/streams_long"),
  partitioning = schema(REGION = utf8())
) %>%
  transmute(
    hashed_id,
    song_id,
    listening_time,
    ts_listen,
    is_listened,
    media_type = "song"
  )

# -------------------------
# UNION + time features

streams_all <- union_all(streams_short, streams_long) %>%
  mutate(
    dt = as_datetime(ts_listen),
    year = year(dt),
    month = month(dt)
  )

# -------------------------
# WRITE DATASET

write_dataset(
  streams_all,
  bucket$path("records_w3/new_streams/streams_all"),
  format = "parquet",
  partitioning = c("year", "month"),
  existing_data_behavior = "overwrite"
)















