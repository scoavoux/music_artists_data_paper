join_items <- function(file1, file2) {
  require(arrow)
  require(dplyr)
  
  items_old <- read_parquet(file1, col_select = c("song_id", "artist_id"))
  items_new <- read_parquet(file2, col_select = c("song_id", "artist_id"))
  
  items <- bind_rows(items_old, items_new) |> distinct()
  
  return(items)
}