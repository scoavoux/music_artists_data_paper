make_artist_popularity_data <- function(user_artist, to_remove_file){
  library(tidyverse)
  library(tidytable)
  library(arrow)
  
  s3 <- initialize_s3()
  
  # separate between survey respondents and control group
  f <- s3$download_file(Bucket = "scoavoux", 
                        Key = "records_w3/RECORDS_hashed_user_group.parquet", 
                        Filename = "data/temp/RECORDS_hashed_user_group.parquet")
  us <- read_parquet("data/temp/RECORDS_hashed_user_group.parquet")
  rm(f)
  
  # make artist popularity for control group
  pop_control <- us %>% 
    filter(is_in_control_group) %>% 
    select(hashed_id) %>% 
    inner_join(user_artist) %>% # join control group with user_artist
    group_by(artist_id) %>% 
    summarise(l_play = sum(l_play, na.rm=TRUE), # length play
              n_play = sum(n_play, na.rm=TRUE), # N plays
              n_users = n_distinct(hashed_id)) %>% 
    mutate(f_l_play = l_play / sum(l_play, na.rm=TRUE), # share of length play
           f_n_play = n_play / sum(n_play, na.rm=TRUE)) %>%  # share of N plays
    rename_with(~paste0("control_", .x), -artist_id)
  
  # make artist popularity for respondent group
  pop_respondants <- us %>% 
    filter(is_respondent) %>% 
    select(hashed_id) %>% 
    inner_join(user_artist) %>% # join respondent group with user_artist
    group_by(artist_id) %>% 
    summarise(l_play = sum(l_play, na.rm=TRUE),
              n_play = sum(n_play, na.rm=TRUE),
              n_users = n_distinct(hashed_id)) %>% 
    mutate(f_l_play = l_play / sum(l_play, na.rm=TRUE),
           f_n_play = n_play / sum(n_play, na.rm=TRUE)) %>% 
    rename_with(~paste0("respondent_", .x), -artist_id)
  
  pop <- full_join(pop_control, pop_respondants)
  return(pop)
}