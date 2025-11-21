# Preparation ------
library(targets)
library(tarchetypes)

tar_option_set(
  packages = c("paws", "tidyverse", "arrow"),
  repository = "aws", 
  repository_meta = "aws",
  error = "null",
  resources = tar_resources(
    aws = tar_resources_aws(
      endpoint = Sys.getenv("S3_ENDPOINT"),
      bucket = "scoavoux",
      prefix = "music_artists"
    )
  )
)

tar_source("R")

# List of targets ------
list(
  
    ####### USER DATA ############
  
    # load artists to remove
    tar_target(name = to_remove_file_path, 
               command = "data/artists_to_remove.csv", format = "file"),
    tar_target(name = to_remove_file,
               command = read_csv(to_remove_file_path)),
    
  
    # load and join items (song-to-artist pairing from deezer)
    # moved to_remove_file from user_artist to items!!
    tar_target(name = items,
               command = join_items(to_remove_file)),
   
   # map artist_id to hashed
   tar_target(name = user_artist,
             command = make_user_artist_2022(items)),
   
   # compute popularity from user_artist
   tar_target(name = artists_pop, 
              command = make_artist_popularity_data(user_artist)),
   
   # load artist names from artists_data.snappy.parquet
   tar_target(name = names,
              command = make_artist_names(to_remove_file)),
   
   # left_join artists_pop and names
   tar_target(name = pop,
              command = right_join(names, 
                                   artists_pop, by="artist_id")),
   
   # load manual_search_file
   tar_target(name = manual_search_file_path,
              command = "data/manual_search.csv", format = "file"),
   tar_target(name = manual_search_file,
              command = read_csv(manual_search_file_path))
   

)

setwd('./music_artists_data_paper')













