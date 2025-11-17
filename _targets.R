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
  
    ####### ARTIST ID #############

    # list artist names
    # add steps to exclude names and aliases

    # placeholder function. artist_id should not inherit from clean_items
    tar_target(name = artist_id,
             command = make_artist_id(clean_items)),
  
    ####### USER DATA ############
   # tar_target(name = items,
   #            command = join_items(file1 = download_s3(key="records_w3/items/songs.snappy.parquet",
   #                                                     local="data/temp/songs.snappy.parquet"),
   #                                 file2 = download_s3(key="records_w3/items/song.snappy.parquet",
   #                                                     local="data/temp/song.snappy.parquet"))), # pass data as arguments. see if implement this elsewhere
   # tar_target(name = clean_items, 
   #          command = remove_artists(items = items,
   #                                   to_remove = "data/artists_to_remove.csv")),
   # tar_target(name = user_data,
   #           command = make_user_data(clean_items)), # time-intensive
  #
    ####### ARTIST DEMOGRAPHICS ##########
    #tar_target(name = artist_gender,
    #          command = make_artist_gender())
  
)

  
















