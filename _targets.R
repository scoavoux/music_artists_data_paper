# Preparation ------
library(targets)
library(tarchetypes)

tar_option_set(
  packages = c("paws", "tidyverse", "arrow"),
  repository = "aws", 
  repository_meta = "aws",
  resources = tar_resources(
    aws = tar_resources_aws(
      endpoint = Sys.getenv("S3_ENDPOINT"),
      bucket = "scoavoux",
      prefix = "music_artist"
    )
  )
)

tar_source("R")


# List of targets ------
list(
  
    ####### USER DATA ############
    tar_target(name = streams,
               command = load_streams()),
    
    tar_target(name = to_remove_file,
               command = read.csv("data/artists_to_remove.csv")),
    
    tar_target(name = items,
               command = bind_items(items_old = "records_w3/items/songs.snappy.parquet",
                                    items_new = "records_w3/items/song.snappy.parquet",
                                    to_remove = to_remove_file)),
    tar_target(name = names,
               command = load_s3(file = "records_w3/items/artists_data.snappy.parquet"))
  
)














