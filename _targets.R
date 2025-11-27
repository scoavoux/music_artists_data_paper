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
    tar_target(streams,
               command = load_streams())
              

)









