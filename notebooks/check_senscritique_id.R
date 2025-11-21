require(tidyverse)
require(tidytable)
require(WikidataQueryServiceR)
library(targets)

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
s3 <- initialize_s3()


## CONTACT ID - 776K unique
f <- s3$get_object(Bucket = "scoavoux", 
                   Key = "senscritique/contacts.csv")
co <- f$Body %>% rawToChar() %>% fread() %>% distinct()
rm(f)

f <- s3$get_object(Bucket = "scoavoux", 
                   Key = "senscritique/contacts_tracks.csv")
co_tr <- f$Body %>% rawToChar() %>% fread()
rm(f)
co <- bind_rows(co, co_tr) %>% distinct()

co <- co[,c("contact_id", "contact_name", "mbz_id", "spotify_id")]

length(unique(co$spotify_id))
























