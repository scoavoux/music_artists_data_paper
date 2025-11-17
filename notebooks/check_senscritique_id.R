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

manual_search_file_path <- fread("data/manual_search.csv")

## senscritique pairings

### SensCritique / deezer id ------
#### From manual search by me... highly trustworthy ------
pairings0 <- manual_search_file

#### from Deezer api search (old) ------
f <- s3$get_object(Bucket = "scoavoux", 
                   Key = "senscritique/senscritique_id_deezer_id_pairing.csv")
pairings1 <- f$Body %>% rawToChar() %>% read_csv()
rm(f)

f <- s3$get_object(Bucket = "scoavoux", 
                   Key = "senscritique/senscritique_deezer_id_pairing_2.csv")
pairings2 <- f$Body %>% rawToChar() %>% read_csv()
rm(f)

#### From exact matches ------
## exact match between artists in dz and sc.
## Only those with unique match
## see script pair_more_artists
f <- s3$get_object(Bucket = "scoavoux", 
                   Key = "senscritique/senscritique_deezer_id_pairing_3.csv")
pairings3 <- f$Body %>% rawToChar() %>% read_csv()
rm(f)  

## Exact match but allow multiple matches -- script will collapse them
## together afterwards
f <- s3$get_object(Bucket = "scoavoux", 
                   Key = "senscritique/senscritique_deezer_id_pairing_4.csv")
pairings4 <- f$Body %>% rawToChar() %>% read_csv()
rm(f)  

#### Put them together ------
pairings <- pairings1 %>% 
  select(contact_id, artist_id) %>% 
  bind_rows(pairings0,
            select(pairings2, contact_id, artist_id = "deezer_id"),
            pairings3,
            pairings4) %>% 
  distinct()

pairings <- pairings %>% 
  filter(!is.na(artist_id))
