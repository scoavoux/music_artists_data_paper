require(tidyverse)
require(tidytable)
require(WikidataQueryServiceR)

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

# match senscritique_id with artist_id
manual_search_file <- fread("data/manual_search.csv") # artist_id and contact_id

# ----------- chatgpt ---------------
wikidata_all <- query_wikidata('
SELECT DISTINCT ?item ?itemLabel ?mbid ?spotify_id ?deezer_id WHERE {
  OPTIONAL { ?item wdt:P434  ?mbid. }
  OPTIONAL { ?item wdt:P1902 ?spotify_id. }
  OPTIONAL { ?item wdt:P2722 ?deezer_id. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}')


# spotify id x deezer id
spotify_deezer <- query_wikidata('SELECT DISTINCT ?spotify_id ?deezer_id
  WHERE {
    ?item p:P1902 ?statement0.
    ?statement0 (ps:P1902) _:anyValueP1902.
    ?item p:P2722 ?statement1.
    ?statement1 (ps:P2722) _:anyValueP2722.
    ?item wdt:P2722 ?deezer_id.
    ?item wdt:P1902 ?spotify_id.
  }')


# wikidata_id x deezer_id
wikidata_deezer <- query_wikidata('SELECT DISTINCT ?item ?deezer_id
  WHERE {
    ?item p:P2722 ?statement1.
    ?statement1 (ps:P2722) _:anyValueP2722.
    ?item wdt:P2722 ?deezer_id.
  }')
wikidata_deezer <- wikidata_deezer %>% 
  mutate(wikidata_id = str_extract(item, "/(Q\\d+)", group = 1)) %>% 
  select(-item)

# mbid x mbname x wikidata_id
f <- s3$get_object(Bucket = "scoavoux", 
                   Key = "musicbrainz/mbid_wikidataid_pair.csv")
mbz_wikidata <- f$Body %>% rawToChar() %>% read_csv()


### Wikidata/deezer id pairs from WIKIDATA ------
wikidata_deezer <- query_wikidata('SELECT DISTINCT ?item ?deezer_id
  WHERE {
    ?item p:P2722 ?statement1.
    ?statement1 (ps:P2722) _:anyValueP2722.
    ?item wdt:P2722 ?deezer_id.
  }')
wikidata_deezer <- wikidata_deezer %>% 
  mutate(wikidata_id = str_extract(item, "/(Q\\d+)", group = 1)) %>% 
  select(-item)

f <- s3$get_object(Bucket = "scoavoux", Key = "musicbrainz/mbid_wikidataid_pair.csv")
mbz_wikidata <- f$Body %>% rawToChar() %>% read_csv()
rm(f)

mbz_wikidata <- mbz_wikidata %>% 
  select(-mbname) %>% 
  inner_join(wikidata_deezer) %>% 
  select(-wikidata_id)

























