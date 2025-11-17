# placeholder for computing of master id
make_artist_id <- function(clean_items){
  artist_id <- clean_items$artist_id
  return(artist_id)
}


# match senscritique_id with artist_id (from deezer)
manual_search_file <- fread("data/manual_search.csv")


# spotify id x deezer id
wikidata_spotify_deezer <- query_wikidata('SELECT DISTINCT ?spotify_id ?deezer_id
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
s3 <- initialize_s3()
f <- s3$get_object(Bucket = "scoavoux", 
                   Key = "musicbrainz/mbid_wikidataid_pair.csv")
mbz_wikidata <- f$Body %>% rawToChar() %>% read_csv()


















