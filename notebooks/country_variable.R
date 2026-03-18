################################################

# Make artist area data ------
make_artist_area_data <- function(area_country_file,
                                  country_rank_file,
                                  mbid_area_file = "musicbrainz/mbid_area.csv",
                                  area_names_file = "musicbrainz/area_names.csv",
                                  area_types_file = "musicbrainz/area_types.csv",
                                  mbid_deezerid_file = "musicbrainz/mbid_deezerid.csv"){
  s3 <- initialize_s3()
  mbid_deezerid <- s3$get_object(Bucket = "scoavoux", Key = mbid_deezerid_file)$Body %>% 
    rawToChar() %>% 
    read_csv()
  mbid_area <- s3$get_object(Bucket = "scoavoux", Key = mbid_area_file)$Body %>% 
    rawToChar() %>% 
    read_csv()
  area_names <- s3$get_object(Bucket = "scoavoux", Key = area_names_file)$Body %>% 
    rawToChar() %>% 
    read_csv()
  area_types <- s3$get_object(Bucket = "scoavoux", Key = area_types_file)$Body %>% 
    rawToChar() %>% 
    read_csv()
  deezerid_area <- mbid_deezerid %>% 
    inner_join(mbid_area, by = c(mbid = "gid")) %>% 
    inner_join(area_names, by = c(area = "id")) %>% 
    inner_join(rename(area_types, type_name = "name"), by = c(type = "id")) %>% 
    select(-mbid, -type, -area) 
  
  # todo! complete this file. Use a LLM?
  area_country <- read_csv(area_country_file) %>% 
    filter(!is.na(country)) %>% 
    select(-n)
  
  deezerid_country <- deezerid_area %>% 
    left_join(area_country) %>% 
    mutate(name = ifelse(!is.na(country), country, name),
           type_name = ifelse(!is.na(country), "Country", type_name)) %>% 
    filter(type_name == "Country") %>% 
    select(artist_id, country = "name") %>% 
    distinct()
  
  ## Finally we need only a single country for each artist. To ease the process
  ## we rank countries, with France first, French speaking countries (Canada,
  ## Switzerland, Belgium) second, culturally dominant English speaking 
  ## countries third (US, UK, Australia, Ireland, New Zealand), the rest
  ## after. The idea is that if an artist has a link to France, they will
  ## be considered French.
  
  country_rank <- read_csv(country_rank_file) %>% 
    rename(country = "Country", rank = "Rank")
  
  deezerid_country <- deezerid_country %>% 
    left_join(country_rank) %>% 
    arrange(artist_id, rank) %>% 
    group_by(artist_id) %>% 
    slice(1) %>% 
    ungroup() %>% 
    select(-rank)
  return(deezerid_country)
}


























