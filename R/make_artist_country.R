# load artists' countries from musicbrainz
# recode areas (eg cities or regions) to countries
# and rank countries by defined dict
make_artist_country <- function(mbz_area_file,
                                area_to_country_file,
                                country_rank_file){
  
  # AREAS TO COUNTRIES MAPPING
  area_to_country <- read.csv(area_to_country_file) %>% 
    filter(!is.na(country)) %>% 
    rename(area_name = "name") %>% 
    mutate(country = ifelse(type_name == "Country", area_name, country)) %>% 
    as_tibble() %>% 
    select(-n)
  
  # COUNTRY RANK FILE
  country_rank <- read.csv(country_rank_file) %>% 
    rename(country = "Country", rank = "Rank") %>% 
    as_tibble()
  
  mbz_artist_area <- load_s3(mbz_area_file)
  
  mbz_artist_area <- mbz_artist_area %>%
    as_tibble() %>% 
    separate(area_type, into = c("area_type_id", "area_type"),
             sep = ",",
             remove = TRUE) %>%
    mutate(
      mbz_artist_id = mbid,
      area_type_id = as.integer(gsub("[()]", "", area_type_id)),
      area_type = gsub("[()]", "", area_type)
      ) %>% 
    left_join(area_to_country, by = "area_name") %>% 
    distinct(mbz_artist_id, country) %>% 
    
    left_join(country_rank, by = "country") %>% 
    arrange(mbz_artist_id, rank) %>% 
    group_by(mbz_artist_id) %>% 
    slice(1) %>% 
    ungroup() %>% 
    select(mbz_artist_id,
           artist_country = "country")
  
  
  return(mbz_artist_area)
  
}