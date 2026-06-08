# load artists' countries from musicbrainz
# recode areas (eg cities or regions) to countries
# and rank countries by defined dict
make_artist_country <- function(mbz_area_file,
                                area_country_file,
                                country_rank_file){
  
  # AREAS TO COUNTRIES MAPPING
  area_country <- load_s3(area_country_file) %>% 
    filter(!is.na(country)) %>% 
    rename(area_name = "name") %>% 
    mutate(country = ifelse(type_name == "Country", area_name, country)) %>% 
    as_tibble() %>% 
    select(-n)
  
  # COUNTRY RANK FILE
  country_rank <- load_s3(country_rank_file) %>% 
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
    left_join(area_country, by = "area_name") %>% 
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


# LOAD GENDER FROM MBZ AND GPT, COALESCE BOTH
make_artist_gender <- function(artists, mbz_gender_file, gpt_gender_file){
  
  require(stringr)
  
  mbz_gender <- load_s3(mbz_gender_file)
  
  mbz_gender <- mbz_gender %>% 
    rename(mbz_artist_id = "artist_mbid") %>% 
    mutate(gender = str_to_lower(gender),
           gender = ifelse(gender == "non-binary", "nonbinary", gender)) %>% 
    filter(gender %in% c("female", "male", "nonbinary")) %>% 
    as_tibble()
  
  gpt_gender <- load_s3(gpt_gender_file)
  
  gpt_gender <- gpt_gender %>% 
    mutate(dz_artist_id = as.character(artist_id)) %>% 
    filter(gender %in% c("female", "male", "nonbinary")) %>% 
    select(dz_artist_id, gender) %>%
    as_tibble()
  
  artists <- artists %>% 
    select(dz_artist_id, mbz_artist_id)
  
  mbz_gpt_gender <- artists %>% 
    
    # mbz if available
    left_join(mbz_gender, by = "mbz_artist_id") %>%
    rename(gender_mbz = gender) %>%
    
    # else gpt
    left_join(gpt_gender, by = "dz_artist_id") %>%
    rename(gender_gpt = gender) %>%
    
    mutate(
      gender = coalesce(gender_mbz, gender_gpt)
    ) %>%
    
    select(dz_artist_id, gender) %>% 
    
    distinct(dz_artist_id, .keep_all = TRUE) # TEMP: TO RESOLVE DUPLICATE 2244301
  
  return(mbz_gpt_gender)
}


# sub <- df %>%
#   select(dz_artist_id, mbz_artist_id, dz_name, n_plays)
# 
# mbz_gender <- load_s3("musicbrainz/musicbrainz_artist_gender.csv") %>%
#   left_join(sub, by = c(artist_mbid = "mbz_artist_id"))

# gpt_gender <- load_s3("interim/prod/gpt_gender.csv")
# 
# table(mbz_gender$gender)
# table(gpt_gender$gender)
# 
# 
# 
# tar_load(mbz_gpt_gender)
# table(mbz_gpt_gender$gender)

# tar_load(df)
# 
# mbz_gender <- mbz_gender %>% 
#   rename(mbz_artist_id = "artist_mbid") %>% 
#   mutate(gender = str_to_lower(gender),
#          gender = ifelse(gender == "non-binary", "nonbinary", gender)) %>% 
#   filter(gender %in% c("female", "male", "nonbinary")) %>% 
#   as_tibble()
# 
# 
# mbz_gender <- df %>% 
#   
#   # mbz if available
#   left_join(mbz_gender, by = "mbz_artist_id") %>%
#   rename(gender_mbz_2 = gender.y) 
# 
# mbz_gender <- mbz_gender %>% 
#   filter(is.na(gender_mbz_2))
# 
# 
# sample_gender <- mbz_gender %>% 
#   slice_sample(n = 200) %>% 
#   select(dz_artist_id = "dz_artist_id.x", 
#          mbz_artist_id,
#          dz_name = "dz_name.x")
# 
# write.csv(sample_gender, "data/gender_sample_200.csv")
# 

