first_name_dict <- function(fr_names="data/firstnames.csv", 
                            n_us_names=1000, 
                            n_fr_names=2000){
  
  require(babynames)
  
  us_names <- babynames %>% 
    group_by(name) %>% 
    summarise(n = sum(n)) %>% 
    arrange(desc(n)) %>% 
    slice_head(n = n_us_names) %>% 
    select(name) %>% 
    mutate(origin = "us")
  
  fr_names <- read.csv(fr_names) %>% 
    slice_head(n = n_fr_names) %>% 
    select(name = "firstname") %>% 
    mutate(origin = "fr")
  
  first_names <- fr_names %>% 
    bind_rows(us_names) %>% 
    mutate(name = str_to_lower(name)) %>% 
    as_tibble() %>% 
    distinct()
  
  return(first_names)
}


first_names <- first_name_dict(fr_names = "data/firstnames.csv",
                               n_us_names=10, n_fr_names=2000)

first_names


tar_load(all_final)

realname_artists <- all_final %>%
  mutate(dz_name = str_normalize(dz_name)) %>%
  filter(str_count(dz_name, " ") %in% c(1:2))

realname_artists <- realname_artists %>% 
  filter(dz_stream_share > 0.0001) %>% 
  filter(!str_detect(dz_name, "&")) %>% 
  filter(!str_detect(dz_name, "\\d"))



pattern <- paste0("\\b(", paste(first_names$name, collapse = "|"), ")\\b")

t <- realname_artists %>% 
  filter(str_detect(dz_name, pattern))


















