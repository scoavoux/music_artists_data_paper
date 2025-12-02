source("./R/api_name_scraper.R")

conflicts_na_names <- conflicts %>%
  filter(is.na(name_new_id)) %>% 
  select(deezer_id.old, name_old_id)



scrape_names_from_api(dat = conflicts_na_names)



























