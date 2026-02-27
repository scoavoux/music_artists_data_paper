

tar_load(aliases)
tar_load(all_final)

all_final <- all_final %>% 
  filter(!is.na(sc_artist_id) & !is.na(mbz_artist_id)) %>% 
  mutate(dz_name = normalize_string(dz_name),
         dz_name = regexify(dz_name)) %>% 
  select(dz_name, dz_stream_share)



ents <- read.csv("data/extracted_ents_clean.csv", sep = ";")

ents <- ents %>% 
  as_tibble() %>% 
  mutate(is_in_press = TRUE,
         press_name = normalize_string(name),
         press_name = regexify(press_name)) %>% 
  select(c(press_name, name_count, is_in_press)) #%>% 


t <- all_final %>% 
  left_join(ents, by = c(dz_name = "press_name"))


t <- t %>% 
  arrange(desc(name_count))


ents %>% 
  arrange(desc(name_count))

ents %>% 
  filter(str_detect(press_name, "boney"))
