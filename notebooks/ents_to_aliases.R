# run tests on matching entities to alias/dz_names
## which names match, which names don't, and why?
## do we need aliases? if yes, for whom?


# ------------------- LOAD AND CLEAN DATA
tar_load(aliases)
tar_load(all_final)

all_final <- all_final %>% 
  filter(!is.na(sc_artist_id) & !is.na(mbz_artist_id)) %>% 
  mutate(dz_name = normalize_string(dz_name))

ents <- read.csv("data/extracted_ents_ALL.csv", sep = ";")

ents <- ents %>% 
  as_tibble() %>% 
  mutate(is_in_press = TRUE,
         press_name = normalize_string(name)) %>% 
  select(c(press_name, name_count, is_in_press)) #%>% 
  #distinct(press_name, .keep_all =  T)

ents %>% 
  add_count(press_name) %>% 
  filter(n > 1)


# with dz artists
t <- all_final %>% 
  mutate(dz_name = normalize_string(dz_name)) %>% 
  left_join(ents, by = c(dz_name = "press_name")) %>% 
  select(is_in_press, dz_name, dz_stream_share, name_count)

nomatch <- t %>% 
  filter(is.na(is_in_press))



# with aliases
t2 <- aliases %>% 
  mutate(mbz_alias = str_to_lower(mbz_alias)) %>% 
  left_join(ents, by = c(mbz_alias = "press_name")) %>% 
  select(is_in_press, mbz_alias, type, name_count) %>% 
  distinct()

test2 <- t2 %>% 
  filter(is_in_press == TRUE)

test2 <- test2 %>% 
  filter(type == "alias") %>% 
  arrange(desc(name_count)) %>% 
  distinct()


xxxx <- test2 %>% 
  inner_join(all_final, by = c("mbz_alias" = "dz_name")) %>% 
  arrange(desc(dz_stream_share))

press_corpus %>% 
  filter(str_detect(article_text, ""))

ents %>% 
  filter(str_detect(press_name, "city"))







