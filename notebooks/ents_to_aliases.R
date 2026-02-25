
tar_load(aliases)
tar_load(all_final)

all_final <- all_final %>% 
  filter(!is.na(sc_artist_id) & !is.na(mbz_artist_id)) %>% 
  mutate(dz_name = str_to_lower(dz_name))
  

ents <- read.csv("data/extracted_ents_ALL.csv", sep = ";")

ents <- ents %>% 
  as_tibble() %>% 
  mutate(is_in_press = TRUE,
         press_name = str_to_lower(name)) %>% 
  select(c(press_name, name_count, is_in_press)) %>% 
  distinct(press_name, .keep_all =  T)


# with dz artists
t <- all_final %>% 
  mutate(dz_name = str_to_lower(dz_name)) %>% 
  left_join(ents, by = c(dz_name = "press_name")) %>% 
  select(is_in_press, dz_name, dz_stream_share, name_count)

test <- t %>% 
  filter(is_in_press == TRUE)


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


mutate(entity = name...2,
       clean_ent = str_replace(entity, "-\\s.*", "")) %>% 
filter(!str_detect(clean_ent, "CD "))


















