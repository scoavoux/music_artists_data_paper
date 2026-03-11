# DISCLAIMER: this workflow is **temporary**.
# separate targets are made for aliases to add and names to drop because
# we might update these later, and until then it is best to not integrate them. 
# importantly, both aliases and names to drop lists are made looking at 

# 1. the export of the entities WITHOUT a match that have 
# the highest mentions (currently n > 30),

# 2. the export of the highest outliers (currently n > 30) 
# among entities WITH a match. outliers <=> abs(log(dz_stream_share / name_count)

# for FUTURE WORK on this: just take exports built into "press_counts"
# and continue hand-coding them


list_aliases <- function(file1, file2, all_final){
  
  # prepare all_final
  all_final <- all_final %>% 
    filter(!is.na(sc_artist_id) & !is.na(mbz_artist_id)) %>% # complete cases
    mutate(dz_name = str_normalize(dz_name)) %>% # normalize name
    group_by(dz_name) %>% 
    mutate(keep = ifelse(dz_stream_share == max(dz_stream_share), 
                         TRUE, 
                         FALSE)) %>% # deduplicate CHANGE: SUM OF NAME_COUNT INSTEAD!!
    filter(keep == TRUE) %>% 
    ungroup() %>% 
    select(dz_name, dz_stream_share, dz_artist_id)
  
  ents_without_match <- load_s3(file1) 
  press_outliers_checked <- load_s3(file2) 
  
  added_aliases <- ents_without_match %>% 
    filter(alias == 1 & to_artist != "") %>% 
    left_join(all_final, by = c(to_artist = "dz_name")) %>%
    rename(alias_to_recode = entity,
           dz_name = to_artist) %>% 
    select(alias_to_recode, dz_name, name_count,
           name_count_lefigaro,
           name_count_lemonde,
           name_count_liberation,
           name_count_telerama) %>% 
    as_tibble()
  
  recoded_aliases <- press_outliers_checked %>% 
    filter(alias == 1) %>% 
    rename(alias_to_recode = dz_name,
           dz_name = to_artist) %>% 
    as_tibble() %>% 
    select(alias_to_recode, dz_name, name_count,
           name_count_lefigaro,
           name_count_lemonde,
           name_count_liberation,
           name_count_telerama)
  
  aliases <- rbind(added_aliases, recoded_aliases)
  
  return(aliases)
}


# make a df of entities which get NA either because 
# they are non-musical artists or homonyms of famous artists
list_entities_to_drop <- function(file){
  
  press_outliers_checked <- load_s3(file)
  
  # non-artists or ambiguous names
  wrong_name <- press_outliers_checked %>% 
    filter(drop == 1)
  wrong_name <- wrong_name$dz_name
  
  # homonyms of famous artists (e.g., "beethoven")
  wrong_alias <- press_outliers_checked %>% 
    filter(alias == 1)
  wrong_alias <- wrong_alias$dz_name
  
  names_to_drop <- c(wrong_name, wrong_alias)
  
  # assign NA on both metrics to update
  names_to_drop <- tibble(dz_name = names_to_drop,
                          name_count = NA,
                          article_count = NA)
  return(names_to_drop)
}


count_names_press <- function(all_final, press_named_entities, name_count_threshold){
  
  # prepare all_final
  all_final <- all_final %>% 
    filter(!is.na(sc_artist_id) & !is.na(mbz_artist_id)) %>% # complete cases
    mutate(dz_name = str_normalize(dz_name)) %>% # normalize name
    group_by(dz_name) %>% 
    mutate(keep = ifelse(dz_stream_share == max(dz_stream_share), 
                         TRUE, 
                         FALSE)) %>% # deduplicate
    filter(keep == TRUE) %>% 
    ungroup() %>% 
    select(dz_name, dz_stream_share, dz_artist_id)
  
  # prepare ents
  ents <- press_named_entities %>% 
    as_tibble() %>% 
    mutate(ent_name = str_normalize(name)) %>% 
    group_by(ent_name) %>% 
    mutate(keep_name = ifelse(name_count == max(name_count), 
                              TRUE, 
                              FALSE)) %>% # deduplicate
    filter(keep_name) %>% 
    ungroup() %>% 
    distinct(ent_name, .keep_all = T) %>%  # remaining duplicates with equal name_count --> CHANGE LATER
    filter(str_length(ent_name) > 2) %>% # remove short ents
    select(-c(V1, name))
  
  # ------------- 1. match on name in dz_names
  press_name_counts <- all_final %>% 
    left_join(ents, by = c(dz_name = "ent_name")) %>% 
    mutate(corr_pop = abs(log(dz_stream_share / name_count))) %>% # MIX WITH ARTICLE_COUNT?
    arrange(desc(name_count))
  
  # ------------- 2.  export (biggest?) non-matched ents + biggest outliers
  # 1. non-matched entities
  ents_without_match <- ents %>% 
    anti_join(press_name_counts, by = c(ent_name = "dz_name")) %>% 
    filter(name_count >= name_count_threshold)
  
  write_s3(ents_without_match, file = "interim/press_files/ents_without_match_1103.csv")
  
  # 2. outliers
  press_counts_outliers <- press_name_counts %>% 
    filter(name_count >= name_count_threshold) %>% 
    arrange(desc(corr_pop))
  
  write_s3(press_counts_outliers, file = "interim/press_files/press_counts_outliers_1103.csv")
  
  press_name_counts <- press_name_counts %>% 
    select(-c(article_id, keep_name, corr_pop))
  
  return(press_name_counts)
}


# aliases_to_add
# ## ADD ALIASES + TO_DROP
# press_name_counts <- press_name_counts %>%
#   left_join(
#     aliases_to_add %>% select(alias_to_recode, canonical = dz_name),
#     by = c("dz_name" = "alias_to_recode")
#   ) %>%
#   mutate(dz_name = coalesce(canonical, dz_name)) %>%
#   group_by(dz_name) %>%
#   summarise(across(name_count, sum), .groups = "drop")
# 
# press_name_counts_updated <- press_name_counts_updated %>%
#   mutate(
#     name_count = if_else(dz_name %in% names_to_drop$dz_name,
#                          NA_integer_,
#                          name_count),
#     article_count = if_else(dz_name %in% names_to_drop$dz_name,
#                             NA_integer_,
#                             article_count)
#   )



# LOAD THE 2 CHECKED DATASETS AND APPEND DECOMPOSED COUNTS

decomp <- press_named_entities %>%
  mutate(ent_name = str_normalize(name)) %>%
  group_by(ent_name) %>%
  mutate(keep_name = ifelse(name_count == max(name_count),
                            TRUE,
                            FALSE)) %>% # deduplicate
  filter(keep_name) %>%
  ungroup() %>%
  distinct(ent_name, .keep_all = T) %>% 
  select(c(ent_name, name_count, name_count_lefigaro, name_count_lemonde,
           name_count_liberation, name_count_telerama))

t <- decomp %>%
  add_count(ent_name) %>%
  filter(n > 1)

ents_without_match_checked_1003 <- load_s3("interim/press_files/ents_without_match_checked_1003.csv")
press_outliers_checked_1003 <- load_s3("interim/press_files/press_outliers_checked_1003.csv")

nrow(ents_without_match_checked_1003)


ents_without_match_checked_new <- ents_without_match_checked_1003 %>%
  left_join(decomp, by = c(entity = "ent_name")) %>%
  as_tibble()

ents_without_match_checked_1003
ents_without_match_checked_new



# prepare ents
ents <- press_named_entities %>% 
  as_tibble() %>% 
  mutate(ent_name = str_normalize(name)) %>% 
  group_by(ent_name) %>% 
 # mutate(keep_name = ifelse(name_count == max(name_count), 
                           # TRUE, 
                           # FALSE)) %>% # deduplicate
 # filter(keep_name) %>% 
  ungroup() %>% 
  #distinct(ent_name, .keep_all = T) %>%  # remaining duplicates with equal name_count --> CHANGE LATER
  filter(str_length(ent_name) > 2) %>% # remove short ents
  select(-c(V1, name))

dups <- ents %>% 
  add_count(ent_name) %>% 
  filter(n > 1)

dups %>% 
  filter(ent_name == "figaro") %>% 
  select(c(name_count, ent_name))

t <- dups %>% 
  left_join(press_named_entities %>% select(c(name_id,name)), by = "name_id") %>% 
  select(c(name, ent_name, name_count)) %>% 
  arrange(desc(ent_name))

t %>% 
  filter(ent_name == "liberation") %>% 
  select(c(name, ent_name))





















