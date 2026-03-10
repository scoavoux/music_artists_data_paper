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


list_aliases <- function(){
  
  ## ------- 3. import corrected outliers
  press_v1_outliers_CHECKED <- read.csv("data/press_outliers_CHECKED.csv", sep = ";") 
  
  to_drop <- press_v1_outliers_CHECKED %>% 
    filter(drop == 1)
  
  wrong_alias <- press_v1_CHECKED %>% 
    filter(alias == 1) %>% 
    rename(press_alias = dz_name,
           dz_name = to_artist) %>% 
    select(press_alias, dz_name, name_count) %>% 
    as_tibble()
  
  # wrongly matched homonyms to integrate with name_count == 0
  wrong_homonyms <- wrong_alias$press_alias
  
  
  return(to_alias_dict)
}


list_ents_to_drop <- function(){
  
  # ------ 2. integrate hand-coded aliases
  aliases_no_match <- read.csv("data/ent_no_dzname_CHECKED.csv", sep = ";") 
  
  aliases_no_match <- aliases_no_match %>% 
    filter(alias == 1 & to_artist != "") %>% 
    left_join(all_final, by = c(to_artist = "dz_name")) %>%
    rename(dz_name = to_artist,
           press_alias = entity) %>% 
    select(press_alias, dz_name, name_count) %>% 
    as_tibble()
  
  
  return(to_drop_dict)
}


make_press_counts <- function(all_final, ent_file){
  
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
  ents <- ents %>% 
    as_tibble() %>% 
    mutate(is_in_press = TRUE,
           entity = str_normalize(name)) %>% 
    group_by(entity) %>% 
    mutate(keep_name = ifelse(name_count == max(name_count), TRUE, FALSE)) %>% 
    filter(keep_name) %>% 
    ungroup() %>% 
    filter(str_length(entity) > 2) %>% # remove short ents
    select(c(entity, name_count, name_id)) %>% #
    distinct()  # remove perfect duplicates
  
  # ------------- 1. match on name in dz_names
  press_counts <- all_final %>% 
    left_join(ents, by = c(dz_name = "entity")) %>% 
    arrange(desc(name_count))
  
  ## and export (biggest?) non-matched ents + biggest outliers
  
  # outliers
  press_counts_outliers <- press_counts %>% 
    filter(name_count > 30) %>% 
    arrange(desc(corr_pop)) %>% 
    select(dz_name, name_count, dz_artist_id, 
           corr_pop, dz_stream_share)
  
  write_s3(press_counts_outliers)
  
  return(press_counts)
}
  
  
  
correct_press_counts <- function(press_counts, to_alias, to_drop){
  
  
  
  # ------------- 2. add aliases from alias dict
  
  
  
  # ------------- 3. drop errors
  names_to_drop <- c("paul", "camille", "anna", "juliette",
                     "jacques", "raphael", "antoine", 
                     "simon", "claude", "corneille",
                     "keith", "bob", 
                     
                     "reich", "morrison") # ADD!
  
  names_to_drop <- c(names_to_drop, wrong_homonyms)
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}