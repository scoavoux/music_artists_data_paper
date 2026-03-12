

# LOAD THE 2 CHECKED DATASETS AND APPEND DECOMPOSED COUNTS
# TO BE SAFE JOIN TO press_named_entities by name?


# aliases_to_add
## ADD ALIASES + TO_DROP

press_name_counts

aliases_to_add

library(dplyr)

totals <- bind_rows(
  press_name_counts %>%
    select(dz_name,
           name_count,
           name_count_lefigaro,
           name_count_lemonde,
           name_count_liberation,
           name_count_telerama),
  
  aliases_to_add %>%
    select(dz_name,
           name_count,
           name_count_lefigaro,
           name_count_lemonde,
           name_count_liberation,
           name_count_telerama)
) %>%
  group_by(dz_name) %>%
  summarize(
    name_count = sum(name_count, na.rm = TRUE),
    name_count_lefigaro = sum(name_count_lefigaro, na.rm = TRUE),
    name_count_lemonde = sum(name_count_lemonde, na.rm = TRUE),
    name_count_liberation = sum(name_count_liberation, na.rm = TRUE),
    name_count_telerama = sum(name_count_telerama, na.rm = TRUE),
    .groups = "drop"
  )

t <- bind_rows(press_name_counts, aliases_to_add) %>%
  select(-starts_with("name_count")) %>%
  left_join(totals, by = "dz_name")
















