library(tidyr)




tar_load(df)


t <- df %>% 
  left_join(artist_main_genres, by = "dz_artist_id") %>% 
  select(dz_name, genre_1, genre_2, n_plays_share)


# ------- W/ RECORD TYPE WEIGHTS
# ------- TO REDUCE NAS ON GENRE_2
# 
# artist_main_genres <- albums %>%
#   mutate(
#     weight = log(fans + 1)
#   ) %>%
#   
#   # 1. Detect availability of release types
#   group_by(dz_artist_id) %>%
#   mutate(
#     has_album = any(record_type == "album"),
#     has_ep    = any(record_type == "ep")
#   ) %>%
#   ungroup() %>%
#   
#   # 2. Apply tier-based weighting
#   mutate(
#     type_multiplier = case_when(
#       has_album & record_type == "album"  ~ 1,
#       has_album & record_type == "ep"     ~ 0.5,
#       has_album & record_type == "single" ~ 0.25,
#       
#       !has_album & has_ep & record_type == "ep"     ~ 1,
#       !has_album & has_ep & record_type == "single" ~ 0.5,
#       
#       TRUE ~ 1  # only singles case
#     ),
#     weighted_score = weight * type_multiplier
#   ) %>%
#   
#   # 3. Aggregate per genre
#   group_by(dz_artist_id, genre) %>%
#   summarise(
#     genre_weight = sum(weighted_score, na.rm = TRUE),
#     .groups = "drop"
#   ) %>%
#   
#   # 4. Extract top 2 genres
#   group_by(dz_artist_id) %>%
#   arrange(desc(genre_weight), genre, .by_group = TRUE) %>%
#   summarise(
#     genre_1 = first(genre),
#     genre_2 = dplyr::nth(genre, 2),
#     .groups = "drop"
#   )














