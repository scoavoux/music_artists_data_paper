
library(textcat)

textcat_options <- textcat::TC_char_profiles[c("english", "french")]

t <- albums %>% 
  filter(genre == "Pop") %>% 
  slice(1:10000) %>%  
  mutate(release_lang = textcat(album_title, p = textcat_options))


x <- t %>% 
  count(dz_artist_id, release_lang) %>% 
  group_by(dz_artist_id) %>% 
  slice_max(n, n = 1, with_ties = FALSE) %>% 
  ungroup()


new <- df %>% 
  inner_join(x, by = "dz_artist_id") %>% 
  select(dz_name, release_lang, n_plays_share, genre_1, genre_2)


t <- df %>% 
  mutate(recod_genre = dplyr::case_when(
    genre_1 == "Pop" & lang_main == "fr" ~ "Variété FR",
    genre_1 == "Rap/Hip Hop" & lang_main == "fr" ~ "Rap FR",
    TRUE ~ genre_1
  )) %>%
  filter(genre_1 != recod_genre) %>% 
  select(dz_name, genre_1, recod_genre, n_plays_share)

sum(t$n_plays_share)


rap_fr <- t %>% 
  filter(recod_genre == "Rap FR")

sum(rap_fr$n_plays_share)


rap_df <- df %>% 
  filter(genre_1 == "Rap/Hip Hop")


rap_df_not_lang <- rap_df %>% 
  filter(is.na(lang_main))

rap_df_not_lang <- rap_df %>% 
  anti_join(t, by = "dz_name")


write.csv2(t, "data/recoded_french_rap_and_pop.csv")



x <- rap_df_not_lang %>% 
  slice(1:300)

sum(x$n_plays_share)















