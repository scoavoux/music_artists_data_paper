make_lang <- function(dz_songs){
  dz_songs <- select(dz_songs, song_id, dz_artist_id)
  song_id_lang <- load_s3("records_w3/items/song_id_lang.csv")
  song_id_lang <- song_id_lang %>% 
    mutate(lang = str_remove_all(lang, '(\\"\\")|\\[|\\]')) %>% 
    separate_rows(lang, sep = ",") %>% 
    # remove missing and instrumental
    filter(lang != "", lang != "zxx", lang != "qlt-instrumental")
  artist_lang_from_tracks <- inner_join(dz_songs, song_id_lang) %>% 
    count(dz_artist_id, lang) %>% 
    arrange(desc(n)) %>% 
    group_by(dz_artist_id) %>% 
    mutate(total_tracks = sum(n)) %>% 
    slice(1) %>% 
    ungroup()
  
  artist_lang_from_tracks <- artist_lang_from_tracks %>% 
    rename(language_main = "lang",
           language_main_n_songs = "n",
           language_main_n_total_songs = "total_tracks")
  # A few artists are not documented here, we add them from the previous dataset
  artist_lang_from_deezer <- load_s3("interim/prod/artists_songs_languages.csv") %>%
    as_tibble() %>%
    mutate(dz_artist_id = as.character(art_id),
           language_main = lang,
           language_main_n_songs = nb_songs) %>%
    arrange(dz_artist_id, desc(language_main_n_songs)) %>%
    group_by(dz_artist_id) %>%
    slice(1) %>%
    ungroup() %>%
    select(dz_artist_id, language_main, language_main_n_songs)
  missing <- artist_lang_from_deezer %>% 
    anti_join(artist_lang_from_tracks, by = c(dz_artist_id = "dz_artist_id"))
  artist_lang_from_tracks <- bind_rows(artist_lang_from_tracks, missing)
  return(artist_lang_from_tracks)
  
}
