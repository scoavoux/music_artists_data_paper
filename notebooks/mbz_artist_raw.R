
tar_load(mbz_genre_artist)

genre <- mbz_genre_artist

frq(genre$mbz_genre, sort.frq = "desc")

# rn 42k artists with 73% of streams
nrow(genre)
sum(genre$n_plays_share)


## what happens if we reduce to > 10 artists per genre??
genre_10 <- genre %>% 
  group_by(mbz_genre) %>% 
  add_count(mbz_genre) %>% 
  filter(n > 10) %>% 
  ungroup() %>% 
  select(-n)

# we lose 2K artists and 1.5%
nrow(genre_10)
sum(genre_10$n_plays_share)

length(unique(genre$mbz_genre))

# but among which many famous artists, e.g. CCR "swamp rock" or woodkid "chamber pop"
marg_genres <- genre %>% 
  group_by(mbz_genre) %>% 
  add_count(mbz_genre) %>% 
  filter(n < 10) %>% 
  ungroup() 

## ----> better to not filter genres at all















