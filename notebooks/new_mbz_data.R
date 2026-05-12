library(sjmisc)



# ------- 1. load genres and inner_join to df

tar_load(df)

genres_raw <- load_s3("musicbrainz/musicbrainz_artist_releasegroup_genre.csv")

genres <- genres_raw %>% 
  as_tibble() %>% 
  rename(mbz_artist_id = "artist_mbid") %>% 
  filter(album_type == "Studio") %>% 
  filter(genre_name != "") %>% 
  select(-album_type)

dat <- df %>% 
  inner_join(genres, by = "mbz_artist_id") %>% 
  select(mbz_artist_id, dz_name, genre_name, n_plays_share, n_releases)

frq(dat$genre_name, sort.frq = "desc")



# ------- 2. set genres

# remove rare genres --> remove ~500 artists of 52k
dat <- dat %>% 
  group_by(genre_name) %>% 
  add_count(genre_name) %>% 
  filter(n > 200) %>% 
  ungroup()

mbz_genre_frq <- frq(dat$genre_name, sort.frq = "desc")[[1]]

mbz_genre_frq <- mbz_genre_frq %>% 
  select(-c(label, valid.prc, cum.prc))

write.csv2(mbz_genre_frq, "data/mbz_genre_frq.csv", sep = ";")


genre_recode <- read.csv("data/mbz_genre_frq-2.csv", sep = ";")
genre_recode <- genre_recode %>% 
  select(orig_genre, new_genre) %>% 
  as_tibble()

genre_recode


# find artists of a specific genre
t <- dat %>% 
  filter(genre_name == "progressive") %>% 
  select(dz_name) %>% 
  distinct(dz_name)


dat <- dat %>% 
  left_join(genre_recode, by = c(genre_name = "orig_genre")) %>%
  mutate(genre_name = new_genre) %>% 
  select(-c(new_genre, n))


dat_group <- dat %>% 
  group_by(mbz_artist_id, dz_name, genre_name, n_plays_share) %>% 
  summarise(n_releases = sum(n_releases)) %>% 
  ungroup() %>% 
  group_by(mbz_artist_id) %>% 
  filter(n_releases == max(n_releases)) %>% 
  ungroup() %>% 
  filter(!is.na(genre_name)) %>% 
  arrange(desc(n_plays_share))


dup <- dat_group %>% 
  add_count(mbz_artist_id) %>% 
  filter(n > 1)

nodup <- dat_group %>% 
  distinct(mbz_artist_id, .keep_all = T)


table(dup$n_releases)















  
  
  
  
  

