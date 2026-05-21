library(sjmisc)
library(janitor)
library(stringr)
library(ggplot2)

# ------- 1. preprocess mbz genres

## choose one genre per release
mbz_genre_frq <- frq(dat$genre_name, sort.frq = "desc")[[1]]
mbz_genre_frq <- mbz_genre_frq %>% 
  select(genre_name = "val", 
         genre_frq = frq)

dat <- dat %>% 
  left_join(mbz_genre_frq, by = "genre_name")


dat <- dat %>% 
  group_by(release_group_mbid) %>% 
  filter(genre_frq == max(genre_frq)) %>% 
  ungroup()


frq(dat$genre_name, sort.frq = "desc")[[1]]


# ------- 2. recode and aggregate genres

### filter irrelevant genres
dat <- dat %>% 
  group_by(genre_name) %>% 
  add_count(genre_name) %>% 
  filter(n > 200) %>% 
  ungroup() %>% 
  select(-n)

### export mbz genres table
mbz_genre_frq <- frq(dat$genre_name, sort.frq = "desc")[[1]]
mbz_genre_frq <- mbz_genre_frq %>% 
  select(-c(label, valid.prc, cum.prc))
write.csv2(mbz_genre_frq, "data/mbz_genre_frq.csv", sep = ";")

### import recoded genres
genre_recode <- read.csv("data/mbz_genre_frq-2.csv", sep = ";")
genre_recode <- genre_recode %>% 
  select(orig_genre, new_genre) %>% 
  as_tibble()

### apply recodes from csv
dat <- dat %>% 
  left_join(genre_recode, by = c(genre_name = "orig_genre")) %>%
  mutate(genre_name = new_genre) %>% 
  select(-new_genre)

### aggregate genres into first and second
dat_group <- dat %>%
  group_by(mbz_artist_id, dz_name, genre_name, n_plays_share) %>%
  summarise(
    n_releases = sum(n_releases),
    .groups = "drop"
  ) %>%
  filter(!is.na(genre_name)) %>%
  group_by(mbz_artist_id, dz_name, n_plays_share) %>%
  arrange(desc(n_releases), .by_group = TRUE) %>%
  slice_head(n = 2) %>%   # keep top 2 genres only
  mutate(rank = row_number()) %>%
  select(-n_releases) %>%
  tidyr::pivot_wider(
    names_from = rank,
    values_from = genre_name,
    names_prefix = "mbz_genre_"
  ) %>%
  ungroup() %>% 
  arrange(desc(n_plays_share))

## join 
dz_genre <- df %>% 
  filter(!is.na(mbz_artist_id), !is.na(genre_1)) %>% 
  mutate(
    genre_1 = recode(
      genre_1,
      "Alternative" = "Rock", # recode some deezer genres
      "Dance" = "Pop"
    )) %>% 
  select(mbz_artist_id, genre_1)

check <- dat_group %>% 
  inner_join(dz_genre, by = "mbz_artist_id") %>% 
  mutate(genre_1 = str_to_lower(genre_1)) %>% 
  select(-mbz_genre_2)


# confusion matrix
janitor::tabyl(check, genre_1, mbz_genre_1) %>% 
  janitor::adorn_percentages("row") %>%
  janitor::adorn_pct_formatting()
  


# recode genres
# harmonize MBZ genres into the old taxonomy
check <- check %>%
  mutate(
    mbz_genre_1 = case_when(
      mbz_genre_1 == "classical"        ~ "classique",
      mbz_genre_1 == "electronic"       ~ "electro",
      mbz_genre_1 == "hip hop"          ~ "rap/hip hop",
      mbz_genre_1 == "latin"            ~ "latino",
      mbz_genre_1 == "soul"             ~ "soul & funk",

      
      # perfect mappings
      mbz_genre_1 == "folk"             ~ "folk",
      mbz_genre_1 == "rock"             ~ "rock",
      mbz_genre_1 == "pop"              ~ "pop",
      mbz_genre_1 == "metal"            ~ "metal",
      mbz_genre_1 == "jazz"             ~ "jazz",
      mbz_genre_1 == "blues"            ~ "blues",
      mbz_genre_1 == "country"          ~ "country",
      mbz_genre_1 == "reggae"           ~ "reggae",
      mbz_genre_1 == "r&b"              ~ "r&b",
      
      # debatable mappings
      #mbz_genre_1 == "experimental"     ~ "alternative",
      
      TRUE ~ mbz_genre_1
    )
  )

# common ordering for both axes
genre_order <- c(
  "blues",
  "chanson française",
  "classique",
  "country",
  "electro",
  # "dance",
  "folk",
  "jazz",
  "latino",
  "metal",
  "pop",
  "r&b",
  "rap/hip hop",
  "reggae",
  "rock",
  "soul & funk"
)

# heatmap
check %>%
  mutate(
    genre_1 = factor(genre_1, levels = genre_order),
    mbz_genre_1 = factor(mbz_genre_1, levels = genre_order)
  ) %>%
  count(genre_1, mbz_genre_1) %>%
  group_by(genre_1) %>%
  mutate(pct = n / sum(n)) %>%
  ungroup() %>%
  ggplot(aes(x = genre_1, y = mbz_genre_1, fill = pct)) +
  geom_tile(color = "white") +
  geom_text(
    aes(label = scales::percent(pct, accuracy = 1)),
    size = 3
  ) +
  scale_fill_viridis_c(
    limits = c(0, 1),
    labels = scales::percent
  ) +
  coord_equal() +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid = element_blank()
  ) +
  labs(
    x = "deezer genre",
    y = "mbz genre",
    fill = "% within deezer genre"
  )

  

