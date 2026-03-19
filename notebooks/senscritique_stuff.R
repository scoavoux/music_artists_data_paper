
ratings <- load_s3("/senscritique/ratings.csv")
contacts_albums_list <- load_s3("senscritique/contacts_albums_link.csv")

tracks <- load_s3("senscritique/tracks.csv")
contacts_tracks_list <- load_s3("senscritique/contact_tracks_link.csv")



track_weight = .2


# clean contact-album
contacts_albums_list <- contacts_albums_list %>% 
  filter(contact_subtype_id %in% c(11, 13)) %>% 
  select(-contact_subtype_id)

# join contact-album to ratings and make mean rating per album
albums_ratings <- ratings %>%
  group_by(product_id) %>% 
  summarize(n = n(),
            mean = mean(rating)) %>% 
  filter(n > 3) %>% # OK let us consider that 4 grades is enough
  inner_join(contacts_albums_list, by = "product_id") %>% 
  mutate(
    weight = 1,
    type = "album"
    )

tracks_ratings <- tracks %>% 
  filter(rating_count > 3) %>% 
  mutate(mean = rating_average / 10, weight = track_weight) %>% 
  select(product_id, 
         mean, 
         n = "rating_count", 
         weight) %>% 
  inner_join(contacts_tracks_list, by = "product_id") %>% 
  mutate(type = "track")


cora_summary <- bind_rows(tracks_ratings, albums_ratings) %>%
  
  group_by(contact_id) %>%
  
  summarise(
    
    # mean maven score of artist's albums + tracks
    sc_avg_score = mean(mean, na.rm = TRUE),
    
    # same, normalized by total n_ratings of artist
    sc_avg_score_wtd = sum(mean * n, na.rm = TRUE) / sum(n, na.rm = TRUE),
    
    # mean maven score of artist's albums
    sc_avg_score_albums = mean(mean[type == "album"], na.rm = TRUE),
    
    # same, normalized by total n_ratings of artist
    sc_avg_score_albums_wtd = sum(mean[type == "album"] * n[type == "album"], na.rm = TRUE) /
      sum(n[type == "album"], na.rm = TRUE)
  )





















