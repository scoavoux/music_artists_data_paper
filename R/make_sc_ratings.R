

# load senscritique ratings for albums and tracks and compute
# 4 metrics:

make_sc_ratings <- function(sc_albums_ratings_file, 
                            sc_albums_list_file,
                            sc_tracks_ratings_file, 
                            sc_tracks_list_file,
                            track_weight) {
  
  sc_albums_ratings <- load_s3(sc_albums_ratings_file)
  sc_albums_list <- load_s3(sc_albums_list_file)
  
  sc_tracks_ratings <- load_s3(sc_tracks_ratings_file)
  sc_tracks_list <- load_s3(sc_tracks_list_file)
  
  
  
  # ------- PREPARE VARIABLES
  
  # clean contact-album
  sc_albums_list <- sc_albums_list %>% 
    filter(contact_subtype_id %in% c(11, 13)) %>% 
    select(-contact_subtype_id)
  
  # join contact-album to ratings and make mean rating per album
  sc_albums_ratings <- sc_albums_ratings %>%
    group_by(product_id) %>% 
    summarize(n_ratings = n(),
              mean_rating = mean(rating)) %>% 
    filter(n_ratings > 3) %>% 
    inner_join(sc_albums_list, by = "product_id") %>% 
    mutate(weight = 1, type = "album")
  
  sc_tracks_ratings <- sc_tracks_ratings %>% 
    filter(rating_count > 3) %>% 
    mutate(mean_rating = rating_average / 10,
           weight = track_weight) %>% 
    inner_join(sc_tracks_list, by = "product_id") %>% 
    mutate(type = "track") %>% 
    select(product_id, 
           mean_rating, 
           n_ratings = "rating_count",
           weight)
  
  

  # ----------- COMPUTE VARIABLES

  sc_rating_metrics <- bind_rows(sc_tracks_ratings, sc_albums_ratings) %>%
    
    mutate(sc_artist_id = as.character(contact_id)) %>% 
    
    group_by(sc_artist_id) %>%
    
    summarise(
      
      # unweighted mean (unchanged)
      sc_avg_score = mean(mean_rating, na.rm = TRUE),
      
      # weighted mean: ratings * weight
      sc_avg_score_wtd = sum(mean_rating * n_ratings * weight, na.rm = TRUE) / 
        sum(n_ratings * weight, na.rm = TRUE),
      
      # albums only (unweighted)
      sc_avg_score_albums = mean(mean_rating[type == "album"], na.rm = TRUE),
      
      # albums only (weighted by ratings)
      sc_avg_score_albums_wtd = sum(mean_rating[type == "album"] * n_ratings[type == "album"], na.rm = TRUE) /
        sum(n_ratings[type == "album"], na.rm = TRUE)
      )

  return(sc_rating_metrics)
}
  











