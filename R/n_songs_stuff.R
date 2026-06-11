
tracks_vars <- function(dz_songs){
  
  tracks_vars <- dz_songs %>%
  
  # identify songs with multiple artists
  group_by(song_id) %>%
  mutate(
    is_feat_track = n_distinct(dz_artist_id) > 1
  ) %>%
  ungroup() %>%
  
  group_by(dz_artist_id) %>%
    
  summarise(
    # unweighted
    n_tracks = n_distinct(song_id),
    n_feat_tracks = n_distinct(song_id[is_feat_track]),
    feat_share = n_feat_tracks / n_tracks,
    
    # weighted
    n_tracks_w = sum(w_feat),
    n_feat_tracks_w = sum(w_feat[is_feat_track]),
    feat_share_w = n_feat_tracks_w / n_tracks_w,
    
    .groups = "drop"
  )
  
  return(tracks_vars)
  
}





