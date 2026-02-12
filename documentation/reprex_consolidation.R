




streams <- tibble(song_id,
                  n_play)


items_old <- tibble(song_id,
                    song_title = c("Africa", "Rosanna", "Hold the Line",
                                   "Under Pressure", "Killer Queen", "Radio Ga Ga",
                                   "Heroes", "Starman", "Space Oddity"),
                    deezer_id = c(""),
                    deezer_feat_id)

items_new <- tibble(song_id,
                    song_title,
                    deezer_id,
                    deezer_feat_id)


items <- tibble()

artists

contacts

mbz_deezer

all


all_enriched






c("Africa", "Rosanna", "Hold the Line",
  "Under Pressure", "Killer Queen", "Radio Ga Ga",
  "Heroes", "Starman", "Space Oddity")



## TEST PIPELINE A-Z

bind_items <- function(items_old, items_new){
  
  # bind items_old and items_new
  # prioritize deezer_id of items_new
  items <- items_new %>% 
    bind_rows(
      items_old %>% 
        anti_join(items_new, by = "song_id")
    )
}

items_old <- tibble()

















