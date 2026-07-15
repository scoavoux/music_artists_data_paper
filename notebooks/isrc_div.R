

df_song_title <- read.csv2("data/df_div_song_title.csv") %>% 
  as_tibble() %>% 
  select(dz_artist_id,
         title_top1_share = "div_top1_share",
         title_top5_share = "div_top5_share",
         title_top10_share = "div_top10_share",
         title_h_index = "div_h_index",
         title_shannon_effective = "div_shannon_effective",
         title_songs_for_80 = "div_songs_for_80") %>% 
  arrange(desc(title_shannon_effective))

mean(df_song_title$title_top5_share)
mean(df_isrc$isrc_top5_share)



tar_load(df_complete)
df_isrc <- df_complete %>% 
  select(dz_artist_id,
         dz_name,
         isrc_top1_share = "div_top1_share",
         isrc_top5_share = "div_top5_share",
         isrc_top10_share = "div_top10_share",
         isrc_h_index = "div_h_index",
         isrc_shannon_effective = "div_shannon_effective",
         isrc_songs_for_80 = "div_songs_for_80") %>% 
  arrange(desc(isrc_shannon_effective))


df_all <- df_song_title %>% 
  mutate(dz_artist_id = as.character(dz_artist_id)) %>% 
  left_join(df_isrc, by = "dz_artist_id") %>% 
  as_tibble()

df_all <- df_all %>% 
  mutate(diff_1 = title_top1_share - isrc_top1_share,
         diff_2 = title_top10_share - isrc_top10_share,
         diff_3 = title_h_index - isrc_h_index,
         diff_4 = title_shannon_effective - isrc_shannon_effective)

df <- df_all %>% 
  slice_head(n = 1000) %>% 
  mutate(diff_shannon = title_shannon_effective - isrc_shannon_effective) %>% 
  arrange(desc(abs(diff_shannon))) %>% 
  select(dz_artist_id, 
         dz_name, 
         title_top10_share,
         isrc_top10_share,
         isrc_shannon_effective,
         diff_shannon)

cor(df_all %>% 
  select(
         ends_with("_share")))


mean(df_all$title_shannon_effective)

















