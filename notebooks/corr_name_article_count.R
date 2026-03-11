## robustness check: compare name_count and article_count,
## through pearson's r and by looking at extreme cases
## even the most extreme cases never have a crazy ratio between the 2 metrics,
## so it is probably safe to take name_count only


press_name_counts <- all_final %>% 
  left_join(ents, by = c(dz_name = "entity")) %>% 
  mutate(corr_pop = abs(log(dz_stream_share / name_count))) %>% # MIX WITH ARTICLE_COUNT?
  arrange(desc(name_count))

cor.test(ents$name_count, ents$article_count)

ents <- ents %>% 
  mutate(corr_count = abs(log(name_count / article_count))) %>% 
  filter(name_count > 30 & article_count > 10)
arrange(desc(corr_count))