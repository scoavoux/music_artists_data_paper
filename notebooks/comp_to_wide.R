
helene <- read.csv("data/base_compositeurs_10.06.csv",
                   sep = ";") %>% 
  as_tibble() %>% 
  mutate(dz_artist_id = as.character(dz_artist_id),
         nom_helene = dz_name) %>% 
  select(-surname_only, -ortho, -dz_name)

helene <- helene %>% 
  inner_join(dz_names, by = "dz_artist_id")

artist_names <- helene %>%
  group_by(dz_artist_id, dz_name) %>%
  summarise(
    aliases = paste(
      unique(na.omit(nom_helene)),
      collapse = " | "
    ),
    .groups = "drop"
  )

helene <- helene %>%
  #select(-dz_name, -surname_only) %>%
  filter(period_szk != "" | period_s != "")

t <- artist_names %>%
  left_join(helene, by = "dz_artist_id") %>%
  distinct() %>% 
  mutate(last_name_only = FALSE) %>% 
  select(dz_artist_id, 
         dz_name = "dz_name.x", 
         aliases, 
         last_name_only, 
         period_szk, 
         period_s)

readr::write_excel_csv2(
  t,
  "data/comp_wide_1706.csv"
)










