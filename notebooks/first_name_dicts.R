# # remove common US and french first names (by dictionary)
# first_names <- first_name_dict(fr_names = "data/firstnames.csv",
#                                n_us_names=500, n_fr_names=2000)
# 
# aliases <- aliases %>% 
#   mutate(name = str_to_lower(mbz_alias)) %>% 
#   anti_join(first_names, by = "name")
# 
# 