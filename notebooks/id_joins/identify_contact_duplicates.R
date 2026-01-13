# some contact_names cannot be safely linked to a deezer name because there are duplicates
# among those duplicates, check nr of reviews etc to see if some of them are way more popular
# same logic as identify_deezer_duplicates.R!

contacts <- load_s3("senscritique/contacts.csv")

contacts <- as_tibble(contacts) %>% 
  select(-c(contact_name_url, subtype_id, spotify_id,
            mbz_id, freebase_id))

# for each name in contacts, compute popularity of occurrences
co_pop_share <- contacts %>% 
  group_by(contact_name) %>% # maybe: name, deezer_id?
  mutate(col_share = collection_count / sum(collection_count),
         prod_share = product_count / sum(product_count),
         gen_like_share = gen_like_count / sum(gen_like_count),
         gen_pos_share = gen_like_positive_count / sum(gen_like_positive_count))

test <- co_pop_share %>% 
  filter(col_share > 0.9 | prod_share > 0.9)


### note: high correlation between metrics
cor.test(co_pop_share$col_share,
         co_pop_share$gen_pos_share)

# ------- join to missing contact_data in all

co_unique <- co_pop_share %>% 
  filter(col_share > 0.9) %>% 
  select(contact_name, contact_id)

co_unique

t <- miss_co %>% 
  inner_join(co_unique, by = c(name = "contact_name")) %>% 
  mutate(contact_id = contact_id.y) %>% 
  select(-c(contact_id.x, contact_id.y))

sum(t$pop)

View(t)


mean(co_pop_share$product_count)










