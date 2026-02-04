
## export biggest missings to csv for handcoding

tar_load(all_enriched)

library(stringr)

missing <- all_enriched %>%
  filter(is.na(contact_id) | is.na(musicbrainz_id)) %>%
  slice(1:1000)

missing_mbz <- all_enriched %>%
  filter(is.na(musicbrainz_id)) %>%
  slice(1:1000)

missing_contacts <- all_enriched %>%
  filter(is.na(contact_id)) %>%
  slice(1:1000)

cleanpop(missing)
cleanpop(missing_mbz)
cleanpop(missing_contacts)

write_s3(missing, "interim/missings_to_handcode/missing_either.csv")
write_s3(missing_mbz, "interim/missings_to_handcode/missing_mbz.csv")
write_s3(missing_contacts, "interim/missings_to_handcode/missing_contacts.csv")


df <- load_s3("interim/missings_to_handcode/missing_either.csv")

df <- load_s3("interim/missings_to_handcode/missing_either.csv")


