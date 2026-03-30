library(targets)
library(tarchetypes)

# Preparation ------
tar_option_set(
  packages = c("paws", "tidyverse", "arrow"),
  repository = "aws", 
  repository_meta = "aws",
  resources = tar_resources(
    aws = tar_resources_aws(
      endpoint = Sys.getenv("S3_ENDPOINT"),
      bucket = "scoavoux",
      prefix = "music_artist"
    )
  )
)

tar_source("R")



# List of targets ------
list(
  
    ### CREATE (deezer) ARTISTS ----------------------------------------------

    # maybe put in clean_raw
    tar_target(dz_users,
               command = load_s3("records_w3/RECORDS_hashed_user_group.parquet") %>% 
                 mutate(
                   is_respondent = ifelse(is_respondent == TRUE, 1, 0),
                   is_control = ifelse(is_in_control_group == TRUE, 1, 0)) %>% 
                 filter(
                   is_respondent != 0 | is_control != 0
                   ) %>% # remove users who are neither control nor respondent
                 select(hashed_id, 
                        is_respondent, 
                        is_control)),
    
    # make popularity split by control and respondents
    tar_target(name = dz_stream_data,
               command = make_stream_popularity(dz_songs, dz_users)),
    
    # make popularity at user-artist level for respondents
    tar_target(name = respondent_streams,
               command = make_respondent_plays(dz_songs, dz_users)),
    
    tar_target(name = to_remove_file,
               command = read.csv("data/artists_to_remove.csv")),
    
    
    # bind old and new songs and names, join to streams
    tar_target(name = dz_names,
               command = bind_dz_names(file_1 = "records_w3/items/artists_data.snappy.parquet",
                                       file_2 = "interim/new_artists_names_from_api.csv")),
    
    tar_target(name = dz_songs_old,
               command = make_dz_songs(to_remove = to_remove_file,
                                    file = "records_w3/items/songs.snappy.parquet")),
    
    tar_target(name = dz_songs_new,
               command = make_dz_songs(to_remove = to_remove_file,
                                    file = "records_w3/items/song.snappy.parquet")),
    
    tar_target(name = dz_songs,
               command = bind_dz_songs(dz_songs_old, dz_songs_new, dz_names)),
    
    
    # group songs by featured artists and compute weighted popularity
    tar_target(name = dz_artists,
               command = group_songs_by_artist(dz_songs, dz_stream_data)),
    
    
    # -------- load and process raw ID data
    
    # sc_artist_id (+ metadata) to mbz_artist_id
    tar_target(name = senscritique, 
               command = load_senscritique(sc_file="senscritique/contacts.csv")),
    
    # manual searches sc_artist_id to dz_artist_id
    tar_target(name = manual_search,
               command = load_manual_search(file="data/manual_search.csv")),

    # mbz_artist_id to dz_artist_id
    tar_target(name = mbz_deezer,
               command = load_mbz_deezer(file="musicbrainz/musicbrainz_urls.csv")),
    
    # wikidata artist names
    tar_target(name = wiki_labels,
               command = load_s3("interim/wiki_labels.csv")),

    # wikidata itemId to mbz_artist_id and dz_artist_id
    tar_target(name = wiki,
               command = load_wiki(mbz_deezer)),
    
    
    ### CONSOLIDATE ARTISTS ----------------------------------------
    
    ##### left-join raw data
    tar_target(name = all,
               command = join_artist_ids(dz_artists, 
                                         mbz_deezer,
                                         senscritique, 
                                         manual_search, 
                                         wiki)),
    
    # unique matches between deezer and senscritique names
    tar_target(name = sc_names_patch,
               command = patch_names(all = all,
                                     ref = senscritique,
                                     ref_id = "sc_artist_id",
                                     ref_name = "sc_name",
                                     all_name = "dz_name")),
    
    # unique matches between deezer and mbz names
    tar_target(name = mbz_names_patch,
               command = patch_names(all = all,
                                     ref = mbz_deezer,
                                     ref_id = "mbz_artist_id",
                                     ref_name = "mbz_name",
                                     all_name = "dz_name")),
    
    # unique matches between deezer and mbz names from wiki
    tar_target(name = wiki_mbz_names_patch,
               command = patch_names(all = all,
                                     ref = wiki,
                                     ref_id = "mbz_artist_id",
                                     ref_name = "mbz_name",
                                     all_name = "dz_name")),
    
    # unique matches between deezer and wiki names
    tar_target(name = wiki_names_patch,
               command = patch_names(all = all,
                                     ref = wiki,
                                     ref_id = "mbz_artist_id",
                                     ref_name = "wiki_name",
                                     all_name = "dz_name")),
    
    # mbz ids retrieved from wiki
    tar_target(name = wiki_mbz_ids_patch,
               command = mbz_from_wiki(all, wiki)),
    
    # unique matches between duplicated deezer names and
    # unique sc names when one deezer duplicate has 90% of streams
    tar_target(name = dup_dz_sc_patch,
               command = patch_deezer_dups(ref = senscritique, 
                                           ref_id = "sc_artist_id", 
                                           ref_name = "sc_name",
                                           all = all)),
    
    # unique matches between duplicated deezer names and
    # unique mbz names when one deezer duplicate has 90% of dz_stream_share
    tar_target(name = dup_dz_mbz_patch,
               command = patch_deezer_dups(ref = mbz_deezer, 
                                           ref_id = "mbz_artist_id", 
                                           ref_name = "mbz_name",
                                           all = all)),
    
    # unique matches between unique deezer names and
    # duplicated sc names when one deezer duplicate has 90% of collection_counts
    tar_target(name = dup_sc_patch,
               command = patch_sc_dups(all, senscritique)),
    
    # update initial left-joined dataset with all patches
    tar_target(name = all_patched,
               command = all %>% 
                 update_rows(sc_names_patch = sc_names_patch,
                             dup_dz_mbz_patch = dup_dz_mbz_patch,
                             dup_dz_sc_patch = dup_dz_sc_patch,
                             mbz_names_patch = mbz_names_patch,
                             wiki_names_patch = wiki_names_patch,
                             wiki_mbz_names_patch = wiki_mbz_names_patch,
                             wiki_mbz_ids_patch = wiki_mbz_ids_patch,
                             dup_sc_patch = dup_sc_patch)
               ),
  
  # deduplicate all 3 ids by taking the most popular duplicate on
  # collection_count for dz duplicates, and on dz_stream_share 
  # for sc and mbz duplicates
  # export dropped duplicates to onyxia
  tar_target(name = all_final, 
             command = deduplicate_ids(all_patched)),
  
  # for testing purposes!
  tar_target(name = telerama,
             command = clean_telerama(file="telerama_raw.csv")),

  tar_target(name = lefigaro,
             command = clean_lefigaro(file="lefigaro-complet-v0.csv")),
  
  tar_target(name = liberation,
             command = clean_liberation(file="liberation-complet-v2.csv")),
  
  tar_target(name = lemonde,
             command = clean_lemonde(filepath="lemonde/lemonde-20")),
  
  tar_target(name = press_corpus,
             command = bind_press_corpora(telerama, lefigaro, liberation, lemonde)),
  
  # load entities file separately
  tar_target(name = press_named_entities,
             command = clean_press_ents("press_files/extracted_ents_1203.csv")),
  
  # names to drop
  tar_target(name = entities_to_drop,
             command = list_entities_to_drop(file="press_files/press_outliers_checked_1003.csv")),
  
  # aliases to update
  # attention: hand-coded csv files which we might update!
  tar_target(name = aliases_to_add,
             command = list_aliases(file1 = "press_files/ents_without_match_checked_1003.csv",
                                    file2 = "press_files/press_outliers_checked_1003.csv",
                                    all_final)),

  # output: all press counts linked to valid dz_artist_id
  # AND export the "CHECK" datasets as byproduct!!!
  # implement my dictionaries to remove names and add aliases
  tar_target(name = press_name_counts,
             command = count_names_press(all_final, 
                                         press_named_entities, 
                                         min_n_mentions = 30)),
  
  tar_target(name = upd_press_name_counts,
             command = update_press_names(press_name_counts, 
                                          aliases_to_add, 
                                          entities_to_drop)),
  
  # join press name counts into all_final
  tar_target(name = all_final_press,
             command = press_counts_to_final(all_final, 
                                             upd_press_name_counts)),
  
  
  # compute mbz release variables
  # left_join this to all_final later
  tar_target(name = mbz_releases,
             command = load_mbz_releases(all_final,
                                         release_file="musicbrainz/musicbrainz_releases.csv",
                                         dates_active_file="/musicbrainz/mbid_artist_end_date.csv",
                                         genre_file="records_w3/items/artists_data.snappy.parquet")), # PLACEHOLDER!
  
  # compute 2 radio variables
  # integrate later
  tar_target(name = radio_counts,
             command = count_radio_plays(file="records_w3/radio/radio_plays_with_artist_id.csv")),
  
  tar_target(name = mbz_artist_country,
             command = make_artist_country(mbz_area_file="musicbrainz/musicbrainz_area.csv",
                                           area_to_country_file="data/area_country.csv",
                                           country_rank_file="data/country_rank.csv")),
  
  # language
  tar_target(name = artist_language,
             command = load_s3("records_w3/artists_songs_languages.csv") %>% 
               as_tibble() %>% 
               mutate(dz_artist_id = as.character(art_id),
                      lang_main = lang,
                      lang_main_nb_songs = nb_songs) %>% 
               arrange(dz_artist_id, desc(lang_main_nb_songs)) %>% 
               group_by(dz_artist_id) %>% 
               slice(1) %>% 
               ungroup() %>% 
               select(dz_artist_id, lang_main, lang_main_nb_songs)
             ),
  
  # gender
  tar_target(name = mbz_gpt_gender,
             command = make_artist_gender(all_final,
                                          mbz_gender_file="musicbrainz/mbid_gender.csv",
                                          gpt_gender_file="gpt_music_data/gpt_gender.csv")),
  
  # ratings
  tar_target(name = sc_ratings,
             command = make_sc_ratings(sc_albums_ratings_file="/senscritique/ratings.csv",
                                       sc_albums_list_file="senscritique/contacts_albums_link.csv",
                                       sc_tracks_ratings_file="senscritique/tracks.csv",
                                       sc_tracks_list_file="senscritique/contact_tracks_link.csv",
                                       track_weight = .2)
             ),
  
  
  
  ### --------------------------- RESPONDENT DEMOGRAPHICS
  # load survey
  tar_target(name = survey_raw,
             command = load_s3("records_w3/survey/RECORDS_Wave3_apr_june_23_responses_corrected.csv") %>% 
               as_tibble() %>% 
               filter(Progress == 100,
                      country == "FR")
             ),
  
  # compute isei scores of respondent
  # TEMP: delete target once isei is stable
  tar_target(name = raw_isei,
             command = make_raw_isei(survey_raw, 
                                            isco_isei_file = "PCS2020/isco_isei.csv", 
                                            isco_file = "PCS2020/L72_Matrice_codification_ISCO_collecte_2023.csv", 
                                            openrefine_file = "records_w3/survey/pcs_openrefine1.csv")),

  tar_target(name = respondent_isei,
             command = make_respondent_isei(respondent_streams, raw_isei)),
  
  tar_target(name = respondent_highered,
             command = make_respondent_highered(survey_raw, respondent_streams)),
  
  # combine respondent education, isei, age and gender
  tar_target(name = respondent_demographics,
             command = make_respondent_demo(respondent_streams, survey_raw,
                                            respondent_highered, respondent_isei)),
  

  # final dataframe with selected variables
  tar_target(name = df,
             command = all_final_press %>% 
               
              
               left_join(radio_counts, by = "dz_artist_id") %>% 
               left_join(mbz_releases, by = "mbz_artist_id") %>% 
               left_join(mbz_artist_country, by = "mbz_artist_id") %>% 
               left_join(artist_language, by = "dz_artist_id") %>% 
               left_join(mbz_gpt_gender, by = "dz_artist_id") %>%
               left_join(sc_ratings, by = "sc_artist_id") %>% 

               # respondent demographics
               left_join(respondent_demographics, by = "dz_artist_id") %>% 

               
               # compute stream share
               mutate(
                 n_plays_share = n_plays / sum(n_plays, na.rm = T) * 100,
                 n_plays_share_respondent = n_plays_respondent / sum(n_plays_respondent, na.rm = T) * 100
                 ) %>% 
               
               select(
                 dz_name,
                 ends_with("_id"),
                 starts_with("n_"),
                 sc_collection_count,
                 starts_with("sc_avg_"),
                 starts_with("press_n_"),
                 starts_with("radio_"),
                 starts_with("press_"),
                 artist_country,
                 gender,
                 starts_with("lang_"),
                 starts_with("respondent_")
                 )
  )

)

# 2 PERFECT DUPLICATES (1): "Crash!" ---> dz_artist_id == 271763
# COMES FROM PRESS I THINK WHERE I CODED HIM TWICE
# SOLVE LATER

# df <- df %>% 
#   add_count(dz_artist_id) %>% 
#   filter(n > 1)


















