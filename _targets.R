
options(warn=0) # to suppress warnings, set to -1

# SIMULATION=FALSE LOCAL_DATA_DIR="" Rscript -e "targets::tar_make()"

SIMULATION <- as.logical(
  Sys.getenv("SIMULATION", unset = "FALSE")
)

LOCAL_DATA_DIR <- Sys.getenv(
  "LOCAL_DATA_DIR",
  unset = ""
)

library(targets)
library(tarchetypes)
library(dplyr)
library(paws)
library(tidyr)
library(stringr)
library(arrow)
library(data.table)
library(sjmisc)
library(stringi)
library(yardstick)
library(kableExtra)


targets::tar_option_set(

  repository = "aws", 
  repository_meta = "aws",
  resources = tar_resources(
    aws = tar_resources_aws(
      endpoint = Sys.getenv("S3_ENDPOINT"),
      bucket = "scoavoux",
      prefix = "omnivorism"),
    
    ),
  packages = c("tarchetypes", "paws", "tidyr", "stringr",
               "tidyverse", "arrow", "data.table", "sjmisc",
               "stringi", "yardstick", "kableExtra")
)

targets::tar_source("R")


# List of targets ------
list(
  
    ### CREATE (deezer) ARTISTS ----------------------------------------------

    tar_target(dz_users,
               load_s3("records_w3/survey/RECORDS_hashed_user_group.parquet") %>% 
                 mutate(
                   is_respondent = ifelse(is_respondent == TRUE, 1, 0),
                   is_control = ifelse(is_in_control_group == TRUE, 1, 0)) %>% 
                 filter(
                   is_respondent != 0 | is_control != 0
                   ) %>% # remove users who are neither control nor respondent
                 select(hashed_id, 
                        is_respondent, 
                        is_control)),
    
    ## ---- streams: 2 targets because calculating user-song popularity
    ## for all users is too intensive computationally
    
    # make popularity split by control and respondents
    tar_target(dz_stream_data,
               make_stream_popularity(dz_songs, dz_users)),
    
    # make popularity at user-artist level for respondents
    tar_target(respondent_streams,
               make_respondent_plays(dz_songs, dz_users)),
    
    # bind old and new songs and names, join to streams
    tar_target(dz_names,
               bind_dz_names(file_1 = "records_w3/items/artists_data.snappy.parquet",
                            file_2 = "interim/prod/new_artists_names_from_api.csv")),
    
    tar_target(dz_songs_old,
               make_dz_songs(to_remove_file = "interim/dict/artists_to_remove.csv",
                             file = "records_w3/items/songs.snappy.parquet")),
    
    tar_target(dz_songs_new,
               make_dz_songs(to_remove_file = "interim/dict/artists_to_remove.csv",
                              file = "records_w3/items/song.snappy.parquet")),
    
    tar_target(classical_albums,
               filter_classical_albums(album_file="interim/prod/genres_from_albums.parquet",
                                       genre_mapping_file="interim/dict/deezer_genre_mapping.csv")),

    tar_target(dz_songs,
               bind_dz_songs(dz_songs_old, dz_songs_new, 
                             classical_albums, dz_names)),
    
    
    # -------- load and process raw ID data
    
    # sc_artist_id (+ metadata) to mbz_artist_id
    tar_target(senscritique, 
               load_senscritique(sc_file="senscritique/contacts.csv")),
    
    # manual searches sc_artist_id to dz_artist_id
    tar_target(manual_search,
               load_s3("interim/dict/manual_search_ids.csv") %>% 
                 mutate(dz_artist_id = as.character(dz_artist_id),
                        sc_artist_id = as.character(sc_artist_id))),

    # mbz_artist_id to dz_artist_id
    tar_target(mbz_deezer,
               load_mbz_deezer(file="musicbrainz/musicbrainz_urls.csv")),
    
    # wikidata artist names
    tar_target(wiki_labels,
               load_s3("interim/prod/wiki_labels.csv")),

    # wikidata itemId to mbz_artist_id and dz_artist_id
    tar_target(wiki,
               load_wiki(mbz_deezer)),
    
    
    ### BUILD AND CONSOLIDATE ARTISTS ----------------------------------------
    
    ##### group by artist and left-join raw data
    tar_target(artists_to_patch,
               join_artist_ids(dz_songs, 
                               dz_stream_data,
                               mbz_deezer,
                               senscritique, 
                               manual_search, 
                               wiki)),
    
    # unique matches between deezer and senscritique names
    tar_target(sc_names_patch,
               patch_names(all = artists_to_patch,
                                     ref = senscritique,
                                     ref_id = "sc_artist_id",
                                     ref_name = "sc_name",
                                     all_name = "dz_name")),
    
    # unique matches between deezer and mbz names
    tar_target(mbz_names_patch,
               patch_names(all = artists_to_patch,
                                     ref = mbz_deezer,
                                     ref_id = "mbz_artist_id",
                                     ref_name = "mbz_name",
                                     all_name = "dz_name")),
    
    # unique matches between deezer and mbz names from wiki
    tar_target(wiki_mbz_names_patch,
               patch_names(all = artists_to_patch,
                                     ref = wiki,
                                     ref_id = "mbz_artist_id",
                                     ref_name = "mbz_name",
                                     all_name = "dz_name")),
    
    # unique matches between deezer and wiki names
    tar_target(wiki_names_patch,
               patch_names(all = artists_to_patch,
                                     ref = wiki,
                                     ref_id = "mbz_artist_id",
                                     ref_name = "wiki_name",
                                     all_name = "dz_name")),
    
    # mbz ids retrieved from wiki
    tar_target(wiki_mbz_ids_patch,
               patch_mbz_from_wiki(artists_to_patch, wiki)),
    
    # unique matches between duplicated deezer names and
    # unique sc names when one deezer duplicate has 90% of streams
    tar_target(dup_dz_sc_patch,
               patch_deezer_dups(ref = senscritique, 
                                           ref_id = "sc_artist_id", 
                                           ref_name = "sc_name",
                                           all = artists_to_patch)),
    
    # unique matches between duplicated deezer names and
    # unique mbz names when one deezer duplicate has 90% of dz_stream_share
    tar_target(dup_dz_mbz_patch,
               patch_deezer_dups(ref = mbz_deezer, 
                                           ref_id = "mbz_artist_id", 
                                           ref_name = "mbz_name",
                                           all = artists_to_patch)),
    
    # unique matches between unique deezer names and
    # duplicated sc names when one deezer duplicate has 90% of collection_counts
    tar_target(dup_sc_patch,
               patch_sc_dups(artists_to_patch, senscritique)),
    
    # update initial left-joined dataset with all patches
    # and deduplicate ids with custom function
    tar_target(artists,
               artists_to_patch %>% 
                 update_rows(sc_names_patch = sc_names_patch,
                             dup_dz_mbz_patch = dup_dz_mbz_patch,
                             dup_dz_sc_patch = dup_dz_sc_patch,
                             mbz_names_patch = mbz_names_patch,
                             wiki_names_patch = wiki_names_patch,
                             wiki_mbz_names_patch = wiki_mbz_names_patch,
                             wiki_mbz_ids_patch = wiki_mbz_ids_patch,
                             dup_sc_patch = dup_sc_patch) %>% 
                 
                 # deduplicate ids!
                 deduplicate_ids() 
               ),

  tar_target(press_corpus,
             bind_press_corpora(telerama_file = "telerama_raw.csv",
                                lefigaro_file = "lefigaro-complet-v0.csv",
                                liberation_file = "liberation-complet-v2.csv",
                                lemonde_filepath = "lemonde/lemonde-20",
                                bert_reviews_file = "interim/press/bert_review_classif.csv")),

  # load entities file separately
  tar_target(press_named_entities,
             clean_press_ents("interim/press/extracted_ents_2105.csv")), # CHANGED FROM 1203 TO NEW ENT FILE
  
  # names to drop
  tar_target(entities_to_drop,
             list_entities_to_drop(file="interim/press/press_outliers_checked_1003.csv")),
  
  # aliases to update
  # attention: hand-coded csv files which we might update!
  tar_target(aliases_to_add,
             list_aliases(file1 = "interim/press/ents_without_match_checked_1003.csv",
                          file2 = "interim/press/press_outliers_checked_1003.csv",
                          artists)),

  # output: all press counts linked to valid dz_artist_id
  # AND export the "CHECK" datasets as byproduct!!!
  # implement my dictionaries to remove names and add aliases
  tar_target(press_name_counts,
             count_names_press(artists, 
                               press_named_entities, 
                               min_n_mentions = 30)),
  
  tar_target(upd_press_name_counts,
             update_press_names(press_name_counts, 
                                aliases_to_add, 
                                entities_to_drop)),
  
  # compute mbz release variables
  tar_target(mbz_releases,
             load_mbz_releases(release_file="musicbrainz/musicbrainz_releases.csv",
                               dates_active_file="musicbrainz/musicbrainz_artist_end_date.csv",
                               genre=mbz_genre_album)), # PLACEHOLDER!
  
  # compute 2 radio variables
  # integrate later
  tar_target(radio_counts,
             count_radio_plays(file="records_w3/radio/radio_plays_with_artist_id.csv")),
  
  tar_target(mbz_artist_country,
             make_artist_country(mbz_area_file="musicbrainz/musicbrainz_area.csv",
                                 area_country_file="interim/prod/area_country.csv",
                                 country_rank_file="interim/dict/country_rank.csv")),
  
  # language
  tar_target(artist_language,
             load_s3("interim/prod/artists_songs_languages.csv") %>% 
               as_tibble() %>% 
               mutate(dz_artist_id = as.character(art_id),
                      language_main = lang,
                      language_main_n_songs = nb_songs) %>% 
               arrange(dz_artist_id, desc(language_main_n_songs)) %>% 
               group_by(dz_artist_id) %>% 
               slice(1) %>% 
               ungroup() %>% 
               select(dz_artist_id, language_main, language_main_n_songs)
             ),
  
  # gender
  tar_target(mbz_gpt_gender,
             make_artist_gender(artists,
                                mbz_gender_file="musicbrainz/musicbrainz_artist_gender.csv",
                                gpt_gender_file="interim/prod/gpt_gender.csv")
             ),
  
  # ratings
  tar_target(sc_ratings,
             make_sc_ratings(sc_albums_ratings_file="senscritique/ratings.csv", # REMOVED / in filepath
                                       sc_albums_list_file="senscritique/contacts_albums_link.csv",
                                       sc_tracks_ratings_file="senscritique/tracks.csv",
                                       sc_tracks_list_file="senscritique/contact_tracks_link.csv",
                                       track_weight = .2)
             ),
  
  
  
  ### --------------------------- RESPONDENT DEMOGRAPHICS
  # load survey --> ALREADY MAKE SURVEY VARIABLES HERE??
  tar_target(survey_raw,
             load_s3("records_w3/survey/RECORDS_Wave3_apr_june_23_responses_corrected.csv") %>% 
               as_tibble() %>% 
               filter(Progress == 100,
                      country == "FR") %>% 
               select(hashed_id, 
                      E_birth_year, 
                      E_gender, 
                      E_diploma,
                      starts_with("E_FR_prof_"))
             ),
  
  # compute isei scores of respondent
  # TEMP: delete target once isei is stable
  tar_target(raw_isei,
             make_raw_isei(survey_raw, 
                           isco_isei_file = "interim/prod/isco_isei.csv", 
                           isco_file = "interim/prod/L72_Matrice_codification_ISCO_collecte_2026.csv", 
                           recode_file = "interim/prod/professions_recodees.csv")),

  tar_target(respondent_isei,
             make_respondent_isei(respondent_streams, 
                                  raw_isei)),
  
  tar_target(respondent_educ,
             make_respondent_educ(survey_raw, 
                                  respondent_streams)),
  
  # combine respondent education, isei, age and gender
  tar_target(respondent_demographics,
             make_respondent_demo(respondent_streams, 
                                  survey_raw,
                                  respondent_educ, 
                                  respondent_isei,
                                  min_n_users = 10)),
  
  # -------------- GENRE 

  tar_target(dz_genre_album,
             load_dz_genre_album(album_file="interim/prod/genres_from_albums.parquet",
                                  genre_mapping_file="interim/dict/deezer_genre_mapping.csv")),
  
  tar_target(mbz_genre_album,
             load_mbz_genre_album(file="musicbrainz/musicbrainz_artist_releasegroup_genre.csv")),
  
  tar_target(mbz_genre_artist,
             load_mbz_genre_artist(file="musicbrainz/musicbrainz_artist_genre.csv")),
  
  
  # ------------------- NEW VARIABLES, TEMP LOCATION
  
  tar_target(n_tracks_feats,
             compute_n_tracks(dz_songs)),
  
  tar_target(dz_likes,
             make_dz_likes(favorites_file="records_w3/favorites/RECORDS_hashed_user_favorites.parquet",
                               dz_songs, 
                               survey_raw, 
                               raw_isei, 
                               dz_users)
  ),
  
  tar_target(song_diversity,
             make_song_diversity(dz_songs, 
                                 dz_users, 
                                 path_long="records_w3/streams/streams_long", 
                                 path_short="records_w3/streams/streams_short")
  ),
  
  tar_target(n_followers,
             load_s3("records_w3/artists_pop.csv") %>% 
               mutate(dz_artist_id = as.character(artist_id)) %>% 
               select(dz_artist_id, n_followers = "nb_fans")
             ),
  
  # Tables and plots for data paper
  tar_target(tb_gender_validation_metric,
             validate_annotation(),
             format = "file", 
             repository = "local"),

  # final dataframe with selected variables
  tar_target(df_complete,
             artists %>% 
               
               # press counts updated with aliases counts
               make_press_counts(upd_press_name_counts) %>% 
               
               # artist information
               left_join(radio_counts, by = "dz_artist_id") %>% 
               left_join(mbz_releases, by = "mbz_artist_id") %>% 
               left_join(mbz_artist_country, by = "mbz_artist_id") %>% 
               left_join(artist_language, by = "dz_artist_id") %>% 
               left_join(mbz_gpt_gender, by = "dz_artist_id") %>%
               left_join(sc_ratings, by = "sc_artist_id") %>% 

               # respondent demographics
               left_join(respondent_demographics, by = "dz_artist_id") %>% 
               
               # genres

               # left_join(dz_genre_artist, by = "dz_artist_id") %>% 
               left_join(dz_genre_album, by = "dz_artist_id") %>% 
               
               left_join(mbz_genre_artist, by = "mbz_artist_id") %>% 
               left_join(mbz_genre_album, by = "mbz_artist_id") %>% 
               
               left_join(n_tracks_feats, by = "dz_artist_id") %>% 
               
               left_join(dz_likes, by = "dz_artist_id") %>% 
               
               left_join(song_diversity, by = "dz_artist_id") %>% 
               
               left_join(n_followers, by = "dz_artist_id") %>% 
               
               # rm, among others, our dear friend michel onfray
               filter(is.na(genre_dz_album_1) | genre_dz_album_1 != "Livres audio") %>% 
             
               # compute final stream share
               mutate(
                 n_plays_share = n_plays / sum(n_plays, na.rm = T) * 100,
                 n_plays_share_respondent = n_plays_respondent / sum(n_plays_respondent, na.rm = T) * 100
                 ) %>% 
               
               # select needed variables
               select(
                 dz_name,
                 ends_with("_id"),
                 starts_with("n_"),
                 starts_with("likes_"),
                 starts_with("div_"),
                 starts_with("genre_"),
                 sc_collection_count,
                 starts_with("sc_avg_"),
                 starts_with("press_n_"),
                 starts_with("radio_"),
                 starts_with("press_"),
                 starts_with("release_"),
                 country_of_origin,
                 gender,
                 starts_with("language_"),
                 starts_with("audience_")
                 )
  ),
  
  
  tar_target(df_data_paper,
             
             df_complete %>% 
               
      
               # bin n_plays
               mutate(
                 rank = row_number(),
                 n_plays = case_when(
                   rank <= 100   ~ 100,
                   rank <= 500   ~ 500,
                   rank <= 1000  ~ 1000,
                   rank <= 10000 ~ 10000,
                   rank <= 50000 ~ 50000,
                   rank <= 100000 ~ 100000,
                   TRUE          ~ 300000
                 )
               ) %>% 
  
               # select data paper variables
               select(
                 dz_name,
                 ends_with("_id"),
                 
                 # artist metadata
                 starts_with("genre_dz_album_"), 
                 country_of_origin,
                 gender,
                 language_main,
                 starts_with("release_"),
                 n_tracks, 
                 n_tracks_feat,

                 # popularity
                 n_plays, 
                 div_shannon_effective,
                 sc_avg_score,
                 press_n_mentions,
                 starts_with("radio_"),
                 
                 # listener demographics
                 starts_with("audience_")
               )
  )

  
)












