# Preparation ------
library(targets)
library(tarchetypes)

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
    tar_target(name = dz_streams,
               command = load_streams()),
    
    tar_target(name = to_remove_file,
               command = read.csv("data/artists_to_remove.csv")),
    
    tar_target(name = dz_songs_old,
               command = make_songs(to_remove = to_remove_file,
                                    file = "records_w3/items/songs.snappy.parquet")),
    
    tar_target(name = dz_songs_new,
               command = make_songs(to_remove = to_remove_file,
                                    file = "records_w3/items/song.snappy.parquet")),
    
    tar_target(name = dz_names,
               command = bind_names(file_1 = "records_w3/items/artists_data.snappy.parquet",
                                    file_2 = "interim/new_artists_names_from_api.csv")),
    
    tar_target(name = dz_songs,
               command = bind_songs(dz_songs_old, dz_songs_new, dz_streams, dz_names)),
    
    tar_target(name = dz_artists,
               command = group_items_by_artist(dz_songs)),
    
    
    # -------- load and process raw ID data
    tar_target(name = senscritique, 
               command = load_senscritique(file="senscritique/contacts.csv")),
    
    tar_target(name = sc_ratings,
               command = load_ratings(sc_ratings_file = "senscritique/ratings.csv",
                                      sc_albums_file = "senscritique/contacts_albums_link.csv")),

    tar_target(name = manual_search,
               command = load_manual_search(file="data/manual_search.csv")),

    tar_target(name = mbz_deezer,
               command = load_mbz_deezer(file="musicbrainz/musicbrainz_urls.csv")),
    
    # temp --- make a dedicated wiki_labels function some time
    # code is (commented out) in wiki_keys.R
    tar_target(name = wiki_labels,
               command = load_s3("interim/wiki_labels.csv")),

    tar_target(name = wiki,
               command = load_wiki(mbz_deezer)),
    
    
    ### CONSOLIDATE ARTISTS ----------------------------------------
    
    tar_target(name = all,
               command = consolidate_artists(dz_artists, mbz_deezer,
                                             senscritique, manual_search, wiki)),
    

    # unique names matches between deezer and senscritique names
    tar_target(name = sc_names_patch,
               command = patch_names(all = all,
                                     ref = senscritique,
                                     ref_id = "sc_artist_id",
                                     ref_name = "sc_name",
                                     all_name = "dz_name")),
    
    tar_target(name = mbz_names_patch,
               command = patch_names(all = all,
                                     ref = mbz_deezer,
                                     ref_id = "mbz_artist_id",
                                     ref_name = "mbz_name",
                                     all_name = "dz_name")),
    
    tar_target(name = wiki_mbz_names_patch,
               command = patch_names(all = all,
                                     ref = wiki,
                                     ref_id = "mbz_artist_id",
                                     ref_name = "mbz_name",
                                     all_name = "dz_name")),
    
    tar_target(name = wiki_names_patch,
               command = patch_names(all = all,
                                     ref = wiki,
                                     ref_id = "mbz_artist_id",
                                     ref_name = "wiki_name",
                                     all_name = "dz_name")),
    
    tar_target(name = wiki_mbz_ids_patch,
               command = mbz_from_wiki(all, wiki)),
    
    tar_target(name = dup_dz_sc_patch,
               command = patch_deezer_dups(ref = senscritique, 
                                           ref_id = "sc_artist_id", 
                                           ref_name = "sc_name",
                                           all = all)),
    tar_target(name = dup_dz_mbz_patch,
               command = patch_deezer_dups(ref = mbz_deezer, 
                                           ref_id = "mbz_artist_id", 
                                           ref_name = "mbz_name",
                                           all = all)),
    tar_target(name = dup_sc_patch,
               command = patch_contact_dups(all, senscritique)),
    
    tar_target(name = all_enriched,
               command = all %>% 
                 update_rows(sc_names_patch = sc_names_patch,
                             dup_dz_mbz_patch = dup_dz_mbz_patch,
                             dup_dz_sc_patch = dup_dz_sc_patch,
                             mbz_names_patch = mbz_names_patch,
                             wiki_names_patch = wiki_names_patch,
                             wiki_mbz_names_patch = wiki_mbz_names_patch,
                             wiki_mbz_ids_patch = wiki_mbz_ids_patch,
                             dup_sc_patch = dup_sc_patch) %>% 
                 left_join(ratings, by = "sc_artist_id")),
  
  tar_target(name = all_dedup, 
             command = deduplicate_ids(all_enriched))
)

make()


