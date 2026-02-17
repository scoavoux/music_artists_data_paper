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
    
    # load and aggregate raw streams
    tar_target(name = dz_streams,
               command = load_streams()),
    
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
               command = bind_dz_songs(dz_songs_old, dz_songs_new, dz_streams, dz_names)),
    
    
    # group songs by featured artists and compute weighted popularity
    tar_target(name = dz_artists,
               command = group_songs_by_artist(dz_songs)),
    
    
    # -------- load and process raw ID data
    
    # sc_artist_id (+ metadata) to mbz_artist_id
    tar_target(name = senscritique, 
               command = load_senscritique(sc_file="senscritique/contacts.csv",
                                           sc_ratings_file = "senscritique/ratings.csv",
                                           sc_albums_file = "senscritique/contacts_albums_link.csv")),
    
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
               command = join_artist_ids(dz_artists, mbz_deezer,
                                             senscritique, manual_search, wiki)),
    
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
                             dup_sc_patch = dup_sc_patch) %>% 
                 
                 ## append ratings (refactor later)
                 select(-n_ratings) %>% 
                 left_join(senscritique %>% 
                             select(sc_artist_id, n_ratings),
                           by = "sc_artist_id")),
  
  # deduplicate all 3 ids by taking the most popular duplicate on
  # collection_count for dz duplicates, and on dz_stream_share 
  # for sc and mbz duplicates
  # export dropped duplicates to onyxia
  tar_target(name = all_final, 
             command = deduplicate_ids(all_patched)),
  
  # for testing purposes!
  tar_target(name = telerama,
             command = clean_telerama(file="french_media/telerama_raw.csv")),
  
  tar_target(name = aliases,
             command = make_aliases(all_final, 
                                    mbz_alias_file="musicbrainz/mbid_name_alias.csv"))
)


## RENAME all TO artists EVERYWHERE! (variations of artists)











