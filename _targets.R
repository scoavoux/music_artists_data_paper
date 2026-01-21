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
  
    ### CREATE ARTISTS ----------------------------------------------
    tar_target(name = streams,
               command = load_streams()),
    
    tar_target(name = to_remove_file,
               command = read.csv("data/artists_to_remove.csv")),
    
    tar_target(name = items_old,
               command = make_items(to_remove = to_remove_file,
                                    file = "records_w3/items/songs.snappy.parquet")),
    
    tar_target(name = items_new,
               command = make_items(to_remove = to_remove_file,
                                    file = "records_w3/items/song.snappy.parquet")),
    
    tar_target(name = names,
               command = bind_names(file_1 = "records_w3/items/artists_data.snappy.parquet",
                                    file_2 = "interim/new_artists_names_from_api.csv")),
    
    tar_target(name = items,
               command = bind_items(items_old, items_new, streams, names)),
    
    tar_target(name = artists,
               command = group_items_by_artist(items)),
    
    # ID shit
    ### LOAD RAW KEYS ----------------------------------------------
    tar_target(name = contacts, 
               command = load_s3("senscritique/contacts.csv")),
    
        tar_target(name = musicbrainz_urls,
               command = load_s3("musicbrainz/musicbrainz_urls.csv")),
    
    tar_target(name = manual_search_path,
               command = "data/manual_search.csv", format = "file"),
    tar_target(name = manual_search,
               command = read.csv(manual_search_path)),
    
    ### PROCESS KEYS -----------------------------------------------
    # transform musicbrainz_urls to mbz_deezer
    tar_target(name = mbz_deezer,
               command = make_mbz_deezer(musicbrainz_urls)),
    
    # temporary --- make a dedicated wiki_labels function some time
    # code is (commented out) in wiki_keys.R
    tar_target(name = wiki_labels,
               command = load_s3("interim/wiki_labels.csv")),

    tar_target(name = wiki,
               command = make_wiki_keys(wiki_labels, mbz_deezer)),
    
    
    ### CONSOLIDATE ARTISTS ----------------------------------------
    
    tar_target(name = all,
               command = consolidate_artists(artists, mbz_deezer,
                                             contacts, manual_search)),
    

    # unique names matches between deezer and contact names
    tar_target(name = contact_names_patch,
               command = patch_names(all = all,
                                     ref = contacts,
                                     ref_id = "contact_id",
                                     ref_name = "contact_name",
                                     all_name = "name")),
    
    tar_target(name = mbz_names_patch,
               command = patch_names(all = all,
                                     ref = mbz_deezer,
                                     ref_id = "musicbrainz_id",
                                     ref_name = "mbz_name",
                                     all_name = "name")),
    
    tar_target(name = wiki_mbz_names_patch,
               command = patch_names(all = all,
                                     ref = wiki,
                                     ref_id = "musicbrainz_id",
                                     ref_name = "mbz_name",
                                     all_name = "name")),
    
    # tar_target(name = wiki_names_patch,
    #            command = patch_names(all = all,
    #                                  ref = wiki,
    #                                  ref_id = "musicbrainz_id",
    #                                  ref_name = "wiki_name",
    #                                  all_name = "name")),
    
    tar_target(name = wiki_mbz_ids_patch,
               command = mbz_from_wiki(all, wiki)),
    
    tar_target(name = all_enriched,
               command = update_rows(all, 
                                     contact_names_patch = contact_names_patch,
                                     dup_deezer_mbz_patch = contact_names_patch,
                                     dup_deezer_co_patch = dup_deezer_co_patch,
                                     mbz_names_patch = mbz_names_patch,
                                     #wiki_names_patch = wiki_names_patch,
                                     wiki_mbz_names_patch = wiki_mbz_names_patch,
                                     wiki_mbz_ids_patch = wiki_mbz_ids_patch,
                                     dup_contacts_patch = dup_contacts_patch)),
    
    tar_target(name = dup_deezer_co_patch,
               command = patch_deezer_dups(ref = contacts, 
                                           ref_id = "contact_id", 
                                           ref_name = "contact_name",
                                           all = all)),
    tar_target(name = dup_deezer_mbz_patch,
               command = patch_deezer_dups(ref = mbz_deezer, 
                                           ref_id = "musicbrainz_id", 
                                           ref_name = "mbz_name",
                                           all = all)),
    tar_target(name = dup_contacts_patch,
               command = patch_contact_dups(all, contacts))
)



## subset contacts to reviews > 0




