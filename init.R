

needed.packages <- c('targets',
                     'tarchetypes',
                     'visNetwork',
                     'aws.s3',
                     'tidytable',
                     'logging',
                     'dplyr',
                     'WikidataQueryServiceR',
                     'rstudioapi')


for(pack in needed.packages){
  if(pack %in% rownames(installed.packages()) == FALSE)
  {install.packages(pack)}
}


# set theme and pane layout 

rstudioapi::applyTheme("Merbivore Soft")

rstudioapi::writeRStudioPreference("pane_config", list(
     console = "left",
     source = "right",
     tabSet1 = "right",
     tabSet2 = "right",
     hiddenTabSet = "none"
     ))
















