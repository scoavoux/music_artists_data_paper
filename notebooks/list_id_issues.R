library(dplyr)
library(tidyverse)
library(data.table)
library(ggplot2)

tar_source("R")

nrow(items) # 14,77M
uniqueN(items$artist_id) # 1,15M --> why so many??
nrow(user_artist) # 32,22M
uniqueN(user_artist$artist_id) # 426K
nrow(pop) # 380K, unique artist_id

######################

## look at how artist popularity influences matching
pop2 <- na.omit(pop)


sum(pop2$control_f_l_play)
sum(pop2$control_f_n_play)

pop2 <- pop2[order(pop2$control_f_n_play, decreasing = T),]


#### cumulative listening time and N plays
pop2 <- pop2 %>%
  arrange(desc(control_f_n_play)) %>%      # make sure in correct order
  mutate(cum_control_f_n_play = cumsum(control_f_n_play),)

pop2 <- pop2 %>%
  arrange(desc(control_f_l_play)) %>%
  mutate(cum_control_f_l_play = cumsum(control_f_l_play),)


# ~85-90K artists totalize 99% of plays / length of play
pop2[pop2$cum_control_f_n_play < 0.99]
pop2[pop2$cum_control_f_l_play < 0.99]



## see how popularity influences chances of matching

### try matching artists with other ids (mbz?)
pop99 <- pop2[pop2$cum_control_f_n_play < 0.99, c("artist_id", "name", 
                                                  "control_f_n_play",
                                                  "cum_control_f_n_play")]






























