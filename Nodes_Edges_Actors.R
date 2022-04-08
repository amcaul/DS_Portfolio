library(gdeltr2)
library(readr)
library(dplyr)
library(stringi)
library(stringr)
library(dbplyr)
library(DBI)
library("aws.s3")
library(fuzzyjoin)
library(lubridate)


agg = s3read_using(FUN = readRDS, bucket = "cdwbucket/AubreyTestData", object = "Combined_Events_2021.RDS")
agg$actor1 = tolower(agg$actor1)
agg$actor2 = tolower(agg$actor2)


actors = c(agg$actor1, agg$actor2)
actors = as.data.frame(table(actors))
actors = actors[actors$Freq >20,]


actors = stringdist_join(actors, actors, by = "actors", mode = "left", ignore_case = TRUE, method = "jw", max_dist = 0.075, distance_col = "dist") %>% 
  group_by(actors.x) %>% slice_min(order_by = dist, n = 3) 

actors = actors %>% ungroup() %>% droplevels()
actors$actors.x = as.character(actors$actors.x)
actors$actors.y = as.character(actors$actors.y)

actors$choice = ifelse(actors$Freq.x > actors$Freq.y, actors$actors.x, actors$actors.y)



mapdf <- data.frame(old=c(actors$actors.x, actors$actors.y),new=c(actors$choice, actors$choice))
mapdf = unique(mapdf)
agg$actor1 = str_to_title(mapdf$new[match(agg$actor1,mapdf$old)])
agg$actor2 = str_to_title(mapdf$new[match(agg$actor2,mapdf$old)])


actors = unique(c(agg$actor1, agg$actor2))

edges = agg %>% group_by(actor1, actor2, country, year = year(event_date), month = month(event_date)) %>% summarize(n = n())

saveRDS(edges, "Event_Edges.RDS")
