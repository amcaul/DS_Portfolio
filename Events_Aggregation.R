library(dplyr)
library(DBI)
library(icews)
library(gdeltr2)
library(RPostgreSQL)
library(lubridate)
library(data.table)


con = dbConnect(odbc::odbc(), "PostgreSQL30", encoding = "latin1", bigint = "numeric")

## Goldstein Scores

data(goldstein_mappings)
goldstein = as.data.frame(goldstein_mappings[,1:3])
rownames(goldstein) = NULL

## ACLED

acled = tbl(con, "ACLED")

acled = acled %>% 
  dplyr::select(c("data_id", "event_date", "event_type", "sub_event_type", 
                  "actor1", "country", "admin1", "admin2", "location", 
                  "latitude", "longitude", "source", "fatalities", 
                  "timestamp")) %>%
  rename(id = data_id, sources = source) %>% collect()
acled$event_date = as.Date(acled$event_date)
acled_codes = c("14", "145", "145", "", "173", "14", "1712", "19", "18", "", 
                "192", "18", "183", "195", "181", "192", "1821", "1831", "080", 
                "183", "175","1246", "057", "080")
acled_match = data.frame(event_type = unique(acled$sub_event_type), code = acled_codes)
goldstein$name = paste(toupper(substr(goldstein$name, 1, 1)), 
                       tolower(substr(goldstein$name, 2, nchar(goldstein$name))), sep="")
match = merge(acled_match, goldstein, all.x = TRUE)
match = match %>% dplyr::select(event_type, name, goldstein)
mapdf = data.frame(old = match$event_type, new = match$name)
acled$event_type = mapdf$new[match(acled$sub_event_type, mapdf$old)]
mapdf = data.frame(old = match$name, new = match$goldstein)
acled$goldstein = mapdf$new[match(acled$event_type, mapdf$old)]
acled = acled %>% dplyr::select(-c(timestamp, sub_event_type))


## UCDP


ucdp = tbl(con, "UCDP")
ucdp = ucdp %>% 
  dplyr::select("id", "code_status", "side_a", "side_b", "number_of_sources", 
                "where_coordinates", "adm_1", "adm_2", "latitude", "longitude", 
                "country", "date_end", "best") %>%
  rename(actor1 = side_a, actor2 = side_b, sources = number_of_sources, 
         location = where_coordinates, admin1 = adm_1, admin2 = adm_2,
         event_date = date_end, fatalities = best) %>%
  collect()

ucdp$event_date = as.Date(substr(ucdp$event_date, 1, 10))
ucdp$event_type = rep("Fight", length(ucdp$id))
ucdp$goldstein = rep(-10, length(ucdp$id))
ucdp = ucdp[ucdp$code_status %in% c("Clear", "Check deaths", "Check dyad", 
                                    "Check geography"),]

ucdp = ucdp %>% dplyr::select(-c(code_status))


## ICEWS

icews = tbl(con, "ICEWS")

icews = icews %>% 
  dplyr::select("event_id", "event_date", "source_name", "event_text", 
                "target_name", "city", "district", "province", "country", 
                "latitude", "longitude", "goldstein") %>%
  rename(id = event_id, actor1 = source_name, event_type = event_text, 
         actor2 = target_name, location = city, admin2 = district, admin1 = province) %>% collect()
icews$sources = rep(1, length(icews$id))

## GDELT


gdelt = tbl(con, "GDELT")

gdelt = gdelt %>% 
  dplyr::filter(NumSources > 1 & !is.na(c(ActionGeo_Lat))) %>%
  dplyr::select("GLOBALEVENTID", "SQLDATE", "Actor1Name", "Actor2Name", 
                "EventCode", "GoldsteinScale", "NumSources", 
                "ActionGeo_FullName", "ActionGeo_Lat", "ActionGeo_Long") %>% 
  distinct() %>%
  collect()
  

gdelt = gdelt %>% tidyr::separate(ActionGeo_FullName, into =  c("admin1", "admin2", "country"), sep = ", ", extra = "drop",  fill = "left")
mapdf = data.frame(old = goldstein$code, new = goldstein$name)
gdelt$event_type = mapdf$new[match(gdelt$EventCode, mapdf$old)]
gdelt$event_date = as.Date(as.character(gdelt$SQLDATE), format = "%Y%m%d")
gdelt = gdelt %>% dplyr::select(-c(SQLDATE, EventCode))

colnames(gdelt) = c("id", "actor1", "actor2", "goldstein", 
                    "sources", "admin1", "admin2", "country", "latitude", 
                    "longitude", "event_type", "event_date")
gdelt = gdelt[!duplicated(gdelt[c("event_date", "country", "admin2", "actor2", "actor1")]),]
gdelt = gdelt[!is.na(gdelt$actor1) & !is.na(gdelt$actor2),]
gdelt = gdelt[!duplicated(gdelt[c("event_date", "country", "admin2", "event_type")]),]


## Combining Data


sets = list(gdelt, icews, acled, ucdp)
for (i in 1:4){
  #sets[[i]] = sets[[i]] %>% dplyr::filter(event_date < "2021-06-01")
  sets[[i]]$latitude = as.character(sets[[i]]$latitude)
  sets[[i]]$longitude = as.character(sets[[i]]$longitude)
}
names(sets) = c("gdelt", "icews", "acled", "ucdp")

combined = rbindlist(sets, use.names = TRUE, fill = TRUE, idcol = "Source")



combined = dplyr::mutate_if(combined, is.character, .funs = function(x){return(`Encoding<-`(x, "UTF-8"))})


combined = data.frame(lapply(combined, function(variables){
  if (is.character(variables)) {
    variables = gsub("\\s*\\([^\\)]+\\)","",as.character(variables))
  } 
  else {
    return(variables)
  }
}))

combined$actor1 = ifelse(is.na(combined$actor1), combined$country, combined$actor1)
combined$actor2 = ifelse(is.na(combined$actor2), combined$country, combined$actor2)

combined$sources = as.numeric(combined$sources)
combined$sources = ifelse(is.na(combined$sources), 1, combined$sources)


## Aggregating


aggregate = combined %>% 
  group_by(event_date, event_type, country, admin2, actor1, actor2) %>%
  mutate (ids = paste0(Source, "-", id, collapse = ", ")) %>%
  summarize(ids = ids, 
            admin1 = admin1,
            fatalities = sum(fatalities, na.rm = TRUE),
            sources = sum(sources, na.rm = TRUE),
            goldstein = mean(goldstein, na.rm = TRUE))

aggregate = aggregate[!duplicated(aggregate[c("event_date", "country", "admin2", "event_type", "ids")]),]
aggregate = distinct(aggregate)
aggregate = aggregate[!duplicated(aggregate[c("event_date", "country", "actor1", "actor2", "event_type", "ids")]),]

dbWriteTable(con, "Aggregated_Events", aggregate, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "Combined_Events_2021", combined, overwrite = TRUE, row.names = FALSE)


library("aws.s3")
library(StandardizeText)
library(countrycode)
library(rnaturalearth)
library(sf)
library(fuzzyjoin)
library(scales)

if (require("utils")) {
  s3write_using(combined, FUN = saveRDS, object = "Combined_Events_2021.RDS", bucket = "cdwbucket/AubreyTestData")
}

if (require("utils")) {
  s3write_using(aggregate, FUN = saveRDS, object = "Aggregate_Events_2021.RDS", bucket = "cdwbucket/AubreyTestData")
}



## Make SF File

agg = combined


agg$country = standardize.countrynames(agg$country, standard = "default", suggest = "auto", verbose = FALSE, print.changes = FALSE)
countrymaps = ne_states(returnclass = "sf")
countrymaps$country = countrycode(countrymaps$adm0_a3, origin = "iso3c", destination = "country.name", warn = FALSE, nomatch = NA)
countrymaps$new_names = standardize.countrynames(countrymaps$country, standard = "default", suggest = "auto", verbose = FALSE, print.changes = FALSE)
countrynames = unique(intersect(countrymaps$new_names, agg$country)) #%>% filter(complete.cases(new_names, woe_name))
countries = dplyr::mutate_if(countrymaps, is.character, .funs = function(x){return(`Encoding<-`(x, "utf8"))}) %>% 
  dplyr::select(c(woe_name, new_names, geometry, country, latitude, longitude)) %>% na.omit


compiled = agg %>% filter(event_date >= Sys.Date()-90) %>% 
  group_by(country,admin1, event_type) %>% 
  summarize(Number_Events = n()) %>%
  na.omit()
countries = countries %>% rename(Area = woe_name) %>% select(-country)
compiled = compiled %>% 
  stringdist_right_join(countries, by = c(admin1 = "Area", country = "new_names"), max_dist = 1)
compiled$event_type[is.na(compiled$event_type)] = ""
compiled$Number_Events[is.na(compiled$Number_Events)] = 0
compiled = compiled %>% ungroup() %>% select(-c(country, admin1))
compiled = compiled %>% group_by(new_names, Area, latitude, longitude, event_type) %>%
  summarize(Number_Events = sum(Number_Events)
  ) %>% merge(countries) %>% distinct()

compiled = st_as_sf(compiled)

if (require("utils")) {
  s3write_using(compiled, FUN = saveRDS, object = "Compiled_Events_Type_SF.RDS", bucket = "cdwbucket/AubreyTestData")
}


compiled = agg %>% filter(event_date >= Sys.Date()-60) %>% 
  group_by(country,admin1) %>% 
  summarize(Number_Events = n(), goldstein = mean(goldstein, na.rm= TRUE)) %>%
  na.omit()
countries = countries %>% rename(Area = woe_name) %>% select(-country)
compiled = compiled %>% 
  stringdist_right_join(countries, by = c(admin1 = "Area", country = "new_names"), max_dist = 3)
compiled$goldstein[is.na(compiled$goldstein)] = 0
compiled$Number_Events[is.na(compiled$Number_Events)] = 0
compiled = compiled %>% ungroup() %>% select(-c(country, admin1))
compiled = compiled %>% group_by(new_names, Area, latitude, longitude) %>%
  summarize(goldstein = weighted.mean(goldstein, Number_Events),
            Number_Events = sum(Number_Events)
  ) %>% merge(countries) %>% distinct()
compiled$Event_Tone = round(rescale(compiled$goldstein*(log(1+compiled$Number_Events)), to = c(0, 10)), 2)
compiled = st_as_sf(compiled)

if (require("utils")) {
  s3write_using(compiled, FUN = saveRDS, object = "Compiled_Events_SF.RDS", bucket = "cdwbucket/AubreyTestData")
}
