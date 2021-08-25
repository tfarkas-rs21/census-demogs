library(bigrquery)
library(dbplyr)

# BQ creds and authorization
bq_auth(path =  "~/.ssh/veraset-prototype-b9b60a42ffc6.json")
proj <- "veraset-prototype"

# test 
sql <- "select * from veraset-prototype.geofacts.detailed_estimates_2019_5yr limit 100"
tb <- bq_project_query(proj, sql)

# establish connection to geofacts
geofacts <- dbConnect(
  bigrquery::bigquery(),
  project = "veraset-prototype",
  dataset = "geofacts",
  billing = proj
)

dbListTables(geofacts) # test connection, look at tables

# create objects for focal tables
ests <- tbl(geofacts, "detailed_estimates_2019_5yr") # 2019 detailed census estimates
geos <- tbl(geofacts, "geographies") # all geographies

# pums joint distributions, augmented with household demography
pums <- tbl(geofacts, "pums_augmented_demography") 

# pull all estimates for focal tables only into memory, tracts only
# TODO: don't do this? establish connections independently on demand
est_df_raw <- ests %>%
  filter(str_sub(concept_id, start = 1, end = 7) %in% 
           c("B19037A"),
         str_sub(geoid, 1, 2) == "14") %>%
  
  collect()

# chunk up download and save to data frame
est_df_raw <- c(paste0("B01001", LETTERS[1:9]), 
                paste0("B19037", LETTERS[1:9]),  
                "B03002_") %>% 
  map_dfr(~ {
    ests %>%
      filter(str_sub(concept_id, start = 1, end = 7) == .x,
             str_sub(geoid, 1, 2) == "14") %>%
      collect()
  })

est_df_B19037 <- ests %>%
  filter(str_sub(concept_id, start = 1, end = 7) == "B19037_",
         str_sub(geoid, 1, 2) == "14") %>%
  collect()

est_df_B01001 <- ests %>%
  filter(str_sub(concept_id, start = 1, end = 7) == "B01001_",
         str_sub(geoid, 1, 2) == "14") %>%
  collect()

save(est_df_raw, file = "~/projects/mothr/mobility/demographics/tract_tables_df.RData")

# load data frame
load("~/projects/mothr/mobility/demographics/tract_tables_df.RData")

# tracts only
tract_ests <- est_df_raw %>%
  # join in others i forgot
  bind_rows(est_df_B19037, est_df_B01001) %>%
  
  # pull state, county, and tract IDs from GEOID column
  mutate(state = str_sub(geoid, 10, 11),
         county = str_sub(geoid, 12, 14),
         tract = str_sub(geoid, 15, 20)) %>%
  
  # join in geography data to get enclosing PUMA 
  left_join(geos %>% 
              filter(summary_level == "TRACT") %>%
              select(state, county, puma, tract) %>%
              collect(), 
            by = c("state", "county", "tract")) 

# split data into list by tract, for passing to map()
tract_list <- tract_ests %>%
  mutate(across(value, ~ as.integer(.x))) %>%
  group_by(geoid) %>%
  group_split

save(tract_list, file = "~/projects/mothr/mobility/demographics/tract_tables_list.RData")


## PUMS data ##
## all the person-level PUMS data
## not working can't DL this big data from BQ all of a sudden. Use file DL from S3
state_fips <- pums %>%
  distinct(state) %>% collect()
state_fips <- pull(state_fips, state)

pums_prsn_df <- map_dfr(state_fips, ~{
  #print(.x)
  possibly(pums %>%
             filter(state == .x) %>%
             arrange(ethnicity, race, age, income, ethnicity_hh, age_hh, race_hh) %>%
             collect()
           })
  
  pums_prsn_df <- read_csv("~/projects/mothr/mobility/census-demogs/data/2019_pums.csv")
  
  pums_prsn_df <- pums_prsn_df %>%
    mutate(across(PUMS, ~str_pad(as.character(as.numeric(.x)), 5, pad = "0"))) %>%
    select(state = FIPS, puma = PUMS, ethnicity:count)
  
  save(pums_prsn_df, file = "~/projects/mothr/mobility/census-demogs/data/pums_prsn.RData")
  
  # pums_hh <- read_csv("~/projects/mothr/mobility/PUMS/state_household.csv") %>%
  #   mutate(across(PUMA, ~ str_pad(as.character(.x), 5, side = "left", pad = "0")), 
  #          across(ST, ~ str_pad(as.character(.x), 2, side = "left", pad = "0")))
  # save(pums_hh, file = "~/projects/mothr/mobility/census-demogs/data/pums_hhld.RData")