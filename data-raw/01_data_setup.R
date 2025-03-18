# Helper functions --------------------------------------------------------

read_in_jg_data <- function(table_name) {
  sconn::sc() |>
    dplyr::tbl(dbplyr::in_catalog("strategyunit", "csds_jg", table_name))
}
read_in_reference_data <- function(table_name) {
  sconn::sc() |>
    dplyr::tbl(dbplyr::in_catalog("strategyunit", "default", table_name))
}

# Read in data tables -----------------------------------------------------

# CSDS data as prepared by JG
# Contains 191mn rows
# Data for each care contact from 2021-04-01 to 2023-03-31
# 2022/23 data: 96,647,105 rows
csds_data_full_valid <- read_in_jg_data("full_contact_based_valid")
lsoa11_lad18_lookup_eng <- read_in_reference_data("lsoa11_lad18_lookup_eng")
consistent_submitters <- here::here("consistent_submitters_2022_23.csv") |>
  readr::read_csv(col_types = "c") |>
  dplyr::pull("provider_org_id")


icb_cols <- c("icb22cdh", "icb22nm")
dq_cols <- c("consistent", "attendance_status")
tt <- "team_type"
join_cols <- c("lad18cd", "age_int", "gender_cat")
