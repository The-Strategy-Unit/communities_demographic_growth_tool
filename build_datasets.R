
# Load libraries ----------------------------------------------------------


# Optionally run this right away to spin up the compute (it can take a while!)
sconn::sc()



# Data prep ---------------------------------------------------------------

# Just for completeness / lineage: data now uploaded to databricks.

# LSOA:LAD lookup ---------------------------------------------------------

# https://github.com/francisbarton/boundr
lsoa11_lad18_lookup_eng <- boundr::lookup("lsoa", "lad", within_year = 2018) |>
  dplyr::filter(dplyr::if_any("lsoa11cd", \(x) grepl("^E", x)))

# TODO: If I left this unfiltered, we should get a load of Welsh LAs being
# picked up. We could then use these to impute (best guess), based on provider,
# which nearest/most appropriate English LA we could re-allocate them to.
# Or we get population projections for Welsh LAs as well (not currently
# included in ppp dataset available in databricks).





# Helper functions --------------------------------------------------------




read_in_jg_data <- function(table_name) {
  dbplyr::in_catalog("strategyunit", "csds_jg", table_name) |>
    dplyr::tbl(src = sconn::sc())
}
read_in_reference_data <- function(table_name) {
  dbplyr::in_catalog("su_data", "reference", table_name) |>
    dplyr::tbl(src = sconn::sc())
}



# This function was only going to be useful if we needed to summarise to
# PatientID; but now that is out of scope.
# commonest <- \(vec) vctrs::vec_count(vec)[["key"]][[1]]


# List of cols to count by, possibly needs refinement!
provider_count_cols <- \() c(
  "provider_org_id",
  "lad18cd",
  "lad18nm",
  "age_int",
  "gender_cat"
  ## Other columns we might want to bring in later:
  # "TeamType",
  # "TeamTypeDescription",
  # "ConsultationMediumSU",
  # "AttendanceOutcomeSU",
)

icb_count_cols <- \() c(
  "icb22cdh",
  "icb22nm",
  "lad18cd",
  "lad18nm",
  "age_int",
  "gender_cat"
)




# Use LSOA data from NHSE to allocate CSDS contacts to 2018 local authorities
# (function currently unused below).
match_2011_lsoas_to_2018_lads <- function(.data) {
  lookup_tbl <- read_in_reference_data("lsoa11_lad18_lookup_eng") |>
    dplyr::select(!"lsoa11nm")
  .data |>
    dplyr::left_join(lookup_tbl, "lsoa11cd")
}


# Summarise CSDS contacts table (function currently unused below)
count_fin_year_care_contacts <- function(.data, fin_year_start) {
  fys <- as.Date(paste0(as.integer(fin_year_start), "-04-01"))
  fye <- as.Date(paste0(as.integer(fin_year_start) + 1L, "-03-31"))

  .data |>
    dplyr::filter(
      if_any("contact_date", \(x) dplyr::between(x, {{ fys }}, {{ fye }}))
    ) |>
    dplyr::mutate(dplyr::across("age_int", \(x) dplyr::if_else(x > 90L, 90L, x))) |>
    dplyr::count(dplyr::pick(all_of(count_cols())), name = "contacts")
}


# Read in data tables -----------------------------------------------------


# CSDS data as prepared by JG
# Contains 191mn rows
# Data for each care contact from 2021-04-01 to 2023-03-31
csds_data_full_valid <- read_in_jg_data("full_contact_based_valid")
lsoa11_lad18_lookup_eng <- read_in_reference_data("lsoa11_lad18_lookup_eng") |>
  dplyr::select(!"lsoa11nm")

# 327,140 rows (by provider and LA)
csds_contacts_2022_23_provider_summary <- csds_data_full_valid |>
  dplyr::filter(dplyr::if_any("Der_Financial_Year", \(x) x == "2022/23")) |>
  dplyr::rename(
    provider_org_id = "OrgID_Provider",
    age_int = "AgeYr_Contact_Date",
    gender_cat = "Gender",
    lsoa11cd = "Der_Postcode_yr2011_LSOA",
    contact_date = "Contact_Date"
  ) |>
  dplyr::left_join(lsoa11_lad18_lookup_eng, "lsoa11cd") |>
  dplyr::mutate(dplyr::across("age_int", \(x) dplyr::if_else(x > 90L, 90L, x))) |>
  dplyr::count(dplyr::pick(tidyselect::all_of(provider_count_cols())), name = "contacts") |>
  dplyr::collect() |>
  dplyr::mutate(dplyr::across(c("age_int", "contacts"), as.integer))

csds_contacts_2022_23_provider_summary |>
  saveRDS("csds_contacts_provider_summary.rds")


# 327,140 rows (by provider and ICB)
csds_contacts_2022_23_icb_summary <- csds_data_full_valid |>
  dplyr::filter(dplyr::if_any("Der_Financial_Year", \(x) x == "2022/23")) |>
  dplyr::rename(
    age_int = "AgeYr_Contact_Date",
    gender_cat = "Gender",
    lsoa11cd = "Der_Postcode_yr2011_LSOA",
    contact_date = "Contact_Date"
  ) |>
  dplyr::left_join(lsoa11_lad18_lookup_eng, "lsoa11cd") |>
  dplyr::mutate(dplyr::across("age_int", \(x) if_else(x > 90L, 90L, x))) |>
  dplyr::count(dplyr::pick(tidyselect::all_of(icb_count_cols())), name = "contacts") |>
  dplyr::collect() |>
  dplyr::mutate(dplyr::across(c("age_int", "contacts"), as.integer))

csds_contacts_2022_23_icb_summary |>
  saveRDS("csds_contacts_2022_23_icb_summary.rds")




# Population projections data ---------------------------------------------

pps_folder <- "/Volumes/su_data/nhp/population-projections/demographic_data/"
ppp_location <- paste0(pps_folder, "projection=principal_proj")
popn_proj_orig <- sconn::sc() |>
  sparklyr::spark_read_parquet("popn_proj_data", ppp_location)

# 1,305,304 rows
popn_proj_tidy <- popn_proj_orig |>
  dplyr::filter(if_any("year", \(x) x >= 2022L)) |>
  dplyr::select(c(
    cal_yr = "year",
    lad18cd = "area_code",
    age_int = "age",
    gender_cat = "sex",
    "value"
  )) |>
  dplyr::collect() |>
  dplyr::mutate(
    dplyr::across("gender_cat", \(x) if_else(x == 1L, "Male", "Female")),
    dplyr::across("gender_cat", as.factor),
    dplyr::across(c("age_int", "cal_yr"), as.integer),
    dplyr::across("value", as.numeric)
  ) |>
  dplyr::arrange(pick("cal_yr"))


saveRDS(popn_proj_tidy, "popn_proj_tidy.rds")
popn_proj_tidy <- readRDS("popn_proj_tidy.rds")
