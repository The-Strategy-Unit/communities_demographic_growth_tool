# Helper functions --------------------------------------------------------

read_in_jg_data <- function(table_name) {
  sconn::sc() |>
    dplyr::tbl(dbplyr::in_catalog("strategyunit", "csds_jg", table_name))
}
read_in_reference_data <- function(table_name) {
  sconn::sc() |>
    dplyr::tbl(dbplyr::in_catalog("su_data", "reference", table_name))
}


# Read in data tables -----------------------------------------------------

# CSDS data as prepared by JG
# Contains 191mn rows
# Data for each care contact from 2021-04-01 to 2023-03-31
csds_data_full_valid <- read_in_jg_data("full_contact_based_valid")
lsoa11_lad18_lookup_eng <- read_in_reference_data("lsoa11_lad18_lookup_eng")
consistent_submitters <- here::here("consistent_submitters_2022_23.csv") |>
  readr::read_csv(col_types = "c") |>
  dplyr::pull("provider_org_id")

icb_cols <- c("icb22cdh", "icb22nm")
dq_cols <- c("consistent", "attendance_status")
join_cols <- c("lad18cd", "age_int", "gender_cat")


# 111,079 rows
csds_contacts_2022_23_icb_summary <- csds_data_full_valid |>
  dplyr::filter(dplyr::if_any("Der_Financial_Year", \(x) x == "2022/23")) |>
  dplyr::rename(
    age_int = "AgeYr_Contact_Date",
    gender_cat = "Gender",
    lsoa11cd = "Der_Postcode_yr2011_LSOA",
    contact_date = "Contact_Date",
    submitter_id = "OrgID_Provider",
    attendance_status = "AttendanceOutcomeSU"
  ) |>
  dplyr::left_join(lsoa11_lad18_lookup_eng, "lsoa11cd") |>
  dplyr::mutate(
    consistent = (submitter_id %in% {{ consistent_submitters }}),
    dplyr::across("age_int", \(x) dplyr::if_else(x > 90L, 90L, x)),
    .keep = "unused"
  ) |>
  dplyr::count(
    dplyr::pick(tidyselect::all_of(c(icb_cols, dq_cols, join_cols))),
    name = "contacts"
  ) |>
  dplyr::collect() |>
  dplyr::mutate(dplyr::across(c("age_int", "contacts"), as.integer)) |>
  readr::write_rds("csds_contacts_icb_summary.rds")


# Population projections data ---------------------------------------------

pps_folder <- "/Volumes/su_data/nhp/population-projections/demographic_data/"
ppp_location <- paste0(pps_folder, "projection=principal_proj")
popn_proj_orig <- sconn::sc() |>
  sparklyr::spark_read_parquet("popn_proj_data", ppp_location)

# 1,305,304 rows
popn_proj_tidy <- popn_proj_orig |>
  dplyr::filter(dplyr::if_any("year", \(x) x >= 2022L)) |>
  dplyr::select(c(
    cal_yr = "year",
    lad18cd = "area_code",
    age_int = "age",
    gender_cat = "sex",
    "value"
  )) |>
  dplyr::collect() |>
  dplyr::mutate(
    dplyr::across("gender_cat", \(x) dplyr::if_else(x == 1L, "Male", "Female")),
    dplyr::across("gender_cat", as.factor),
    dplyr::across(c("age_int", "cal_yr"), as.integer),
    dplyr::across("value", as.numeric)
  ) |>
  # This arrange() step is crucial for the calculation of growth coefficients
  # in future steps, using lead() and first().
  dplyr::arrange(dplyr::pick("cal_yr")) |>
  readr::write_rds("popn_proj_tidy.rds")


# Calculate growth coefficients per financial year ------------------------

# 1,245,972 rows
popn_fy_projected <- readr::read_rds("popn_proj_tidy.rds") |>
  dplyr::mutate(
    fin_year = paste0(
      .data[["cal_yr"]],
      "_",
      dplyr::lead(.data[["cal_yr"]]) %% 100
    ),
    fin_year_popn = (.data[["value"]] * 0.75) +
      (dplyr::lead(.data[["value"]]) * 0.25),
    growth_coeff = .data[["fin_year_popn"]] /
      dplyr::first(.data[["fin_year_popn"]]),
    .by = tidyselect::all_of(c("lad18cd", "age_int", "gender_cat")),
    .keep = "unused"
  ) |>
  dplyr::filter(!dplyr::if_any("fin_year_popn", is.na))


# Calculate projected community contacts ----------------------------------

join_popn_proj_data <- function(x, y = popn_fy_projected) {
  x |>
    dplyr::left_join(y, join_cols) |>
    dplyr::mutate(
      projected_contacts = .data[["contacts"]] * .data[["growth_coeff"]],
      .keep = "unused"
    ) |>
    dplyr::select(!c("lad18cd")) # possibly drop gender_cat also?
}


contacts_fy_projected_icb <- readr::read_rds("csds_contacts_icb_summary.rds") |>
  # Nesting creates a list-col "data", with a single tibble per row (per ICB)
  tidyr::nest(.by = tidyselect::all_of(c(icb_cols, dq_cols))) |>
  dplyr::mutate(across("data", \(x) purrr::map(x, join_popn_proj_data))) |>
  tidyr::unnest(cols = data)

usethis::use_data(contacts_fy_projected_icb, compress = "xz")
