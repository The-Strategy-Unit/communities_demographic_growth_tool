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


# https://www.datadictionary.nhs.uk/data_elements/attendance_status.html
# 5 - attended on time
# 6 - attended late but was seen
# 7 - attended late and was not seen
# 2 - patient cancelled
# 3 - DNA (without notice)
# 4 - provider cancelled or postponed

# 456,992 rows
csds_contacts_2022_23_icb_summary <- csds_data_full_valid |>
  dplyr::filter(dplyr::if_any("Der_Financial_Year", \(x) x == "2022/23")) |>
  dplyr::select(
    "icb22cdh",
    "icb22nm",
    age_int = "AgeYr_Contact_Date",
    gender_cat = "Gender",
    lsoa11cd = "Der_Postcode_yr2011_LSOA",
    submitter_id = "OrgID_Provider",
    attendance_status = "AttendanceOutcomeSU",
    team_type = "TeamTypeDescription"
  ) |>
  dplyr::left_join(lsoa11_lad18_lookup_eng, "lsoa11cd") |>
  dplyr::mutate(
    consistent = (submitter_id %in% {{ consistent_submitters }}),
    dplyr::across("age_int", \(x) dplyr::if_else(x > 90L, 90L, x)),
    dplyr::across("gender_cat", \(x) dplyr::if_else(x == "Unknown", NA, x)),
    dplyr::across("attendance_status", \(x) {
      dplyr::case_when(
        x %in% c("5", "6") ~ "Attended",
        x %in% c("3", "7") ~ "Did Not Attend",
        x %in% c("2", "4") ~ "Cancelled",
        .default = "Unknown"
      )
    }),
    .keep = "unused"
  ) |>
  dplyr::count(
    dplyr::pick(tidyselect::all_of(c(icb_cols, dq_cols, tt, join_cols))),
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

join_popn_proj_data <- function(dat, y = popn_fy_projected) {
  join_cols <- c("lad18cd", "age_int", "gender_cat")
  sum_con <- \(x) sum(x[["contacts"]])
  all_icb_contacts <- sum_con(dat)
  missing_gen <- sum_con(dplyr::filter(dat, dplyr::if_any("gender_cat", is.na)))
  missing_age <- sum_con(dplyr::filter(dat, dplyr::if_any("age_int", is.na)))
  inconsistnt <- sum_con(dplyr::filter(dat, !dplyr::if_any("consistent")))
  not_attnded <- sum_con(dplyr::filter(
    dat,
    dplyr::if_any("attendance_status", \(x) x == "Did Not Attend")
  ))
  canclld_unk <- sum_con(dplyr::filter(
    dat,
    dplyr::if_any("attendance_status", \(x) x %in% c("Cancelled", "Unknown"))
  ))

  dat_filtered <- dat |>
    dplyr::filter(
      dplyr::if_all(tidyselect::all_of(join_cols), \(x) !is.na(x)) &
        dplyr::if_any("attendance_status", \(x) x == "Attended") &
        dplyr::if_any("consistent")
    ) |>
    dplyr::summarise(
      across("contacts", sum),
      .by = c(tidyselect::all_of(join_cols), "team_type")
    )
  icb_contacts_filt <- sum_con(dat_filtered)

  projected_contacts_by_fy <- dat_filtered |>
    dplyr::left_join(y, join_cols, relationship = "many-to-many") |>
    dplyr::mutate(
      projected_contacts = .data[["contacts"]] * .data[["growth_coeff"]],
      .keep = "unused"
    )

  fy_age_summary <- projected_contacts_by_fy |>
    dplyr::summarise(
      across(c("fin_year_popn", "projected_contacts"), sum),
      .by = c("fin_year", "age_int")
    ) |>
    dplyr::rename(
      icb_popn_by_fy_age = "fin_year_popn",
      icb_proj_contacts_by_fy_age = "projected_contacts"
    )

  projected_contacts_by_fy |>
    dplyr::summarise(
      across("projected_contacts", sum),
      .by = c("fin_year", "age_int", "team_type")
    ) |>
    dplyr::left_join(fy_age_summary, c("fin_year", "age_int")) |>
    dplyr::mutate(
      icb_contacts_all_unfiltd = all_icb_contacts,
      icb_contacts_missing_gen = missing_gen,
      icb_contacts_missing_age = missing_age,
      icb_contacts_inconsistnt = inconsistnt,
      icb_contacts_nt_attended = not_attnded,
      icb_contacts_canclld_unk = canclld_unk,
      icb_contacts_final_count = icb_contacts_filt,
      .before = 1
    )
}


contacts_fy_projected_icb <- readr::read_rds("csds_contacts_icb_summary.rds") |>
  tidyr::nest(.by = tidyselect::all_of(icb_cols)) |>
  dplyr::mutate(across("data", \(x) purrr::map(x, join_popn_proj_data))) |>
  tidyr::unnest(cols = data)

usethis::use_data(contacts_fy_projected_icb, internal = TRUE, compress = "xz")
