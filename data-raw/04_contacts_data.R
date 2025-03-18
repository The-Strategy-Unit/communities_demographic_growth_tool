source(here::here("data-raw", "01_data_setup.R"))
source(here::here("data-raw", "03_popn_projections.R"))

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


# Calculate projected community contacts ----------------------------------

join_popn_proj_data <- function(dat, nat = FALSE, pfp = popn_fy_projected) {
  ncmi <- if (nat) "nat_contacts_missing_icb" else NULL
  join_cols <- c("lad18cd", "age_int", "gender_cat")
  sum_con <- \(x) sum(x[["contacts"]])
  all_icb_contacts <- sum_con(dat)
  missing_gen <- sum_con(dplyr::filter(dat, dplyr::if_any("gender_cat", is.na)))
  missing_age <- sum_con(dplyr::filter(dat, dplyr::if_any("age_int", is.na)))
  inconsistnt <- sum_con(dplyr::filter(dat, !dplyr::if_any("consistent")))
  not_attnded <- dat |>
    dplyr::filter(
      dplyr::if_any("attendance_status", \(x) x == "Did Not Attend")
    ) |>
    sum_con()
  canclld_unk <- dat |>
    dplyr::filter(
      dplyr::if_any("attendance_status", \(x) x %in% c("Cancelled", "Unknown"))
    ) |>
    sum_con()

  dat_filtered <- dat |>
    dplyr::filter(
      dplyr::if_all(tidyselect::all_of(join_cols), \(x) !is.na(x)) &
        dplyr::if_any("attendance_status", \(x) x == "Attended") &
        dplyr::if_any("consistent")
    ) |>
    dplyr::summarise(
      across("contacts", sum),
      .by = tidyselect::all_of(c(join_cols, ncmi, "team_type"))
    )
  icb_contacts_filt <- sum_con(dat_filtered)

  projected_contacts_by_fy <- dat_filtered |>
    dplyr::left_join(pfp, join_cols, relationship = "many-to-many") |>
    dplyr::mutate(
      projected_contacts = .data[["contacts"]] * .data[["growth_coeff"]],
      .keep = "unused"
    )

  fy_age_summary <- projected_contacts_by_fy |>
    dplyr::summarise(
      proj_contacts_by_fy_age = sum(.data[["projected_contacts"]]),
      .by = c(tidyselect::all_of(ncmi), "fin_year", "age_int")
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
      icb_contacts_not_attnded = not_attnded,
      icb_contacts_canclld_unk = canclld_unk,
      icb_contacts_total_excld = all_icb_contacts - icb_contacts_filt,
      icb_contacts_final_count = icb_contacts_filt,
      .before = "fin_year"
    ) |>
    dplyr::arrange(dplyr::pick(c("fin_year", "age_int")))
}

nat_popn_fy_projected <- readr::read_rds("nat_popn_fy_projected.rds")
nat_projected_contacts_fy <- readr::read_rds("csds_contacts_icb_summary.rds") |>
  dplyr::mutate(
    nat_contacts_missing_icb = sum(dplyr::if_else(
      is.na(.data[["icb22cdh"]]),
      .data[["contacts"]],
      0L
    ))
  ) |>
  join_popn_proj_data(nat = TRUE) |>
  dplyr::rename_with(\(x) sub("^icb", "nat", x)) |>
  dplyr::relocate("nat_contacts_missing_icb", .after = 1) |>
  tidyr::nest(.by = tidyselect::starts_with("nat_contacts")) |>
  dplyr::mutate(
    across("data", \(x) {
      purrr::map(x, \(x) {
        dplyr::left_join(x, nat_popn_fy_projected, c("fin_year", "age_int"))
      })
    })
  ) |>
  readr::write_rds("nat_projected_contacts_fy.rds")


hoist_cols <- c(
  "icb_contacts_all_unfiltd",
  "icb_contacts_missing_gen",
  "icb_contacts_missing_age",
  "icb_contacts_inconsistnt",
  "icb_contacts_not_attnded",
  "icb_contacts_canclld_unk",
  "icb_contacts_total_excld",
  "icb_contacts_final_count"
)

icb_popn_fy_projected <- readr::read_rds("icb_popn_fy_projected.rds")
icb_projected_contacts_fy <- readr::read_rds("csds_contacts_icb_summary.rds") |>
  dplyr::filter(!dplyr::if_any("icb22cdh", is.na)) |>
  tidyr::nest(.by = tidyselect::all_of(icb_cols)) |>
  dplyr::mutate(across("data", \(x) purrr::map(x, join_popn_proj_data))) |>
  tidyr::hoist("data", !!!hoist_cols, .transform = unique) |>
  dplyr::left_join(icb_popn_fy_projected, "icb22cdh") |>
  tidyr::nest(
    .by = tidyselect::all_of(c(icb_cols, hoist_cols, "data")),
    .key = "popn_data"
  ) |>
  dplyr::mutate(
    across("data", \(x) {
      purrr::map2(x, .data[["popn_data"]], \(x, y) {
        dplyr::left_join(x, y, c("fin_year", "age_int"))
      })
    })
  ) |>
  dplyr::select(!"popn_data")


usethis::use_data(
  nat_projected_contacts_fy,
  icb_projected_contacts_fy,
  compress = "xz"
)
