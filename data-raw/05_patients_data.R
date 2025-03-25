source(here::here("data-raw", "01_data_setup.R"))
source(here::here("data-raw", "03_popn_projections.R"))

pt <- "patient_id"
eb <- "estd_birth_month"
up <- c(
  "icb_uniq_px",
  "icb_uniq_px_by_tt",
  "icb_uniq_px_by_cat",
  "uniq_patients"
)

# Initial data join and collect ------------------------------------------

# https://www.datadictionary.nhs.uk/data_elements/attendance_status.html
# 5 - attended on time
# 6 - attended late but was seen
# 7 - attended late and was not seen
# 2 - patient cancelled
# 3 - DNA (without notice)
# 4 - provider cancelled or postponed
patient_count_init <- csds_data_full_valid |>
  dplyr::select(
    "icb22cdh",
    "icb22nm",
    patient_id = "Person_ID",
    contact_date = "Contact_Date",
    age_int = "AgeFirstContact2223",
    gender_cat = "Gender",
    lsoa11cd = "Der_Postcode_yr2011_LSOA",
    submitter_id = "OrgID_Provider",
    attendance_status = "AttendanceOutcomeSU",
    team_type = "TeamTypeDescription",
    fin_year = "Der_Financial_Year"
  ) |>
  dplyr::filter(!dplyr::if_any("patient_id", is.na)) |>
  # Very rough guess at when birthday might fall - earliest contact date
  # within each recorded year of age. (Including 2021-22 data to improve guess).
  dplyr::mutate(
    estd_birth_month = min(.data[["contact_date"]], na.rm = TRUE),
    .by = c("patient_id", "age_int")
  ) |>
  # Within each financial year there may be two options from the above
  # (depending on how contacts are spread) - if so, take the later one!
  dplyr::mutate(
    across("estd_birth_month", \(x) lubridate::month(max(x, na.rm = TRUE))),
    .by = c("patient_id", "fin_year")
  ) |>
  # Within each patient's data there may be two options from the above - one per
  # FY - if so, take the earlier one (should be closer to actual birthday).
  dplyr::mutate(across("estd_birth_month", min), .by = "patient_id") |>
  dplyr::filter(dplyr::if_any("fin_year", \(x) x == "2022/23")) |>
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
  dplyr::left_join(lsoa11_lad18_lookup_eng, "lsoa11cd") |>
  dplyr::count(
    dplyr::pick(tidyselect::all_of(c(icb_cols, dq_cols, tt, pt, eb, join_cols)))
  ) |>
  dplyr::collect() |>
  dplyr::arrange(dplyr::pick(c("patient_id", "age_int", "lad18cd"))) |>
  dplyr::mutate(across("age_int", as.integer), across("gender_cat", as.factor))


# Saving to parquet at this point (once collected) because it's a lot of data
# and you don't want to have to generate it again if you can help it.
patient_count_init |>
  arrow::arrow_table() |>
  arrow::write_dataset("patient_count")
patient_count_init <- arrow::read_parquet("patient_count/part-0.parquet")


# Data quality summaries -------------------------------------------------

create_nat_patient_summary <- function(patient_count_data) {
  all_unfiltd <- patient_count_data |>
    dplyr::summarise(all_unfiltd = dplyr::n_distinct(.data[["patient_id"]]))

  missing_icb <- patient_count_data |>
    dplyr::filter(all(is.na(.data[["icb22cdh"]])), .by = "patient_id") |>
    dplyr::summarise(missing_icb = dplyr::n_distinct(.data[["patient_id"]]))

  inconsistnt <- patient_count_data |>
    dplyr::filter(!any(.data[["consistent"]]), .by = "patient_id") |>
    dplyr::summarise(inconsistnt = dplyr::n_distinct(.data[["patient_id"]]))

  not_attnded <- patient_count_data |>
    dplyr::filter(
      !any(.data[["attendance_status"]] == "Attended"),
      .by = "patient_id"
    ) |>
    dplyr::summarise(not_attnded = dplyr::n_distinct(.data[["patient_id"]]))

  missing_age <- patient_count_data |>
    dplyr::filter(all(is.na(.data[["age_int"]])), .by = "patient_id") |>
    dplyr::summarise(missing_age = dplyr::n_distinct(.data[["patient_id"]]))

  missing_gen <- patient_count_data |>
    dplyr::filter(all(is.na(.data[["gender_cat"]])), .by = "patient_id") |>
    dplyr::summarise(missing_gen = dplyr::n_distinct(.data[["patient_id"]]))

  final_count <- patient_count_data |>
    dplyr::filter(
      .data[["consistent"]] & .data[["attendance_status"]] == "Attended"
    ) |>
    dplyr::filter(
      any(!is.na(.data[["age_int"]])) &
        any(!is.na(.data[["gender_cat"]])) &
        any(!is.na(.data[["lad18cd"]])),
      .by = "patient_id"
    ) |>
    dplyr::summarise(final_count = dplyr::n_distinct(.data[["patient_id"]]))

  final_by_tt <- patient_count_data |>
    dplyr::filter(
      .data[["consistent"]] & .data[["attendance_status"]] == "Attended"
    ) |>
    dplyr::filter(
      any(!is.na(.data[["age_int"]])) & any(!is.na(.data[["gender_cat"]])),
      .by = "patient_id"
    ) |>
    dplyr::summarise(
      final_by_tt = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "team_type"
    )
  list(
    all_unfiltd,
    missing_icb,
    inconsistnt,
    not_attnded,
    missing_age,
    missing_gen,
    final_count,
    final_by_tt
  ) |>
    dplyr::bind_cols() |>
    dplyr::mutate(
      total_excld = .data[["all_unfiltd"]] - .data[["final_count"]],
      .before = "final_count"
    ) |>
    tidyr::nest(team_type_data = c("team_type", "final_by_tt")) |>
    dplyr::rename_with(\(x) paste0("nat_patients_", x))
}
nat_patient_dq_summary <- create_nat_patient_summary(patient_count_init)
readr::write_rds(nat_patient_dq_summary, "nat_patient_dq_summary.rds")


create_icb_patient_summary <- function(patient_count_data) {
  all_unfiltd <- patient_count_data |>
    dplyr::summarise(
      all_unfiltd = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  inconsistnt <- patient_count_data |>
    dplyr::filter(!any(.data[["consistent"]]), .by = "patient_id") |>
    dplyr::summarise(
      inconsistnt = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  not_attnded <- patient_count_data |>
    dplyr::filter(
      !any(.data[["attendance_status"]] == "Attended"),
      .by = "patient_id"
    ) |>
    dplyr::summarise(
      not_attnded = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  missing_age <- patient_count_data |>
    dplyr::filter(all(is.na(.data[["age_int"]])), .by = "patient_id") |>
    dplyr::summarise(
      missing_age = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  missing_gen <- patient_count_data |>
    dplyr::filter(all(is.na(.data[["gender_cat"]])), .by = "patient_id") |>
    dplyr::summarise(
      missing_gen = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  final_count <- patient_count_data |>
    dplyr::filter(
      .data[["consistent"]] & .data[["attendance_status"]] == "Attended"
    ) |>
    dplyr::filter(
      any(!is.na(.data[["age_int"]])) & any(!is.na(.data[["gender_cat"]])),
      .by = "patient_id"
    ) |>
    dplyr::summarise(
      final_count = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  final_by_tt <- patient_count_data |>
    dplyr::filter(
      .data[["consistent"]] & .data[["attendance_status"]] == "Attended"
    ) |>
    dplyr::filter(
      any(!is.na(.data[["age_int"]])) & any(!is.na(.data[["gender_cat"]])),
      .by = "patient_id"
    ) |>
    dplyr::summarise(
      final_by_tt = dplyr::n_distinct(.data[["patient_id"]]),
      .by = c("icb22cdh", "team_type")
    )
  list(
    all_unfiltd,
    inconsistnt,
    not_attnded,
    missing_age,
    missing_gen,
    final_count,
    final_by_tt
  ) |>
    purrr::reduce(\(x, y) dplyr::left_join(x, y, "icb22cdh")) |>
    dplyr::mutate(
      dplyr::across(tidyselect::where(is.numeric), \(x) {
        tidyr::replace_na(x, 0L)
      })
    ) |>
    dplyr::mutate(
      total_excld = .data[["all_unfiltd"]] - .data[["final_count"]],
      .before = "final_count"
    ) |>
    tidyr::nest(team_type_data = c("team_type", "final_by_tt")) |>
    dplyr::rename_with(\(x) paste0("icb_patients_", x), !"icb22cdh")
}
icb_patient_dq_summary <- patient_count_init |>
  dplyr::filter(!dplyr::if_any("icb22cdh", is.na)) |>
  create_icb_patient_summary()
readr::write_rds(icb_patient_dq_summary, "icb_patient_dq_summary.rds")


# Create patient count by ICB and team type ------------------------------

patient_count_filt <- patient_count_init |>
  # This step just gets rid of NAs. Values filled may be superseded by next
  # line. It reduces NAs in the data from 172k to 17k.
  tidyr::fill("age_int", .by = "patient_id", .direction = "downup") |>
  # Establish a single canonical age for each patient (if we guess their
  # birthday falls between April and September then we take their higher
  # recorded age in the dataset; if we think it falls between October and March
  # we take their lower recorded age in the dataset. This is in order to match
  # a mid-year ("as at October 1st") date for the population projection.)
  # Then fill that age across all rows for that patient (including overwriting
  # any existing other recorded age... there should be a maximum of 2 ages!)
  dplyr::mutate(
    dplyr::across("age_int", \(x) {
      dplyr::if_else(
        any(.data[["estd_birth_month"]] %in% seq(4, 9)),
        max(x),
        min(x)
      )
    }),
    .by = "patient_id",
    .keep = "unused"
  ) |>
  tidyr::fill(
    c("icb22cdh", "icb22nm", "gender_cat", "lad18cd"),
    .by = "patient_id",
    .direction = "downup"
  ) |>
  dplyr::filter(
    dplyr::if_any("consistent") &
      dplyr::if_any("attendance_status", \(x) x == "Attended") &
      dplyr::if_any("gender_cat", \(x) x %in% c("Male", "Female")) &
      dplyr::if_any("age_int", \(x) !is.na(x)) &
      dplyr::if_any("icb22cdh", \(x) !is.na(x))
  ) |>
  dplyr::select(!tidyselect::any_of(dq_cols))

csds_icb_patient_count <- patient_count_filt |>
  dplyr::mutate(
    total_pat_n = sum(.data[["n"]]),
    .by = tidyselect::all_of(c(icb_cols, pt))
  ) |>
  dplyr::mutate(
    pat_n_by_lad = sum(.data[["n"]]),
    .by = tidyselect::all_of(c(icb_cols, "lad18cd", pt)),
    .keep = "unused"
  ) |>
  dplyr::mutate(
    patient_lad_prop = .data[["pat_n_by_lad"]] / .data[["total_pat_n"]],
    .by = tidyselect::all_of(c(icb_cols, "lad18cd", pt)),
    .keep = "unused"
  ) |>
  dplyr::mutate(
    icb_uniq_patients = dplyr::n_distinct(.data[["patient_id"]]),
    .by = tidyselect::all_of(icb_cols)
  ) |>
  # dplyr::mutate(
  #   icb_uniq_patients_lad = dplyr::n_distinct(.data[["patient_id"]]),
  #   .by = tidyselect::all_of(c(icb_cols, join_cols))
  # ) |>
  # dplyr::mutate(
  #   icb_uniq_patients_tt = dplyr::n_distinct(.data[["patient_id"]]),
  #   .by = tidyselect::all_of(c(icb_cols, tt)),
  #   .keep = "unused"
  # ) |>
  dplyr::mutate(
    patients = sum(.data[["patient_lad_prop"]]),
    .by = tidyselect::all_of(c(icb_cols, join_cols))
  ) |>
  dplyr::mutate(
    patients_by_lad_tt = sum(.data[["patient_lad_prop"]]),
    .by = tidyselect::all_of(c(icb_cols, join_cols, tt)),
    .keep = "unused"
  ) |>
  dplyr::distinct() |>
  dplyr::mutate(
    lad18cd_tt = .data[["lad18cd"]],
    age_int_tt = .data[["age_int"]],
    gender_cat_tt = .data[["gender_cat"]]
  ) |>
  tidyr::nest(team_type_data = c("team_type", tidyselect::ends_with("tt"))) |>
  dplyr::mutate(
    across("team_type_data", \(x) {
      purrr::map(x, \(x) dplyr::rename_with(x, \(x) sub("_tt$", "", x)))
    }),
    across("team_type_data", \(x) {
      purrr::map(x, \(x) dplyr::rename(x, patients = "patients_by_lad"))
    })
  ) |>
  dplyr::arrange(dplyr::pick(tidyselect::all_of(c(icb_cols, join_cols)))) |>
  tidyr::nest(.by = tidyselect::all_of(c(icb_cols, "icb_uniq_patients"))) |>
  # dplyr::mutate(
  #   across("data", \(x) {
  #     purrr::map(x, \(x) {
  #       dplyr::rename(x, uniq_patients = "icb_uniq_patients_lad")
  #     })
  #   })
  # ) |>
  readr::write_rds("csds_icb_patient_count.rds")


# Calculate projected community patients ----------------------------------

join_popn_proj_data <- function(dat, pfp = popn_fy_projected) {
  join_cols <- c("lad18cd", "age_int", "gender_cat")

  projected_patients_by_fy <- dat |>
    dplyr::left_join(pfp, join_cols, relationship = "many-to-many") |>
    dplyr::mutate(
      # proj_uniq_patients = .data[["uniq_patients"]] * .data[["growth_coeff"]],
      projected_patients = .data[["patients"]] * .data[["growth_coeff"]],
      .keep = "unused"
    )

  fy_age_summary <- projected_patients_by_fy |>
    dplyr::summarise(
      across("fin_year_popn", \(x) sum(unique(x))),
      .by = c("fin_year", "age_int")
    ) |>
    dplyr::rename(proj_popn_by_fy_age = "fin_year_popn")

  projected_patients_by_fy |>
    dplyr::summarise(
      across("projected_patients", sum),
      .by = c("fin_year", "age_int")
    ) |>
    dplyr::left_join(fy_age_summary, c("fin_year", "age_int")) |>
    dplyr::arrange(dplyr::pick(c("fin_year", "age_int")))
}


icb_patient_dq_summary <- readr::read_rds("icb_patient_dq_summary.rds")

icb_patient_data <- readr::read_rds("csds_icb_patient_count.rds") |>
  tidyr::unnest("data")
icb_patient_data_tt <- icb_patient_data |>
  dplyr::mutate(across("data", \(x) {
    purrr::map(x, \(x) dplyr::pull(x, "team_type_data"))
  }))
icb_patient_data <- icb_patient_data |>
  dplyr::mutate(across("data", \(x) {
    purrr::map(x, \(x) dplyr::select(x, !"team_type_data"))
  }))


icb_projected_patients_fy <- readr::read_rds("csds_icb_patient_count.rds") |>
  tidyr::nest(.by = tidyselect::all_of(c(icb_cols, up))) |>
  dplyr::mutate(across("data", \(x) purrr::map(x, join_popn_proj_data))) |>
  dplyr::left_join(icb_patient_dq_summary, icb_cols) |>
  dplyr::relocate("data", .after = dplyr::last_col()) |>
  readr::write_rds("icb_projected_patients_fy.rds")


nat_patient_dq_summary <- readr::read_rds("nat_patient_dq_summary.rds")
nat_projected_patients_fy <- readr::read_rds("csds_icb_patient_count.rds") |>
  dplyr::summarise(
    across("patients", sum),
    .by = tidyselect::all_of(c("team_type", join_cols))
  ) |>
  join_popn_proj_data() |>
  dplyr::bind_cols(nat_patient_dq_summary) |>
  tidyr::nest(.by = tidyselect::starts_with("nat_patients")) |>
  readr::write_rds("nat_projected_patients_fy.rds")
