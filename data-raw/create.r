# Databricks notebook source
# MAGIC %md
# MAGIC ## setup

# COMMAND ----------

# DBTITLE 1,initial datasets
sc <- sparklyr::spark_connect(method = "databricks")

csds_data_init <- dplyr::tbl(
  sc,
  dbplyr::in_catalog("strategyunit", "csds_jg", "full_contact_based_valid")
) |>
  dplyr::select(c(
    fin_year = "Der_Financial_Year",
    "icb22cdh",
    "icb22nm",
    patient_id = "Person_ID",
    contact_date = "Contact_Date",
    lsoa11cd = "Der_Postcode_yr2011_LSOA",
    age_int = "AgeFirstContact2223",
    gender_cat = "Gender",
    submitter_id = "OrgID_Provider",
    attendance_cat = "AttendanceOutcomeSU",
    service = "TeamTypeDescription"
  ))
lsoa11_icb22_lookup <- dplyr::tbl(
  sc,
  dbplyr::in_catalog("strategyunit", "csds_fb", "lsoa11_icb22_lookup")
) |>
  dplyr::select(tidyselect::matches("(cd|cdh)$"))
lsoa11_lad18_lookup_eng <- dplyr::tbl(
  sc,
  dbplyr::in_catalog("strategyunit", "csds_fb", "lsoa11_lad18_lookup_eng")
) |>
  dplyr::select(tidyselect::matches("cd$"))
consistent_submitters <- dplyr::tbl(
  sc,
  dbplyr::in_catalog("strategyunit", "csds_fb", "consistent_submitters_2022_23")
) |>
  dplyr::pull("provider_org_id")


# COMMAND ----------

# DBTITLE 1,helper functions
db_vol <- "/Volumes/strategyunit/csds_fb/csds_patients"
db_write_rds <- \(x, f) readr::write_rds(x, glue::glue("{db_vol}/{f}"))
db_read_rds <- \(f) readr::read_rds(glue::glue("{db_vol}/{f}"))

# COMMAND ----------

# DBTITLE 1,reusable variables to refer to key columns
icb_cols <- c("icb22cdh", "icb22nm")
dq_cols <- c("consistent_ind", "attendance_cat")
sv <- "service"
lad <- "lad18cd"
join_cols <- c(lad, "age_int", "gender_cat")
count_cols <- c("fin_year", "age_int")
pt <- "patient_id"
ncp <- c("proj_uniq_px_by_fy_age", "proj_popn_by_fy_age", count_cols)
ncc <- c("proj_popn_by_fy_age", count_cols)

# COMMAND ----------

# DBTITLE 1,core function for growing patient and contact numbers
join_popn_proj_data <- function(dat, pfp = popn_fy_projected) {
  join_cols <- c("lad18cd", "age_int", "gender_cat")
  dat |>
    dplyr::left_join(pfp, join_cols, relationship = "many-to-many") |>
    dplyr::mutate(
      projected_count = .data[["count"]] * .data[["growth_coeff"]],
      .keep = "unused"
    ) |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = c("fin_year", "age_int")
    ) |>
    dplyr::arrange(dplyr::pick(c("fin_year", "age_int")))
}


# COMMAND ----------

# MAGIC %md
# MAGIC ## population projections and growth setup

# COMMAND ----------

# DBTITLE 1,initial population projections data
pps_folder <- "/Volumes/su_data/nhp/population-projections/demographic_data/"
ppp_location <- paste0(pps_folder, "projection=principal_proj")
popn_proj_orig <- sparklyr::spark_read_parquet(
  sc,
  "popn_proj_data",
  ppp_location
) |>
  dplyr::filter(dplyr::if_any("year", \(x) x >= 2022L)) |>
  dplyr::select(c(
    cal_yr = "year",
    lad18cd = "area_code",
    age_int = "age",
    gender_cat = "sex",
    "value"
  ))

# COMMAND ----------

# DBTITLE 1,population projections data ready to apply
popn_proj_tidy <- popn_proj_orig |>
  dplyr::mutate(
    dplyr::across("gender_cat", \(x) dplyr::if_else(x == 1L, "Male", "Female")),
    dplyr::across(c("age_int", "cal_yr"), as.integer),
    dplyr::across("value", as.numeric)
  ) |>
  dplyr::collect() |>
  # This arrange() step is crucial for the calculation of growth coefficients
  # in future steps, using lead() and first().
  dplyr::arrange(dplyr::pick("cal_yr"))

# COMMAND ----------

popn_fy_projected <- popn_proj_tidy |>
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

# COMMAND ----------

# DBTITLE 1,assign LADs (including partially) to ICBs
lad_proportion_by_icb <- lsoa11_icb22_lookup |>
  dplyr::left_join(lsoa11_lad18_lookup_eng, "lsoa11cd") |>
  dplyr::mutate(lad_n = dplyr::n(), .by = "lad18cd") |>
  dplyr::mutate(icb_lad_n = dplyr::n(), .by = c("lad18cd", "icb22cdh")) |>
  dplyr::collect() |>
  # Weighting LADs where they are split across >1 ICB (if completely contained
  # in a single ICB then value will be 1).
  # For example, if a LAD has 50% of its LSOAs within an ICB and 50% in
  # another, this will create 2 rows for that LAD in the data, each with value
  # 0.5. This split is therefore not a completely accurate split by population,
  # but using number of LSOAs as an approximation to this.
  dplyr::reframe(
    proportion = .data[["icb_lad_n"]] / .data[["lad_n"]],
    .by = c("icb22cdh", "lad18cd")
  ) |>
  dplyr::distinct()

# COMMAND ----------

icb_popn_fy_projected <- lad_proportion_by_icb |>
  dplyr::left_join(
    popn_fy_projected,
    "lad18cd",
    relationship = "many-to-many"
  ) |>
  dplyr::mutate(
    across("fin_year_popn", \(x) x * proportion),
    .keep = "unused"
  ) |>
  dplyr::summarise(
    proj_popn_by_fy_age = sum(.data[["fin_year_popn"]]),
    .by = c("icb22cdh", "fin_year", "age_int")
  )

nat_popn_fy_projected <- popn_fy_projected |>
  dplyr::summarise(
    proj_popn_by_fy_age = sum(.data[["fin_year_popn"]]),
    .by = c("fin_year", "age_int")
  )

# COMMAND ----------

db_write_rds(popn_fy_projected, "popn_fy_projected.rds")
db_write_rds(icb_popn_fy_projected, "icb_popn_fy_projected.rds")
db_write_rds(nat_popn_fy_projected, "nat_popn_fy_projected.rds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## initial data read in

# COMMAND ----------

# DBTITLE 1,create csds_data_tidy
csds_data_tidy <- csds_data_init |>
  dplyr::filter(!dplyr::if_any("patient_id", is.na)) |>
  dbplyr::window_order(dplyr::pick("contact_date")) |>
  # Very rough guess at when birthday might fall - earliest contact date
  # within each recorded year of age. (Including 2021-22 data to improve guess).
  dplyr::mutate(
    estd_birth_month = min(.data[["contact_date"]], na.rm = TRUE),
    .by = c("patient_id", "age_int"),
    .keep = "unused"
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
    consistent_ind = dplyr::if_else(
      .data[["submitter_id"]] %in% {{ consistent_submitters }},
      1L,
      0L
    ),
    dplyr::across("age_int", as.integer),
    dplyr::across("age_int", \(x) dplyr::if_else(x > 90L, 90L, x)),
    dplyr::across("gender_cat", \(x) dplyr::if_else(x == "Unknown", NA, x)),
    # https://www.datadictionary.nhs.uk/data_elements/attendance_status.html
    # 5 - attended on time
    # 6 - attended late but was seen
    # 7 - attended late and was not seen
    # 2 - patient cancelled
    # 3 - DNA (without notice)
    # 4 - provider cancelled or postponed
    dplyr::across("attendance_cat", \(x) {
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
  dplyr::select(!c("fin_year", "lsoa11cd")) |>
  dplyr::group_by(dplyr::pick("patient_id")) |>
  # This step just gets rid of NAs. Values filled may be superseded by next
  # line. It reduces NAs in the data from 172k to 17k.
  tidyr::fill("age_int", .direction = "downup") |>
  # Establish a single canonical age for each patient (if we guess their
  # birthday falls between April and September then we take their higher
  # recorded age in the dataset; if we think it falls between October and March
  # we take their lower recorded age in the dataset. This is in order to match
  # a mid-year ("as at October 1st") theoretical date for ONS population data.)
  # Then fill that age across all rows for that patient (including overwriting
  # any existing other recorded age... there should be a maximum of 2 ages!)
  dplyr::mutate(
    dplyr::across("age_int", \(x) {
      dplyr::if_else(
        any(dplyr::between(estd_birth_month, 4L, 9L)),
        max(x),
        min(x)
      )
    }),
    .keep = "unused"
  ) |>
  tidyr::fill(
    c("icb22cdh", "icb22nm", "gender_cat", "lad18cd"),
    .direction = "downup"
  ) |>
  dplyr::ungroup()

# COMMAND ----------

# DBTITLE 1,save csds_data_init to parquet
sparklyr::spark_write_parquet(
  csds_data_tidy,
  glue::glue("{db_vol}/csds_data_tidy"),
  mode = "overwrite"
)

# COMMAND ----------

csds_data_tidy <- sparklyr::spark_read_parquet(
  sc,
  "csds_data_tidy",
  glue::glue("{db_vol}/csds_data_tidy")
)

# COMMAND ----------

# DBTITLE 1,contacts count
contacts_count_init <- csds_data_tidy |>
  dplyr::count(
    dplyr::pick(tidyselect::all_of(c(icb_cols, dq_cols, sv, join_cols))),
    name = "count"
  )

# COMMAND ----------

# DBTITLE 1,patients count
patients_count_init <- csds_data_tidy |>
  dplyr::count(
    dplyr::pick(tidyselect::all_of(c(icb_cols, dq_cols, sv, pt, join_cols))),
    name = "count"
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## data quality summaries

# COMMAND ----------

# DBTITLE 1,national dq summary function (contacts)
create_nat_contacts_summary <- function(contacts_count_init) {
  all_unfiltd <- contacts_count_init |>
    dplyr::summarise(nat_contacts_all_unfiltd = sum(.data[["count"]]))

  missing_icb <- contacts_count_init |>
    dplyr::filter(is.na(.data[["icb22cdh"]])) |>
    dplyr::summarise(nat_contacts_missing_icb = sum(.data[["count"]]))

  inconsistnt <- contacts_count_init |>
    dplyr::filter(.data[["consistent_ind"]] == 0L) |>
    dplyr::summarise(nat_contacts_inconsistnt = sum(.data[["count"]]))

  not_attnded <- contacts_count_init |>
    dplyr::filter(.data[["attendance_cat"]] == "Did Not Attend") |>
    dplyr::summarise(nat_contacts_not_attnded = sum(.data[["count"]]))

  cancelled <- contacts_count_init |>
    dplyr::filter(.data[["attendance_cat"]] == "Cancelled") |>
    dplyr::summarise(nat_contacts_cancelled = sum(.data[["count"]]))

  app_unknown <- contacts_count_init |>
    dplyr::filter(.data[["attendance_cat"]] == "Unknown") |>
    dplyr::summarise(nat_contacts_app_unknown = sum(.data[["count"]]))

  missing_age <- contacts_count_init |>
    dplyr::filter(is.na(.data[["age_int"]])) |>
    dplyr::summarise(nat_contacts_missing_age = sum(.data[["count"]]))

  missing_gen <- contacts_count_init |>
    dplyr::filter(is.na(.data[["gender_cat"]])) |>
    dplyr::summarise(nat_contacts_missing_gen = sum(.data[["count"]]))

  final_count <- contacts_count_init |>
    dplyr::filter(
      .data[["consistent_ind"]] == 1L &
        .data[["attendance_cat"]] == "Attended" &
        dplyr::if_all(tidyselect::all_of(join_cols), \(x) !is.na(x))
    ) |>
    dplyr::summarise(nat_contacts_final_count = sum(.data[["count"]]))

  list(
    all_unfiltd,
    missing_icb,
    inconsistnt,
    not_attnded,
    cancelled,
    app_unknown,
    missing_age,
    missing_gen,
    final_count
  ) |>
    purrr::map(\(x) dplyr::mutate(x, national = "National", .before = 1)) |>
    purrr::reduce(\(x, y) dplyr::left_join(x, y, "national")) |>
    dplyr::select(!"national") |>
    dplyr::mutate(
      nat_contacts_total_excld = .data[["nat_contacts_all_unfiltd"]] -
        .data[["nat_contacts_final_count"]],
      .before = "nat_contacts_final_count"
    )
}

# COMMAND ----------

# DBTITLE 1,icb dq summary function (contacts)
create_icb_contacts_summary <- function(contacts_count_init) {
  contacts_count_init <- dplyr::filter(contacts_count_init, !is.na("icb22cdh"))
  all_unfiltd <- contacts_count_init |>
    dplyr::summarise(
      icb_contacts_all_unfiltd = sum(.data[["count"]]),
      .by = "icb22cdh"
    )

  inconsistnt <- contacts_count_init |>
    dplyr::filter(.data[["consistent_ind"]] == 0L) |>
    dplyr::summarise(
      icb_contacts_inconsistnt = sum(.data[["count"]]),
      .by = "icb22cdh"
    )

  not_attnded <- contacts_count_init |>
    dplyr::filter(.data[["attendance_cat"]] == "Did Not Attend") |>
    dplyr::summarise(
      icb_contacts_not_attnded = sum(.data[["count"]]),
      .by = "icb22cdh"
    )

  cancelled <- contacts_count_init |>
    dplyr::filter(.data[["attendance_cat"]] == "Cancelled") |>
    dplyr::summarise(
      icb_contacts_cancelled = sum(.data[["count"]]),
      .by = "icb22cdh"
    )

  app_unknown <- contacts_count_init |>
    dplyr::filter(.data[["attendance_cat"]] == "Unknown") |>
    dplyr::summarise(
      icb_contacts_app_unknown = sum(.data[["count"]]),
      .by = "icb22cdh"
    )

  missing_age <- contacts_count_init |>
    dplyr::filter(is.na(.data[["age_int"]])) |>
    dplyr::summarise(
      icb_contacts_missing_age = sum(.data[["count"]]),
      .by = "icb22cdh"
    )

  missing_gen <- contacts_count_init |>
    dplyr::filter(is.na(.data[["gender_cat"]])) |>
    dplyr::summarise(
      icb_contacts_missing_gen = sum(.data[["count"]]),
      .by = "icb22cdh"
    )

  final_count <- contacts_count_init |>
    dplyr::filter(
      .data[["consistent_ind"]] == 1L &
        .data[["attendance_cat"]] == "Attended" &
        dplyr::if_all(tidyselect::all_of(join_cols), \(x) !is.na(x))
    ) |>
    dplyr::summarise(
      icb_contacts_final_count = sum(.data[["count"]]),
      .by = "icb22cdh"
    )

  list(
    all_unfiltd,
    inconsistnt,
    not_attnded,
    cancelled,
    app_unknown,
    missing_age,
    missing_gen,
    final_count
  ) |>
    purrr::reduce(\(x, y) dplyr::left_join(x, y, "icb22cdh")) |>
    dplyr::mutate(
      icb_contacts_total_excld = .data[["icb_contacts_all_unfiltd"]] -
        .data[["icb_contacts_final_count"]], # nolint
      .by = "icb22cdh",
      .before = "icb_contacts_final_count"
    )
}

# COMMAND ----------

# DBTITLE 1,national dq summary function (patients)
create_nat_patients_summary <- function(patients_count_init) {
  all_unfiltd <- patients_count_init |>
    dplyr::summarise(
      nat_patients_all_unfiltd = dplyr::n_distinct(.data[["patient_id"]])
    )

  missing_icb <- patients_count_init |>
    dplyr::filter(all(is.na(.data[["icb22cdh"]])), .by = "patient_id") |>
    dplyr::summarise(
      nat_patients_missing_icb = dplyr::n_distinct(.data[["patient_id"]])
    )

  inconsistnt <- patients_count_init |>
    dplyr::filter(!any(.data[["consistent_ind"]] == 1L), .by = "patient_id") |>
    dplyr::summarise(
      nat_patients_inconsistnt = dplyr::n_distinct(.data[["patient_id"]])
    )

  not_attnded <- patients_count_init |>
    dplyr::filter(
      !any(.data[["attendance_cat"]] == "Attended"),
      .by = "patient_id"
    ) |>
    dplyr::summarise(
      nat_patients_not_attnded = dplyr::n_distinct(.data[["patient_id"]])
    )

  missing_age <- patients_count_init |>
    dplyr::filter(all(is.na(.data[["age_int"]])), .by = "patient_id") |>
    dplyr::summarise(
      nat_patients_missing_age = dplyr::n_distinct(.data[["patient_id"]])
    )

  missing_gen <- patients_count_init |>
    dplyr::filter(all(is.na(.data[["gender_cat"]])), .by = "patient_id") |>
    dplyr::summarise(
      nat_patients_missing_gen = dplyr::n_distinct(.data[["patient_id"]])
    )

  final_count <- patients_count_init |>
    dplyr::filter(
      .data[["consistent_ind"]] == 1L & .data[["attendance_cat"]] == "Attended"
    ) |>
    dplyr::filter(
      any(!is.na(.data[["age_int"]])) &
        any(!is.na(.data[["gender_cat"]])) &
        any(!is.na(.data[["lad18cd"]])),
      .by = "patient_id"
    ) |>
    dplyr::summarise(
      nat_patients_final_count = dplyr::n_distinct(.data[["patient_id"]])
    )

  list(
    all_unfiltd,
    missing_icb,
    inconsistnt,
    not_attnded,
    missing_age,
    missing_gen,
    final_count
  ) |>
    purrr::map(\(x) dplyr::mutate(x, national = "National", .before = 1)) |>
    purrr::reduce(\(x, y) dplyr::left_join(x, y, "national")) |>
    dplyr::select(!"national") |>
    dplyr::mutate(
      nat_patients_total_excld = .data[["nat_patients_all_unfiltd"]] -
        .data[["nat_patients_final_count"]],
      .before = "nat_patients_final_count"
    )
}

# COMMAND ----------

# DBTITLE 1,icb dq summary function (patients)
create_icb_patients_summary <- function(patients_count_init) {
  all_unfiltd <- patients_count_init |>
    dplyr::summarise(
      icb_patients_all_unfiltd = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  inconsistnt <- patients_count_init |>
    dplyr::filter(!any(.data[["consistent_ind"]] == 1L), .by = "patient_id") |>
    dplyr::summarise(
      icb_patients_inconsistnt = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  not_attnded <- patients_count_init |>
    dplyr::filter(
      !any(.data[["attendance_cat"]] == "Attended"),
      .by = "patient_id"
    ) |>
    dplyr::summarise(
      icb_patients_not_attnded = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  missing_age <- patients_count_init |>
    dplyr::filter(all(is.na(.data[["age_int"]])), .by = "patient_id") |>
    dplyr::summarise(
      icb_patients_missing_age = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  missing_gen <- patients_count_init |>
    dplyr::filter(all(is.na(.data[["gender_cat"]])), .by = "patient_id") |>
    dplyr::summarise(
      icb_patients_missing_gen = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  final_count <- patients_count_init |>
    dplyr::filter(
      .data[["consistent_ind"]] == 1L & .data[["attendance_cat"]] == "Attended"
    ) |>
    dplyr::filter(
      any(!is.na(.data[["age_int"]])) & any(!is.na(.data[["gender_cat"]])),
      .by = "patient_id"
    ) |>
    dplyr::summarise(
      icb_patients_final_count = dplyr::n_distinct(.data[["patient_id"]]),
      .by = "icb22cdh"
    )

  list(
    all_unfiltd,
    inconsistnt,
    not_attnded,
    missing_age,
    missing_gen,
    final_count
  ) |>
    purrr::reduce(\(x, y) dplyr::left_join(x, y, "icb22cdh")) |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.numeric),
        \(x) dplyr::if_else(is.na(x), 0L, x)
      )
    ) |>
    dplyr::mutate(
      icb_patients_total_excld = .data[["icb_patients_all_unfiltd"]] -
        .data[["icb_patients_final_count"]],
      .before = "icb_patients_final_count"
    )
}

# COMMAND ----------

create_icb_unique_patients_summary <- function(patients_count_init) {
  patients_count_init |>
    dplyr::filter(
      .data[["consistent_ind"]] == 1L & .data[["attendance_cat"]] == "Attended"
    ) |>
    dplyr::filter(
      any(!is.na(.data[["age_int"]])) & any(!is.na(.data[["gender_cat"]])),
      .by = "patient_id"
    ) |>
    dplyr::summarise(
      count = dplyr::n_distinct(.data[["patient_id"]]),
      .by = tidyselect::all_of(c(icb_cols, join_cols))
    )
}

# COMMAND ----------

create_nat_unique_patients_summary <- function(patients_count_init) {
  patients_count_init |>
    dplyr::filter(
      .data[["consistent_ind"]] == 1L & .data[["attendance_cat"]] == "Attended"
    ) |>
    dplyr::filter(
      any(!is.na(.data[["age_int"]])) & any(!is.na(.data[["gender_cat"]])),
      .by = "patient_id"
    ) |>
    dplyr::summarise(
      count = dplyr::n_distinct(.data[["patient_id"]]),
      .by = tidyselect::all_of(join_cols)
    )
}

# COMMAND ----------

# DBTITLE 1,create data quality summary tables
nat_contacts_summary <- create_nat_contacts_summary(contacts_count_init)
icb_contacts_summary <- create_icb_contacts_summary(contacts_count_init)
nat_patients_summary <- create_nat_patients_summary(patients_count_init)
icb_patients_summary <- create_icb_patients_summary(patients_count_init)

# COMMAND ----------

db_write_rds(dplyr::collect(nat_contacts_summary), "nat_contacts_summary.rds")
db_write_rds(dplyr::collect(icb_contacts_summary), "icb_contacts_summary.rds")
db_write_rds(dplyr::collect(nat_patients_summary), "nat_patients_summary.rds")
db_write_rds(dplyr::collect(icb_patients_summary), "icb_patients_summary.rds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## counts of unique patients by age and financial year

# COMMAND ----------

icb_patients_count_init <- patients_count_init |>
  dplyr::filter(
    dplyr::if_any("consistent_ind", \(x) x == 1L) &
      dplyr::if_any("attendance_cat", \(x) x == "Attended") &
      dplyr::if_any("gender_cat", \(x) x %in% c("Male", "Female")) &
      dplyr::if_any("age_int", \(x) !is.na(x)) &
      dplyr::if_any("icb22cdh", \(x) !is.na(x))
  ) |>
  dplyr::select(!tidyselect::any_of(dq_cols)) |>
  dplyr::mutate(
    total_pat_n = sum(.data[["count"]]),
    .by = tidyselect::all_of(c(icb_cols, pt))
  ) |>
  dplyr::mutate(
    patient_lad_prop = sum(.data[["count"]]) / .data[["total_pat_n"]],
    .by = tidyselect::all_of(c(icb_cols, lad, pt)),
    .keep = "unused"
  ) |>
  dplyr::summarise(
    count = sum(.data[["patient_lad_prop"]]),
    .by = tidyselect::all_of(c(icb_cols, join_cols, sv))
  )


# COMMAND ----------

icb_patients_count_init |>
  sparklyr::spark_write_parquet(
    glue::glue("{db_vol}/icb_patients_count_init"),
    mode = "overwrite"
  )

# COMMAND ----------

popn_fy_projected <- db_read_rds("popn_fy_projected.rds")

# COMMAND ----------

icb_unique_patients_summary <- patients_count_init |>
  create_icb_unique_patients_summary() |>
  dplyr::collect() |>
  tidyr::nest(.by = tidyselect::all_of(icb_cols)) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, join_popn_proj_data)
  )) |>
  tidyr::unnest("data") |>
  dplyr::rename(proj_uniq_px_by_fy_age = "projected_count")


# COMMAND ----------

nat_unique_patients_summary <- patients_count_init |>
  create_nat_unique_patients_summary() |>
  dplyr::collect() |>
  join_popn_proj_data() |>
  dplyr::rename(proj_uniq_px_by_fy_age = "projected_count")

# COMMAND ----------

db_write_rds(icb_unique_patients_summary, "icb_unique_patients_summary.rds")
db_write_rds(nat_unique_patients_summary, "nat_unique_patients_summary.rds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## create and export final datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### patients datasets

# COMMAND ----------

popn_fy_projected <- db_read_rds("popn_fy_projected.rds")
icb_popn_fy_projected <- db_read_rds("icb_popn_fy_projected.rds")
nat_popn_fy_projected <- db_read_rds("nat_popn_fy_projected.rds")
nat_patients_summary <- db_read_rds("nat_patients_summary.rds")
icb_patients_summary <- db_read_rds("icb_patients_summary.rds")
icb_unique_patients_summary <- db_read_rds("icb_unique_patients_summary.rds")
nat_unique_patients_summary <- db_read_rds("nat_unique_patients_summary.rds")

# COMMAND ----------

icb_patients_count_init <- sc |>
  sparklyr::spark_read_parquet(glue::glue("{db_vol}/icb_patients_count_init"))

# COMMAND ----------

# This creates projected patient numbers by ICB and service, and nests this data
icb_patients_final <- icb_patients_count_init |>
  dplyr::collect() |>
  tidyr::nest(.by = tidyselect::all_of(c(icb_cols, sv))) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, join_popn_proj_data)
  )) |>
  tidyr::unnest("data") |>
  dplyr::left_join(
    icb_unique_patients_summary,
    c("icb22cdh", "icb22nm", "fin_year", "age_int")
  ) |>
  dplyr::left_join(
    icb_popn_fy_projected,
    c("icb22cdh", "fin_year", "age_int")
  ) |>
  tidyr::nest(.by = tidyselect::all_of(icb_cols)) |>
  dplyr::left_join(icb_patients_summary, "icb22cdh") |>
  dplyr::relocate("data", .after = tidyselect::last_col()) |>
  dplyr::mutate(
    across("data", \(x) {
      purrr::map(x, \(x) tidyr::nest(x, .by = tidyselect::all_of(ncp)))
    })
  )


# COMMAND ----------

# DBTITLE 1,write patient dataset 1 to rds
db_write_rds(icb_patients_final, "icb_patients_final.rds")

# COMMAND ----------

nat_patients_final <- icb_patients_count_init |>
  dplyr::summarise(
    across("count", sum),
    .by = tidyselect::all_of(c(sv, join_cols))
  ) |>
  dplyr::collect() |>
  tidyr::nest(.by = tidyselect::all_of(sv)) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, join_popn_proj_data)
  )) |>
  tidyr::unnest("data") |>
  dplyr::left_join(nat_unique_patients_summary, c("fin_year", "age_int")) |>
  dplyr::left_join(nat_popn_fy_projected, c("fin_year", "age_int")) |>
  tidyr::nest() |>
  dplyr::bind_cols(nat_patients_summary) |>
  dplyr::relocate("data", .after = tidyselect::last_col()) |>
  dplyr::mutate(
    across("data", \(x) {
      purrr::map(x, \(x) tidyr::nest(x, .by = tidyselect::all_of(ncp)))
    })
  )

# COMMAND ----------

# DBTITLE 1,write patient dataset 2 to rds
db_write_rds(nat_patients_final, "nat_patients_final.rds")

# COMMAND ----------

# MAGIC %md
# MAGIC ### contacts datasets

# COMMAND ----------

popn_fy_projected <- db_read_rds("popn_fy_projected.rds")
icb_popn_fy_projected <- db_read_rds("icb_popn_fy_projected.rds")
nat_popn_fy_projected <- db_read_rds("nat_popn_fy_projected.rds")
nat_contacts_summary <- db_read_rds("nat_contacts_summary.rds")
icb_contacts_summary <- db_read_rds("icb_contacts_summary.rds")

# COMMAND ----------

icb_contacts_count <- contacts_count_init |>
  dplyr::filter(
    dplyr::if_any("consistent_ind", \(x) x == 1L) &
      dplyr::if_any("attendance_cat", \(x) x == "Attended") &
      dplyr::if_any("gender_cat", \(x) x %in% c("Male", "Female")) &
      dplyr::if_any("age_int", \(x) !is.na(x)) &
      dplyr::if_any("icb22cdh", \(x) !is.na(x))
  ) |>
  dplyr::select(!tidyselect::any_of(dq_cols)) |>
  dplyr::summarise(
    across("count", sum),
    .by = tidyselect::all_of(c(icb_cols, join_cols, sv))
  ) |>
  dplyr::collect()


# COMMAND ----------

icb_contacts_final <- icb_contacts_count |>
  tidyr::nest(.by = tidyselect::all_of(c(icb_cols, sv))) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, join_popn_proj_data)
  )) |>
  tidyr::unnest("data") |>
  dplyr::left_join(
    icb_popn_fy_projected,
    c("icb22cdh", "fin_year", "age_int")
  ) |>
  tidyr::nest(.by = tidyselect::all_of(icb_cols)) |>
  dplyr::left_join(icb_contacts_summary, "icb22cdh") |>
  dplyr::relocate("data", .after = tidyselect::last_col()) |>
  dplyr::mutate(
    across("data", \(x) {
      purrr::map(x, \(x) tidyr::nest(x, .by = tidyselect::all_of(ncc)))
    })
  )


# COMMAND ----------

# DBTITLE 1,write contacts dataset 1 to rds
db_write_rds(icb_contacts_final, "icb_contacts_final.rds")

# COMMAND ----------

nat_contacts_final <- icb_contacts_count |>
  dplyr::summarise(
    across("count", sum),
    .by = tidyselect::all_of(c(sv, join_cols))
  ) |>
  tidyr::nest(.by = tidyselect::all_of(sv)) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, join_popn_proj_data)
  )) |>
  tidyr::unnest("data") |>
  dplyr::left_join(nat_popn_fy_projected, c("fin_year", "age_int")) |>
  tidyr::nest() |>
  dplyr::bind_cols(nat_contacts_summary) |>
  dplyr::relocate("data", .after = tidyselect::last_col()) |>
  dplyr::mutate(
    across("data", \(x) {
      purrr::map(x, \(x) tidyr::nest(x, .by = tidyselect::all_of(ncc)))
    })
  )

# COMMAND ----------

# DBTITLE 1,write contacts dataset 2 to rds
db_write_rds(nat_contacts_final, "nat_contacts_final.rds")
