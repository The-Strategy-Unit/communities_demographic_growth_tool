# Databricks notebook source
# MAGIC %md
# MAGIC ## setup

# COMMAND ----------

# DBTITLE 1,initial datasets
sc <- sparklyr::spark_connect(method = "databricks")

csds_data_full_valid <- dplyr::tbl(
  sc,
  dbplyr::in_catalog("strategyunit", "csds_jg", "full_contact_based_valid")
)
lsoa11_lad18_lookup_eng <- dplyr::tbl(
  sc,
  dbplyr::in_catalog("strategyunit", "csds_fb", "lsoa11_lad18_lookup_eng")
)
consistent_submitters <- dplyr::tbl(
  sc,
  dbplyr::in_catalog("strategyunit", "csds_fb", "consistent_submitters_2022_23")
)
icb_lad_split_init <- dplyr::tbl(
  sc,
  dbplyr::in_catalog("strategyunit", "csds_fb", "icb_lad_split")
)

consistent_subs <- dplyr::pull(consistent_submitters, "provider_org_id")
lsoa11_lad18_lookup_eng <- dplyr::select(
  lsoa11_lad18_lookup_eng,
  tidyselect::ends_with("cd")
)

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
icb_lad_split <- icb_lad_split_init |>
  dplyr::left_join(lsoa11_lad18_lookup_eng, "lsoa11cd") |>
  dplyr::mutate(lad_n = dplyr::n(), .by = "lad18cd") |>
  dplyr::mutate(icb_lad_n = dplyr::n(), .by = c("lad18cd", "icb22cdh")) |>
  dplyr::collect() |>
  dplyr::reframe(
    lad_icb_pct = .data[["icb_lad_n"]] / .data[["lad_n"]],
    .by = c("icb22cdh", "lad18cd")
  ) |>
  dplyr::distinct()

# COMMAND ----------

icb_popn_fy_projected <- icb_lad_split |>
  dplyr::left_join(
    popn_fy_projected,
    "lad18cd",
    relationship = "many-to-many"
  ) |>
  dplyr::mutate(
    across("fin_year_popn", \(x) x * .data[["lad_icb_pct"]]),
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

# DBTITLE 1,reusable variables to refer to key columns
icb_cols <- c("icb22cdh", "icb22nm")
dq_cols <- c("consistent_ind", "attendance_cat")
sv <- "service"
join_cols <- c("lad18cd", "age_int", "gender_cat")
pt <- "patient_id"
ncp <- c("proj_uniq_px_by_fy_age", "proj_popn_by_fy_age", "fin_year", "age_int")
ncc <- c("proj_popn_by_fy_age", "fin_year", "age_int")

# COMMAND ----------

# DBTITLE 1,core function for growing patient and contact numbers
join_popn_proj_data <- function(
  dat,
  type = c("patients", "contacts"),
  pfp = popn_fy_projected
) {
  join_cols <- c("lad18cd", "age_int", "gender_cat")
  dat |>
    dplyr::left_join(pfp, join_cols, relationship = "many-to-many") |>
    dplyr::mutate(
      projected = .data[[type]] * .data[["growth_coeff"]],
      .keep = "unused"
    ) |>
    dplyr::summarise(
      dplyr::across("projected", sum),
      .by = c("fin_year", "age_int")
    ) |>
    dplyr::arrange(dplyr::pick(c("fin_year", "age_int")))
}


# COMMAND ----------

# MAGIC %md
# MAGIC ## initial data read in

# COMMAND ----------

# DBTITLE 1,create csds_data_init
# https://www.datadictionary.nhs.uk/data_elements/attendance_status.html
# 5 - attended on time
# 6 - attended late but was seen
# 7 - attended late and was not seen
# 2 - patient cancelled
# 3 - DNA (without notice)
# 4 - provider cancelled or postponed
csds_data_init <- csds_data_full_valid |>
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
  )) |>
  dplyr::filter(!dplyr::if_any("patient_id", is.na)) |>
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
      .data[["submitter_id"]] %in% {{ consistent_subs }},
      1L,
      0L
    ),
    dplyr::across("age_int", as.integer),
    dplyr::across("age_int", \(x) dplyr::if_else(x > 90L, 90L, x)),
    dplyr::across("gender_cat", \(x) dplyr::if_else(x == "Unknown", NA, x)),
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
  dplyr::group_by(patient_id) |>
  # This step just gets rid of NAs. Values filled may be superseded by next
  # line. It reduces NAs in the data from 172k to 17k.
  tidyr::fill("age_int", .direction = "downup") |>
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
  csds_data_init,
  "/Volumes/strategyunit/csds_fb/csds_patients/csds_data_init",
  mode = "overwrite"
)

# COMMAND ----------

csds_data_init <- sparklyr::spark_read_parquet(
  sc,
  "/Volumes/strategyunit/csds_fb/csds_patients/csds_data_init"
)

# COMMAND ----------

# DBTITLE 1,contacts count
contacts_count_init <- csds_data_init |>
  dplyr::count(
    dplyr::pick(tidyselect::all_of(c(icb_cols, dq_cols, sv, join_cols))),
    name = "contacts"
  )

# COMMAND ----------

# DBTITLE 1,patients count
patients_count_init <- csds_data_init |>
  dplyr::count(
    dplyr::pick(tidyselect::all_of(c(icb_cols, dq_cols, sv, pt, join_cols)))
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## data quality summaries

# COMMAND ----------

# DBTITLE 1,national dq summary function (contacts)
create_nat_contacts_summary <- function(contacts_count_init) {
  all_unfiltd <- contacts_count_init |>
    dplyr::summarise(nat_contacts_all_unfiltd = sum(.data[["contacts"]]))

  missing_icb <- contacts_count_init |>
    dplyr::filter(is.na(.data[["icb22cdh"]])) |>
    dplyr::summarise(nat_contacts_missing_icb = sum(.data[["contacts"]]))

  inconsistnt <- contacts_count_init |>
    dplyr::filter(.data[["consistent_ind"]] == 0L) |>
    dplyr::summarise(nat_contacts_inconsistnt = sum(.data[["contacts"]]))

  not_attnded <- contacts_count_init |>
    dplyr::filter(.data[["attendance_cat"]] == "Did Not Attend") |>
    dplyr::summarise(nat_contacts_not_attnded = sum(.data[["contacts"]]))

  canclld_unk <- contacts_count_init |>
    dplyr::filter(.data[["attendance_cat"]] %in% c("Cancelled", "Unknown")) |>
    dplyr::summarise(nat_contacts_canclld_unk = sum(.data[["contacts"]]))

  missing_age <- contacts_count_init |>
    dplyr::filter(is.na(.data[["age_int"]])) |>
    dplyr::summarise(nat_contacts_missing_age = sum(.data[["contacts"]]))

  missing_gen <- contacts_count_init |>
    dplyr::filter(is.na(.data[["gender_cat"]])) |>
    dplyr::summarise(nat_contacts_missing_gen = sum(.data[["contacts"]]))

  final_count <- contacts_count_init |>
    dplyr::filter(
      .data[["consistent_ind"]] == 1L &
        .data[["attendance_cat"]] == "Attended" &
        dplyr::if_all(tidyselect::all_of(join_cols), \(x) !is.na(x))
    ) |>
    dplyr::summarise(nat_contacts_final_count = sum(.data[["contacts"]]))

  list(
    all_unfiltd,
    missing_icb,
    inconsistnt,
    not_attnded,
    canclld_unk,
    missing_age,
    missing_gen,
    final_count
  ) |>
    purrr::map(\(x) dplyr::mutate(x, national = "National", .before = 1)) |>
    purrr::reduce(\(x, y) dplyr::left_join(x, y, "national")) |>
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
      icb_contacts_all_unfiltd = sum(.data[["contacts"]]),
      .by = "icb22cdh"
    )

  inconsistnt <- contacts_count_init |>
    dplyr::filter(.data[["consistent_ind"]] == 0L) |>
    dplyr::summarise(
      icb_contacts_inconsistnt = sum(.data[["contacts"]]),
      .by = "icb22cdh"
    )

  not_attnded <- contacts_count_init |>
    dplyr::filter(.data[["attendance_cat"]] == "Did Not Attend") |>
    dplyr::summarise(
      icb_contacts_not_attnded = sum(.data[["contacts"]]),
      .by = "icb22cdh"
    )

  canclld_unk <- contacts_count_init |>
    dplyr::filter(.data[["attendance_cat"]] %in% c("Cancelled", "Unknown")) |>
    dplyr::summarise(
      icb_contacts_canclld_unk = sum(.data[["contacts"]]),
      .by = "icb22cdh"
    )

  missing_age <- contacts_count_init |>
    dplyr::filter(is.na(.data[["age_int"]])) |>
    dplyr::summarise(
      icb_contacts_missing_age = sum(.data[["contacts"]]),
      .by = "icb22cdh"
    )

  missing_gen <- contacts_count_init |>
    dplyr::filter(is.na(.data[["gender_cat"]])) |>
    dplyr::summarise(
      icb_contacts_missing_gen = sum(.data[["contacts"]]),
      .by = "icb22cdh"
    )

  final_count <- contacts_count_init |>
    dplyr::filter(
      .data[["consistent_ind"]] == 1L &
        .data[["attendance_cat"]] == "Attended" &
        dplyr::if_all(tidyselect::all_of(join_cols), \(x) !is.na(x))
    ) |>
    dplyr::summarise(
      icb_contacts_final_count = sum(.data[["contacts"]]),
      .by = "icb22cdh"
    )

  list(
    all_unfiltd,
    inconsistnt,
    not_attnded,
    canclld_unk,
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
      patients = dplyr::n_distinct(.data[["patient_id"]]),
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
      patients = dplyr::n_distinct(.data[["patient_id"]]),
      .by = tidyselect::all_of(join_cols)
    )
}

# COMMAND ----------

# DBTITLE 1,create data quality summary tables
nat_contacts_summary <- create_nat_contacts_summary(contacts_count_init) |>
  dplyr::collect()
icb_contacts_summary <- create_icb_contacts_summary(contacts_count_init) |>
  dplyr::collect()
nat_patients_summary <- create_nat_patients_summary(patients_count_init) |>
  dplyr::collect()
icb_patients_summary <- create_icb_patients_summary(patients_count_init) |>
  dplyr::collect()

# COMMAND ----------

csds_icb_patients_count_init <- patients_count_init |>
  dplyr::filter(
    dplyr::if_any("consistent_ind", \(x) x == 1L) &
      dplyr::if_any("attendance_cat", \(x) x == "Attended") &
      dplyr::if_any("gender_cat", \(x) x %in% c("Male", "Female")) &
      dplyr::if_any("age_int", \(x) !is.na(x)) &
      dplyr::if_any("icb22cdh", \(x) !is.na(x))
  ) |>
  dplyr::select(!tidyselect::any_of(dq_cols)) |>
  dplyr::mutate(
    total_pat_n = sum(.data[["n"]]),
    .by = tidyselect::all_of(c(icb_cols, pt))
  ) |>
  dplyr::mutate(
    patient_lad_prop = sum(.data[["n"]]) / .data[["total_pat_n"]],
    .by = tidyselect::all_of(c(icb_cols, "lad18cd", pt)),
    .keep = "unused"
  ) |>
  dplyr::summarise(
    patients = sum(.data[["patient_lad_prop"]]),
    .by = tidyselect::all_of(c(icb_cols, join_cols, sv))
  ) |>
  dplyr::collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ### counts of unique patients by age and financial year

# COMMAND ----------

icb_unique_patients_summary <- create_icb_unique_patients_summary(
  patients_count_init
) |>
  dplyr::collect() |>
  tidyr::nest(.by = tidyselect::all_of(icb_cols)) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, \(x) join_popn_proj_data(x, "patients"))
  )) |>
  tidyr::unnest("data") |>
  dplyr::rename(proj_uniq_px_by_fy_age = "projected")


# COMMAND ----------

nat_unique_patients_summary <- create_nat_unique_patients_summary(
  patients_count_init
) |>
  dplyr::collect() |>
  join_popn_proj_data("patients") |>
  dplyr::rename(proj_uniq_px_by_fy_age = "projected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## create and export final datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### patients datasets

# COMMAND ----------

csds_icb_patients_count <- csds_icb_patients_count_init |>
  tidyr::nest(.by = tidyselect::all_of(c(icb_cols, sv))) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, \(x) join_popn_proj_data(x, "patients"))
  )) |>
  tidyr::unnest("data") |>
  dplyr::rename(projected_patients = "projected") |>
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
csds_icb_patients_count |>
  readr::write_rds(
    "/Volumes/strategyunit/csds_fb/csds_patients/csds_icb_patients_count.rds"
  )

# COMMAND ----------

csds_nat_patients_count_init <- csds_icb_patients_count_init |>
  dplyr::summarise(
    across("patients", sum),
    .by = tidyselect::all_of(c(sv, join_cols))
  ) |>
  dplyr::mutate(national = "National", .before = 1)

# COMMAND ----------

csds_nat_patients_count <- csds_nat_patients_count_init |>
  tidyr::nest(.by = tidyselect::all_of(c("national", sv))) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, \(x) join_popn_proj_data(x, "patients"))
  )) |>
  tidyr::unnest("data") |>
  dplyr::rename(projected_patients = "projected") |>
  dplyr::left_join(nat_unique_patients_summary, c("fin_year", "age_int")) |>
  dplyr::left_join(nat_popn_fy_projected, c("fin_year", "age_int")) |>
  tidyr::nest(.by = "national") |>
  dplyr::left_join(nat_patients_summary, "national") |>
  dplyr::relocate("data", .after = tidyselect::last_col()) |>
  dplyr::mutate(
    across("data", \(x) {
      purrr::map(x, \(x) tidyr::nest(x, .by = tidyselect::all_of(ncp)))
    })
  )

# COMMAND ----------

# DBTITLE 1,write patient dataset 2 to rds
csds_nat_patients_count |>
  readr::write_rds(
    "/Volumes/strategyunit/csds_fb/csds_patients/csds_nat_patients_count.rds"
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### contacts datasets

# COMMAND ----------

csds_icb_contacts_count_init <- contacts_count_init |>
  dplyr::filter(
    dplyr::if_any("consistent_ind", \(x) x == 1L) &
      dplyr::if_any("attendance_cat", \(x) x == "Attended") &
      dplyr::if_any("gender_cat", \(x) x %in% c("Male", "Female")) &
      dplyr::if_any("age_int", \(x) !is.na(x)) &
      dplyr::if_any("icb22cdh", \(x) !is.na(x))
  ) |>
  dplyr::select(!tidyselect::any_of(dq_cols)) |>
  dplyr::summarise(
    across("contacts", sum),
    .by = tidyselect::all_of(c(icb_cols, join_cols, sv))
  ) |>
  dplyr::collect()


# COMMAND ----------

csds_icb_contacts_count <- csds_icb_contacts_count_init |>
  tidyr::nest(.by = tidyselect::all_of(c(icb_cols, sv))) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, \(x) join_popn_proj_data(x, "contacts"))
  )) |>
  tidyr::unnest("data") |>
  dplyr::rename(projected_contacts = "projected") |>
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
csds_icb_contacts_count |>
  readr::write_rds(
    "/Volumes/strategyunit/csds_fb/csds_patients/csds_icb_contacts_count.rds"
  )

# COMMAND ----------

csds_nat_contacts_count_init <- csds_icb_contacts_count_init |>
  dplyr::summarise(
    across("contacts", sum),
    .by = tidyselect::all_of(c(sv, join_cols))
  ) |>
  dplyr::mutate(national = "National", .before = 1)

# COMMAND ----------

csds_nat_contacts_count <- csds_nat_contacts_count_init |>
  tidyr::nest(.by = tidyselect::all_of(c("national", sv))) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, \(x) join_popn_proj_data(x, "contacts"))
  )) |>
  tidyr::unnest("data") |>
  dplyr::rename(projected_contacts = "projected") |>
  dplyr::left_join(nat_popn_fy_projected, c("fin_year", "age_int")) |>
  tidyr::nest(.by = "national") |>
  dplyr::left_join(nat_contacts_summary, "national") |>
  dplyr::relocate("data", .after = tidyselect::last_col()) |>
  dplyr::mutate(
    across("data", \(x) {
      purrr::map(x, \(x) tidyr::nest(x, .by = tidyselect::all_of(ncc)))
    })
  )

# COMMAND ----------

# DBTITLE 1,write contacts dataset 2 to rds
csds_nat_contacts_count |>
  readr::write_rds(
    "/Volumes/strategyunit/csds_fb/csds_patients/csds_nat_contacts_count.rds"
  )
