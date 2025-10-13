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

consistent_subs <- dplyr::tbl(
  sc,
  dbplyr::in_catalog("strategyunit", "csds_fb", "consistent_submitters_2022_23")
) |>
  dplyr::pull("provider_org_id")


# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,helper functions
db_vol <- "/Volumes/strategyunit/csds_fb/cdgt_rds_files"
db_write_rds <- \(x, f) readr::write_rds(x, glue::glue("{db_vol}/{f}"))
db_read_rds <- \(f) readr::read_rds(glue::glue("{db_vol}/{f}"))

# COMMAND ----------

# DBTITLE 1,reusable variables to refer to key columns
icb_cols <- c("icb22cdh", "icb22nm")
att <- "attendance_cat"
dq_cols <- c("consistent_ind", att)
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
  dplyr::filter(
    !dplyr::if_any("patient_id", is.na) &
      dplyr::if_any("fin_year", \(x) x == "2022/23")
  ) |>
  dplyr::mutate(
    consistent_ind = dplyr::if_else(
      .data[["submitter_id"]] %in% {{ consistent_subs }},
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
  dbplyr::window_order(.data$contact_date) |>
  dplyr::group_by(.data$patient_id) |>
  tidyr::fill(
    c("icb22cdh", "icb22nm", "age_int", "gender_cat", "lad18cd"),
    .direction = "downup"
  ) |>
  dplyr::ungroup() |>
  dplyr::select(!c("fin_year", "lsoa11cd"))


# COMMAND ----------

# DBTITLE 1,contacts count
contacts_count_init <- csds_data_tidy |>
  dplyr::count(
    dplyr::pick(tidyselect::all_of(c(icb_cols, sv, dq_cols, join_cols))),
    name = "count"
  )

# COMMAND ----------

# DBTITLE 1,patients count
patients_count_init <- csds_data_tidy |>
  dplyr::mutate(
    dplyr::across("attendance_cat", \(x) {
      dplyr::if_else(x == "Attended", 1L, 0L, 0L)
    })
  ) |>
  dplyr::mutate(
    dplyr::across(tidyselect::all_of(dq_cols), max),
    .by = tidyselect::all_of(c(icb_cols, sv, pt))
  ) |>
  dplyr::count(dplyr::pick(tidyselect::all_of(c(
    icb_cols,
    sv,
    pt,
    dq_cols,
    join_cols
  ))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## data quality summaries

# COMMAND ----------

# DBTITLE 1,national dq summary function (contacts)
create_nat_contacts_summary <- function(contacts_count_init) {
  contacts_count_init |>
    dplyr::select(!c("icb22nm", "service")) |>
    dplyr::mutate(
      all_unfiltd = .data[["count"]],
      missing_icb = dplyr::if_else(
        is.na(.data[["icb22cdh"]]),
        .data[["count"]],
        0L
      ),
      inconsistnt = dplyr::if_else(
        .data[["consistent_ind"]] == 0L,
        .data[["count"]],
        0L
      ),
      not_attnded = dplyr::if_else(
        .data[["attendance_cat"]] == "Did Not Attend",
        .data[["count"]],
        0L
      ),
      app_canclld = dplyr::if_else(
        .data[["attendance_cat"]] == "Cancelled",
        .data[["count"]],
        0L
      ),
      app_unknown = dplyr::if_else(
        .data[["attendance_cat"]] == "Unknown",
        .data[["count"]],
        0L
      ),
      missing_age = dplyr::if_else(
        is.na(.data[["age_int"]]),
        .data[["count"]],
        0L
      ),
      missing_gen = dplyr::if_else(
        is.na(.data[["gender_cat"]]),
        .data[["count"]],
        0L
      ),
      final_count = dplyr::if_else(
        dplyr::if_all(
          tidyselect::all_of(c("icb22cdh", join_cols)),
          \(x) !is.na(x)
        ) &
          .data[["consistent_ind"]] == 1L &
          .data[["attendance_cat"]] == "Attended",
        .data[["count"]],
        0L
      ),
      .keep = "unused"
    ) |>
    dplyr::summarise(dplyr::across(tidyselect::everything(), sum)) |>
    dplyr::mutate(
      total_excld = .data[["all_unfiltd"]] - .data[["final_count"]]
    ) |>
    dplyr::relocate("final_count", .after = tidyselect::last_col()) |>
    dplyr::rename_with(\(x) paste0("nat_contacts_", x))
}

# COMMAND ----------

# DBTITLE 1,icb dq summary function (contacts)
create_icb_contacts_summary <- function(contacts_count_init) {
  contacts_count_init |>
    dplyr::select(!c("icb22nm", "service")) |>
    dplyr::filter(!is.na(.data[["icb22cdh"]])) |>
    dplyr::mutate(
      all_unfiltd = .data[["count"]],
      inconsistnt = dplyr::if_else(
        .data[["consistent_ind"]] == 0L,
        .data[["count"]],
        0L
      ),
      not_attnded = dplyr::if_else(
        .data[["attendance_cat"]] == "Did Not Attend",
        .data[["count"]],
        0L
      ),
      app_canclld = dplyr::if_else(
        .data[["attendance_cat"]] == "Cancelled",
        .data[["count"]],
        0L
      ),
      app_unknown = dplyr::if_else(
        .data[["attendance_cat"]] == "Unknown",
        .data[["count"]],
        0L
      ),
      missing_age = dplyr::if_else(
        is.na(.data[["age_int"]]),
        .data[["count"]],
        0L
      ),
      missing_gen = dplyr::if_else(
        is.na(.data[["gender_cat"]]),
        .data[["count"]],
        0L
      ),
      final_count = dplyr::if_else(
        dplyr::if_all(tidyselect::all_of(join_cols), \(x) !is.na(x)) &
          .data[["consistent_ind"]] == 1L &
          .data[["attendance_cat"]] == "Attended",
        .data[["count"]],
        0L
      ),
      .keep = "unused"
    ) |>
    dplyr::summarise(
      dplyr::across(!"icb22cdh", sum),
      .by = "icb22cdh"
    ) |>
    dplyr::mutate(
      total_excld = .data[["all_unfiltd"]] - .data[["final_count"]]
    ) |>
    dplyr::relocate("final_count", .after = tidyselect::last_col()) |>
    dplyr::rename_with(\(x) paste0("icb_contacts_", x), .cols = !"icb22cdh")
}

# COMMAND ----------

# DBTITLE 1,national dq summary function (patients)
create_nat_patients_summary <- function(patients_count_init) {
  patients_count_init |>
    dplyr::mutate(
      final_count = dplyr::if_else(
        any(!is.na(.data[["icb22cdh"]])) &
          any(.data[["consistent_ind"]] == 1L) &
          any(.data[["attendance_cat"]] == 1L) &
          any(!is.na(.data[["age_int"]])) &
          any(!is.na(.data[["gender_cat"]])) &
          any(!is.na(.data[["lad18cd"]])),
        1L,
        0L
      ),
      .by = "patient_id"
    ) |>
    dplyr::summarise(
      missing_icb = dplyr::if_else(all(is.na(.data[["icb22cdh"]])), 1L, 0L),
      inconsistnt = dplyr::if_else(
        all(.data[["consistent_ind"]] == 0L),
        1L,
        0L
      ),
      not_attnded = dplyr::if_else(
        all(.data[["attendance_cat"]] == 0L),
        1L,
        0L
      ),
      missing_age = dplyr::if_else(all(is.na(.data[["age_int"]])), 1L, 0L),
      missing_gen = dplyr::if_else(all(is.na(.data[["gender_cat"]])), 1L, 0L),
      .by = c("patient_id", "final_count")
    ) |>
    dplyr::mutate(all_unfiltd = dplyr::n()) |>
    dplyr::select(!"patient_id") |>
    dplyr::summarise(
      dplyr::across("all_unfiltd", mean), # in lieu of `unique`
      dplyr::across(!"all_unfiltd", sum)
    ) |>
    dplyr::mutate(
      total_excld = .data[["all_unfiltd"]] - .data[["final_count"]]
    ) |>
    dplyr::relocate("final_count", .after = tidyselect::last_col()) |>
    dplyr::rename_with(\(x) paste0("nat_patients_", x))
}

# COMMAND ----------

# DBTITLE 1,icb dq summary function (patients)
create_icb_patients_summary <- function(patients_count_init) {
  patients_count_init |>
    dplyr::filter(!is.na(.data[["icb22cdh"]])) |>
    dplyr::mutate(
      final_count = dplyr::if_else(
        any(.data[["consistent_ind"]] == 1L) &
          any(.data[["attendance_cat"]] == 1L) &
          any(!is.na(.data[["age_int"]])) &
          any(!is.na(.data[["gender_cat"]])) &
          any(!is.na(.data[["lad18cd"]])),
        1L,
        0L
      ),
      .by = c("icb22cdh", "patient_id")
    ) |>
    dplyr::summarise(
      inconsistnt = dplyr::if_else(
        all(.data[["consistent_ind"]] == 0L),
        1L,
        0L
      ),
      not_attnded = dplyr::if_else(
        all(.data[["attendance_cat"]] == 0L),
        1L,
        0L
      ),
      missing_age = dplyr::if_else(all(is.na(.data[["age_int"]])), 1L, 0L),
      missing_gen = dplyr::if_else(all(is.na(.data[["gender_cat"]])), 1L, 0L),
      .by = c("icb22cdh", "patient_id", "final_count")
    ) |>
    dplyr::mutate(all_unfiltd = dplyr::n(), .by = "icb22cdh") |>
    dplyr::select(!"patient_id") |>
    dplyr::summarise(
      dplyr::across("all_unfiltd", mean),
      dplyr::across(!c("icb22cdh", "all_unfiltd"), sum),
      .by = "icb22cdh"
    ) |>
    dplyr::mutate(
      total_excld = .data[["all_unfiltd"]] - .data[["final_count"]]
    ) |>
    dplyr::relocate("final_count", .after = tidyselect::last_col()) |>
    dplyr::rename_with(\(x) paste0("icb_patients_", x), .cols = !"icb22cdh")
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

db_write_rds(nat_contacts_summary, "nat_contacts_summary.rds")
db_write_rds(icb_contacts_summary, "icb_contacts_summary.rds")
db_write_rds(nat_patients_summary, "nat_patients_summary.rds")
db_write_rds(icb_patients_summary, "icb_patients_summary.rds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## counts of unique patients by age and financial year

# COMMAND ----------

icb_patients_count_filt <- patients_count_init |>
  dplyr::filter(
    dplyr::if_all(tidyselect::all_of(dq_cols), \(x) x == 1L) &
      dplyr::if_all(
        tidyselect::all_of(c("icb22cdh", join_cols)),
        \(x) !is.na(x)
      )
  )

icb_patients_count_prop <- icb_patients_count_filt |>
  dplyr::mutate(
    total_pat_n = sum(.data[["n"]]), # still counting contacts at this point
    .by = tidyselect::all_of(c(icb_cols, pt))
  ) |>
  # For each individual patient we now have a "proportion" of that patient that
  # belongs to each LAD. For most patients this will be 1, that is their records
  # only relate to a single LAD of residence. But some patients we will allocate
  # partially to multiple LADs, and then later sum by LAD.
  # The proportion is calculated based on number of _contacts_ recorded against
  # each patient, for each LAD they are associated with.
  dplyr::summarise(
    # mean() required in Spark as it wants to work with a single value per group
    # (In pure R it would automatically use the unique value per group for
    # total_pat_n, I think)
    prop = sum(.data[["n"]]) / mean(.data[["total_pat_n"]]),
    .by = tidyselect::all_of(c(icb_cols, lad, pt))
  )


# An intermediate table to be used in the both the following 2 steps
icb_patients_count_intermediate <- icb_patients_count_filt |>
  dplyr::select(tidyselect::all_of(c(icb_cols, sv, join_cols, pt))) |>
  # This gives us 1 row for each combination of ICB, service, LAD and patient
  # That is, row counts are counts of distinct patients.
  # (age and gender fields have already been made unique per patient).
  dplyr::distinct() |> # db now saying dplyr::n_distinct() not available?!
  dplyr::left_join(icb_patients_count_prop, c(icb_cols, lad, pt))

icb_patients_count <- icb_patients_count_intermediate |>
  dplyr::select(!tidyselect::all_of(sv)) |>
  dplyr::distinct() |>
  # This gives us a count of unique patients (taking into account
  # "proportional patients") by ICB and LAD.
  dplyr::summarise(
    count = sum(.data[["prop"]]),
    .by = tidyselect::all_of(c(icb_cols, join_cols))
  )

icb_patients_count_by_service <- icb_patients_count_intermediate |>
  # This gives us a count of unique patients (taking into account
  # "proportional patients") by ICB and LAD.
  dplyr::summarise(
    count = sum(.data[["prop"]]),
    .by = tidyselect::all_of(c(icb_cols, sv, join_cols))
  )


# COMMAND ----------

nat_patients_count_filt <- patients_count_init |>
  dplyr::filter(
    dplyr::if_all(tidyselect::all_of(dq_cols), \(x) x == 1L) &
      dplyr::if_all(tidyselect::all_of(join_cols), \(x) !is.na(x))
  )

nat_patients_count_prop <- nat_patients_count_filt |>
  dplyr::mutate(
    total_pat_n = sum(.data[["n"]]), # still counting contacts at this point
    .by = tidyselect::all_of(pt)
  ) |>
  # For each individual patient we now have a "proportion" of that patient that
  # belongs to each LAD. For most patients this will be 1, that is their records
  # only relate to a single LAD of residence. But some patients we will allocate
  # partially to multiple LADs, and then later sum by LAD.
  # The proportion is calculated based on number of _contacts_ recorded against
  # each patient, for each LAD they are associated with.
  dplyr::summarise(
    # mean() required in Spark as it wants to work with a single value per group
    # (In pure R it would automatically use the unique value per group for
    # total_pat_n, I think)
    prop = sum(.data[["n"]]) / mean(.data[["total_pat_n"]]),
    .by = tidyselect::all_of(c(lad, pt))
  )


# An intermediate table to be used in the both the following 2 steps
nat_patients_count_intermediate <- nat_patients_count_filt |>
  dplyr::select(tidyselect::all_of(c(sv, join_cols, pt))) |>
  # This gives us 1 row for each combination of ICB, service, LAD and patient
  # That is, row counts are counts of distinct patients.
  # (age and gender fields have already been made unique per patient).
  dplyr::distinct() |> # db now saying dplyr::n_distinct() not available?!
  dplyr::left_join(nat_patients_count_prop, c(lad, pt))

nat_patients_count <- nat_patients_count_intermediate |>
  dplyr::select(!tidyselect::all_of(sv)) |>
  dplyr::distinct() |>
  # This gives us a count of unique patients (taking into account
  # "proportional patients") by ICB and LAD.
  dplyr::summarise(
    count = sum(.data[["prop"]]),
    .by = tidyselect::all_of(join_cols)
  )

nat_patients_count_by_service <- nat_patients_count_intermediate |>
  # This gives us a count of unique patients (taking into account
  # "proportional patients") by ICB and LAD.
  dplyr::summarise(
    count = sum(.data[["prop"]]),
    .by = tidyselect::all_of(c(sv, join_cols))
  )


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
nat_contacts_summary <- db_read_rds("nat_contacts_summary.rds")
icb_contacts_summary <- db_read_rds("icb_contacts_summary.rds")

# COMMAND ----------

icb_overall_patients_count_projected <- icb_patients_count |>
  dplyr::collect() |>
  tidyr::nest(.by = tidyselect::all_of(icb_cols)) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, join_popn_proj_data)
  )) |>
  tidyr::unnest("data")

# This creates projected patient numbers by ICB and service, and nests this data
icb_patients_final <- icb_patients_count_by_service |>
  dplyr::collect() |>
  tidyr::nest(.by = tidyselect::all_of(c(icb_cols, sv))) |>
  dplyr::mutate(
    across("data", \(x) purrr::map(x, join_popn_proj_data))
  ) |>
  tidyr::unnest("data") |>
  # Currently for the tool we don't need any age breakdown of data for each
  # service, so we can simplify the data by summing patient counts across
  # all ages
  dplyr::summarise(
    dplyr::across("projected_count", sum),
    .by = tidyselect::all_of(c(icb_cols, sv, "fin_year"))
  ) |>
  tidyr::nest(
    .by = tidyselect::all_of(icb_cols),
    .key = "service_data"
  ) |>
  dplyr::left_join(icb_overall_patients_count_projected, icb_cols) |>
  dplyr::left_join(
    icb_popn_fy_projected,
    c("icb22cdh", "fin_year", "age_int")
  ) |>
  tidyr::nest(.by = tidyselect::all_of(c(icb_cols, "service_data"))) |>
  dplyr::left_join(icb_patients_summary, "icb22cdh")


# COMMAND ----------

# DBTITLE 1,write patient dataset 1 to rds
db_write_rds(icb_patients_final, "icb_patients_final.rds")

# COMMAND ----------

nat_overall_patients_count_projected <- nat_patients_count |>
  dplyr::collect() |>
  join_popn_proj_data()

nat_patients_final <- nat_patients_count_by_service |>
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
  # Currently for the tool we don't need any age breakdown of data for each
  # service, so we can simplify the data by summing patient counts across
  # all ages
  dplyr::summarise(
    dplyr::across("projected_count", sum),
    .by = tidyselect::all_of(c(sv, "fin_year"))
  ) |>
  tidyr::nest(.key = "service_data") |>
  dplyr::bind_cols(nat_overall_patients_count_projected) |>
  dplyr::left_join(nat_popn_fy_projected, c("fin_year", "age_int")) |>
  tidyr::nest(.by = "service_data") |>
  dplyr::bind_cols(nat_patients_summary)

# COMMAND ----------

# DBTITLE 1,write patient dataset 2 to rds
db_write_rds(nat_patients_final, "nat_patients_final.rds")

# COMMAND ----------

# MAGIC %md
# MAGIC ### contacts datasets

# COMMAND ----------

icb_contacts_count <- contacts_count_init |>
  dplyr::filter(
    dplyr::if_any("consistent_ind", \(x) x == 1L) &
      dplyr::if_any("attendance_cat", \(x) x == "Attended") &
      dplyr::if_any("gender_cat", \(x) x %in% c("Male", "Female")) &
      dplyr::if_any("age_int", \(x) !is.na(x)) &
      dplyr::if_any("icb22cdh", \(x) !is.na(x))
  ) |>
  # The DQ cols (consistent and attendance) can now be deselected
  dplyr::summarise(
    across("count", sum),
    .by = tidyselect::all_of(c(icb_cols, sv, join_cols))
  ) |>
  dplyr::collect()

nat_contacts_count <- icb_contacts_count |>
  dplyr::summarise(
    across("count", sum),
    .by = tidyselect::all_of(c(sv, join_cols))
  )


# COMMAND ----------

icb_overall_contacts_count_projected <- icb_contacts_count |>
  tidyr::nest(.by = tidyselect::all_of(icb_cols)) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, join_popn_proj_data)
  )) |>
  tidyr::unnest("data")

icb_contacts_final <- icb_contacts_count |>
  tidyr::nest(.by = tidyselect::all_of(c(icb_cols, sv))) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, join_popn_proj_data)
  )) |>
  tidyr::unnest("data") |>
  # Currently for the tool we don't need any age breakdown of data for each
  # service, so we can simplify the data by summing contact counts across
  # all ages
  dplyr::summarise(
    dplyr::across("projected_count", sum),
    .by = tidyselect::all_of(c(icb_cols, sv, "fin_year"))
  ) |>
  tidyr::nest(
    .by = tidyselect::all_of(icb_cols),
    .key = "service_data"
  ) |>
  dplyr::left_join(icb_overall_contacts_count_projected, icb_cols) |>
  dplyr::left_join(
    icb_popn_fy_projected,
    c("icb22cdh", "fin_year", "age_int")
  ) |>
  tidyr::nest(.by = tidyselect::all_of(c(icb_cols, "service_data"))) |>
  dplyr::left_join(icb_contacts_summary, "icb22cdh")


# COMMAND ----------

# DBTITLE 1,write contacts dataset 1 to rds
db_write_rds(icb_contacts_final, "icb_contacts_final.rds")

# COMMAND ----------

nat_overall_contacts_count_projected <- nat_contacts_count |>
  join_popn_proj_data()

nat_contacts_final <- nat_contacts_count |>
  tidyr::nest(.by = tidyselect::all_of(sv)) |>
  dplyr::mutate(across(
    "data",
    \(x) purrr::map(x, join_popn_proj_data)
  )) |>
  tidyr::unnest("data") |>
  # Currently for the tool we don't need any age breakdown of data for each
  # service, so we can simplify the data by summing contact counts across
  # all ages
  dplyr::summarise(
    dplyr::across("projected_count", sum),
    .by = tidyselect::all_of(c(sv, "fin_year"))
  ) |>
  tidyr::nest(.key = "service_data") |>
  dplyr::bind_cols(nat_overall_contacts_count_projected) |>
  dplyr::left_join(nat_popn_fy_projected, c("fin_year", "age_int")) |>
  tidyr::nest(.by = "service_data") |>
  dplyr::bind_cols(nat_contacts_summary)

# COMMAND ----------

# DBTITLE 1,write contacts dataset 2 to rds
db_write_rds(nat_contacts_final, "nat_contacts_final.rds")
