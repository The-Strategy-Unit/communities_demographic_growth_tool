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


lsoa_lad18_lookup <- boundr::lookup("lsoa", "lad", within_year = 2018) |>
  dplyr::select(tidyselect::ends_with("cd"))

icb_lad_split <- boundr::opts(return_width = "full") |>
  boundr::lookup("lsoa", "icb", lookup_year = 2011, opts = _) |>
  dplyr::select(c("lsoa11cd", "icb22cdh")) |>
  dplyr::left_join(lsoa_lad18_lookup, "lsoa11cd") |>
  dplyr::mutate(lad_n = dplyr::n(), .by = "lad18cd") |>
  dplyr::mutate(icb_lad_n = dplyr::n(), .by = c("lad18cd", "icb22cdh")) |>
  dplyr::reframe(
    lad_icb_pct = .data[["icb_lad_n"]] / .data[["lad_n"]],
    .by = c("icb22cdh", "lad18cd")
  ) |>
  dplyr::distinct() |>
  readr::write_rds("icb_lad_split.rds")
