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


icb_popn_fy_projected <- readr::read_rds("icb_lad_split.rds") |>
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
  ) |>
  readr::write_rds("icb_popn_fy_projected.rds")

nat_popn_fy_projected <- popn_fy_projected |>
  dplyr::summarise(
    proj_popn_by_fy_age = sum(.data[["fin_year_popn"]]),
    .by = c("fin_year", "age_int")
  ) |>
  readr::write_rds("nat_popn_fy_projected.rds")
