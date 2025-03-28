create_main_projection_chart <- function(dat, horizon) {
  conv_lab <- \(x) {
    paste0(format(x, "%Y"), "/", (as.integer(format(x, "%Y")) + 1) %% 100)
  }
  fy_as_date <- \(x) as.Date(sub("_[0-9]{2}$", "-01-01", x))

  full_dat <- dplyr::mutate(dat, dplyr::across("fin_year", fy_as_date))
  filtered_dat <- dplyr::filter(full_dat, .data$fin_year <= fy_as_date(horizon))

  full_dat |>
    ggplot2::ggplot(ggplot2::aes(
      x = .data$fin_year,
      y = .data$projected_count,
      group = .data$broad_age_cat
    )) +
    ggplot2::geom_line(linewidth = 1, colour = "grey") +
    ggplot2::geom_line(
      data = filtered_dat,
      linewidth = 1,
      ggplot2::aes(colour = .data$broad_age_cat)
    ) +
    ggplot2::geom_point(
      data = filtered_dat,
      ggplot2::aes(colour = .data$broad_age_cat),
      size = 2
    ) +
    ggplot2::labs(x = NULL, y = NULL) +
    ggplot2::scale_y_continuous(
      labels = scales::label_number(scale = 1e-6, suffix = "m")
    ) +
    ggplot2::scale_x_date(
      breaks = seq.Date(as.Date("2022-01-01"), by = "5 years", length.out = 5),
      minor_breaks = seq.Date(
        as.Date("2023-01-01"),
        as.Date("2041-01-01"),
        "1 year"
      ),
      labels = conv_lab,
      guide = ggplot2::guide_axis(minor.ticks = TRUE)
    ) +
    StrategyUnitTheme::scale_colour_su(
      name = "Age group",
      guide = ggplot2::guide_legend(nrow = 1)
    ) +
    su_chart_theme()
}

plot_national_contacts_by_year <- function(measure = "Contacts") {
  get_all_national_data(measure) |>
    prepare_main_plot_data(measure) |>
    create_main_projection_chart(horizon = "2042_43") |>
    enhance_national_contacts_plot()
}
enhance_national_contacts_plot <- function(p) {
  p
}


plot_national_patients_by_year <- function(measure = "Patients") {
  get_all_national_data(measure) |>
    prepare_main_plot_data(measure) |>
    create_main_projection_chart(horizon = "2042_43") |>
    enhance_national_patients_plot()
}
enhance_national_patients_plot <- function(p) {
  p
}

plot_icb_measure_by_year <- function(icb_data, measure, horizon) {
  icb_data |>
    prepare_main_plot_data(measure) |>
    create_main_projection_chart(horizon = horizon)
}

prepare_main_plot_data <- function(dat, measure = c("Contacts", "Patients")) {
  dat |>
    pluck_data() |>
    prepare_plot_data(rlang::arg_match(measure)) |>
    add_broad_age_groups() |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = c("fin_year", "broad_age_cat")
    )
}

prepare_plot_data <- function(dat, measure = c("Contacts", "Patients")) {
  measure <- rlang::arg_match(measure)
  if (measure == "Contacts") {
    dat |>
      tidyr::unnest("data")
  } else {
    dat |>
      dplyr::select(!"data") |>
      # Instead of adding up all the patient counts by service,
      # which would double-count individuals who attended >1 service,
      # we sum the pre-calculated number of unique patients for each year of
      # age and financial year. We just need to re-label this for convenience.
      dplyr::rename(projected_count = "proj_uniq_px_by_fy_age") |>
      dplyr::distinct()
  }
}


plot_percent_change_by_age <- function(
  icb_data,
  measure,
  horizon = "2042_43"
) {
  list(get_all_national_data(measure), icb_data) |>
    purrr::map(pluck_data) |>
    rlang::set_names(c("England", icb_data[["icb22nm"]])) |>
    dplyr::bind_rows(.id = "type") |>
    dplyr::mutate(dplyr::across("type", forcats::fct_inorder)) |>
    dplyr::filter(.data$fin_year %in% c("2022_23", horizon)) |>
    prepare_plot_data(measure) |>
    add_age_groups() |>
    dplyr::summarise(
      value = sum(.data$projected_count),
      .by = c("type", "fin_year", "age_group_cat")
    ) |>
    tidyr::pivot_wider(
      id_cols = c("type", "age_group_cat"),
      names_from = "fin_year",
      names_prefix = "yr_"
    ) |>
    dplyr::mutate(
      pct_change = (.data[[glue::glue("yr_{horizon}")]] -
        .data[["yr_2022_23"]]) /
        .data[["yr_2022_23"]],
      .keep = "unused"
    ) |>
    ggplot2::ggplot(ggplot2::aes(.data$age_group_cat, .data$pct_change)) +
    ggplot2::geom_col(
      ggplot2::aes(fill = .data$type),
      position = "dodge",
      width = 0.75
    ) +
    ggplot2::geom_hline(yintercept = 0, linewidth = 0.4, colour = "#3e3f3a") +
    ggplot2::labs(x = "Age group", y = "% change") +
    ggplot2::scale_y_continuous(
      labels = scales::label_percent(suffix = ""),
      limits = c(-0.25, NA)
    ) +
    StrategyUnitTheme::scale_fill_su(name = NULL) +
    su_chart_theme()
}

plot_count_per_population <- function(icb_data, measure) {
  list(get_all_national_data(measure), icb_data) |>
    rlang::set_names(c("England", icb_data[["icb22nm"]])) |>
    purrr::map(pluck_data) |>
    dplyr::bind_rows(.id = "type") |>
    dplyr::mutate(dplyr::across("type", forcats::fct_inorder)) |>
    dplyr::filter(dplyr::if_any("fin_year", \(x) x == "2022_23")) |>
    dplyr::rename(fin_year_popn = "proj_popn_by_fy_age") |>
    prepare_plot_data(measure) |>
    dplyr::summarise(
      dplyr::across("fin_year_popn", unique),
      dplyr::across("projected_count", sum),
      .by = c("type", "fin_year", "age_int")
    ) |>
    add_age_groups() |>
    dplyr::summarise(
      dplyr::across(c("fin_year_popn", "projected_count"), sum),
      .by = c("type", "age_group_cat")
    ) |>
    dplyr::mutate(
      rate = .data[["projected_count"]] / .data[["fin_year_popn"]],
      .keep = "unused"
    ) |>
    ggplot2::ggplot(ggplot2::aes(.data$age_group_cat, .data$rate)) +
    ggplot2::geom_col(
      ggplot2::aes(fill = .data$type),
      position = "dodge",
      width = 0.75
    ) +
    ggplot2::labs(x = "Age group", y = "Count / 1000 population") +
    ggplot2::scale_y_continuous(
      labels = scales::label_number(scale = 1e3)
    ) +
    StrategyUnitTheme::scale_fill_su(name = NULL) +
    su_chart_theme()
}


plot_percent_change_by_service <- function(
  icb_data,
  measure,
  horizon = "2042_43"
) {
  list(get_all_national_data(measure), icb_data) |>
    purrr::map(pluck_data) |>
    rlang::set_names(c("England", icb_data[["icb22nm"]])) |>
    dplyr::bind_rows(.id = "type") |>
    dplyr::filter(.data$fin_year %in% c("2022_23", horizon)) |>
    tidyr::unnest("data") |>
    dplyr::mutate(
      dplyr::across("type", forcats::fct_inorder),
      dplyr::across("service", \(x) tidyr::replace_na(x, "Not recorded"))
    ) |>
    dplyr::summarise(
      value = sum(.data[["projected_count"]]),
      .by = c("type", "fin_year", "service")
    ) |>
    # Introduces NAs (which we can then use to remove services from the chart
    # completely if any value (national or ICB, base or horizon) is missing).
    tidyr::complete(.data$type, .data$fin_year, .data$service) |>
    dplyr::filter(!any(is.na(.data$value)), .by = "service") |>
    tidyr::pivot_wider(
      id_cols = c("type", "service"),
      names_from = "fin_year",
      names_prefix = "yr_"
    ) |>
    dplyr::mutate(
      pct_change = (.data[[glue::glue("yr_{horizon}")]] -
        .data[["yr_2022_23"]]) /
        .data[["yr_2022_23"]],
      .keep = "unused"
    ) |>
    dplyr::mutate(
      type_ind = dplyr::if_else(.data$type == "England", 1L, 0L),
      pct_change_mod = .data$pct_change * .data$type_ind,
      dplyr::across("service", \(x) {
        forcats::fct_reorder(x, .data$pct_change_mod, .na_rm = FALSE)
      }),
    ) |>
    ggplot2::ggplot(ggplot2::aes(.data$pct_change, .data$service)) +
    ggplot2::geom_col(
      ggplot2::aes(fill = .data$type),
      position = "dodge",
      width = 0.75
    ) +
    ggplot2::geom_vline(xintercept = 0, linewidth = 0.4, colour = "#3e3f3a") +
    ggplot2::labs(x = "% change", y = NULL) +
    ggplot2::scale_x_continuous(labels = scales::label_percent(suffix = "")) +
    StrategyUnitTheme::scale_fill_su(name = NULL) +
    su_chart_theme()
}
