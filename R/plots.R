create_main_projection_chart <- function(dat, measure, horizon) {
  convert_year_labels <- function(x) {
    paste0(format(x, "%Y"), "/", (as.integer(format(x, "%Y")) + 1) %% 100)
  }
  fy_as_date <- \(x) as.Date(sub("_[0-9]{2}$", "-04-01", x))

  full_dat <- dplyr::mutate(dat, dplyr::across("fin_year", fy_as_date))
  filtered_dat <- dplyr::filter(full_dat, .data$fin_year <= fy_as_date(horizon))

  label_fn <- if (max(full_dat$projected_count) < 1e6) {
    scales::label_number(scale = 1e-3, suffix = "k")
  } else {
    scales::label_number(scale = 1e-6, suffix = "m")
  }

  full_dat |>
    ggplot2::ggplot(ggplot2::aes(
      x = .data$fin_year,
      y = .data$projected_count,
      group = .data$broad_age_cat
    )) +
    ggplot2::geom_line(linewidth = 1, colour = "grey") +
    ggplot2::geom_line(
      data = filtered_dat,
      ggplot2::aes(colour = .data$broad_age_cat),
      linewidth = 1.8
    ) +
    ggplot2::geom_point(
      data = filtered_dat,
      ggplot2::aes(colour = .data$broad_age_cat),
      size = 2.7
    ) +
    ggplot2::labs(x = NULL, y = measure) +
    ggplot2::scale_y_continuous(labels = label_fn) +
    ggplot2::scale_x_date(
      breaks = seq.Date(as.Date("2022-04-01"), by = "5 years", length.out = 5),
      minor_breaks = seq.Date(
        as.Date("2023-04-01"),
        as.Date("2041-04-01"),
        "1 year"
      ),
      labels = convert_year_labels
    ) +
    ggplot2::scale_colour_manual(
      name = "Age group",
      values = broad_age_group_colours()
    ) +
    su_chart_theme()
}

plot_national_contacts_by_year <- function(measure = "Contacts") {
  get_all_national_data(measure) |>
    prepare_main_plot_data(measure) |>
    create_main_projection_chart(measure, horizon = "2042_43")
}


plot_national_patients_by_year <- function(measure = "Patients") {
  get_all_national_data(measure) |>
    prepare_main_plot_data(measure) |>
    create_main_projection_chart(measure, horizon = "2042_43")
}

plot_icb_measure_by_year <- function(icb_data, measure, horizon) {
  icb_data |>
    prepare_main_plot_data(measure) |>
    create_main_projection_chart(measure, horizon = horizon)
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

bind_national_icb_data <- function(icb_data, measure) {
  list(get_all_national_data(measure), icb_data) |>
    purrr::map(pluck_data) |>
    rlang::set_names(c("England", icb_data[["icb22nm"]])) |>
    dplyr::bind_rows(.id = "type") |>
    dplyr::mutate(dplyr::across("type", forcats::fct_inorder))
}


plot_percent_change_by_age <- function(icb_data, measure, horizon) {
  light_blue <- "#5881c1"
  bind_national_icb_data(icb_data, measure) |>
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
    ggplot2::geom_hline(yintercept = 0, linewidth = 0.4, colour = light_blue) +
    ggplot2::labs(x = "Age group", y = "% change") +
    ggplot2::scale_y_continuous(labels = scales::label_percent(suffix = "")) +
    ggplot2::scale_fill_manual(
      name = NULL,
      values = duo_colours(icb_data[["icb22nm"]])
    ) +
    su_chart_theme()
}


plot_count_per_population <- function(icb_data, measure) {
  bind_national_icb_data(icb_data, measure) |>
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
    ggplot2::labs(x = "Age group", y = paste0(measure, " / population")) +
    ggplot2::scale_y_continuous(labels = NULL) +
    ggplot2::scale_fill_manual(
      name = NULL,
      values = duo_colours(icb_data[["icb22nm"]])
    ) +
    su_chart_theme()
}


plot_contacts_per_patient <- function(icb) {
  icb_name <- get_all_icb_data("Contacts") |>
    dplyr::filter(.data$icb22cdh == {{ icb }}) |>
    dplyr::collect() |>
    dplyr::pull("icb22nm")
  sum_cols <- c("proj_popn_by_fy_age", "fin_year", "age_int")

  icb_contacts_data <- get_all_icb_data("Contacts") |>
    dplyr::filter(.data$icb22cdh == {{ icb }}) |>
    pluck_data() |>
    prepare_plot_data("Contacts") |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = tidyselect::all_of(sum_cols)
    )
  icb_patients_data <- get_all_icb_data("Patients") |>
    dplyr::filter(.data$icb22cdh == {{ icb }}) |>
    pluck_data() |>
    prepare_plot_data("Patients")

  icb_data <- list(
    contacts = icb_contacts_data,
    patients = icb_patients_data
  ) |>
    dplyr::bind_rows(.id = "measure")

  nat_contacts_data <- get_all_national_data("Contacts") |>
    pluck_data() |>
    prepare_plot_data("Contacts") |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = tidyselect::all_of(sum_cols)
    )
  nat_patients_data <- get_all_national_data("Patients") |>
    pluck_data() |>
    prepare_plot_data("Patients")

  nat_data <- list(
    contacts = nat_contacts_data,
    patients = nat_patients_data
  ) |>
    dplyr::bind_rows(.id = "measure")

  list(nat_data, icb_data) |>
    rlang::set_names(c("England", icb_name)) |>
    dplyr::bind_rows(.id = "type") |>
    dplyr::mutate(dplyr::across("type", forcats::fct_inorder)) |>
    dplyr::filter(dplyr::if_any("fin_year", \(x) x == "2022_23")) |>
    add_age_groups() |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = c("type", "measure", "age_group_cat")
    ) |>
    dplyr::rename(count = "projected_count") |>
    tidyr::pivot_wider(values_from = "count", names_from = "measure") |>
    dplyr::mutate(
      rate = .data[["contacts"]] / .data[["patients"]],
      .keep = "unused"
    ) |>
    ggplot2::ggplot(ggplot2::aes(.data$age_group_cat, .data$rate)) +
    ggplot2::geom_col(
      ggplot2::aes(fill = .data$type),
      position = "dodge",
      width = 0.75
    ) +
    ggplot2::labs(x = "Age group", y = "Contacts / 1000 patients") +
    ggplot2::scale_y_continuous(labels = scales::label_number(scale = 1e3)) +
    ggplot2::scale_fill_manual(
      name = NULL,
      values = duo_colours(icb_name)
    ) +
    su_chart_theme()
}


plot_contacts_per_patient <- function(icb) {
  icb_name <- get_all_icb_data("Contacts") |>
    dplyr::filter(.data$icb22cdh == {{ icb }}) |>
    dplyr::pull("icb22nm")
  sum_cols <- c("proj_popn_by_fy_age", "fin_year", "age_int")

  icb_contacts_data <- get_all_icb_data("Contacts") |>
    dplyr::filter(.data$icb22cdh == {{ icb }}) |>
    pluck_data() |>
    prepare_plot_data("Contacts") |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = tidyselect::all_of(sum_cols)
    )
  icb_patients_data <- get_all_icb_data("Patients") |>
    dplyr::filter(.data$icb22cdh == {{ icb }}) |>
    pluck_data() |>
    prepare_plot_data("Patients")

  icb_data <- list(
    contacts = icb_contacts_data,
    patients = icb_patients_data
  ) |>
    dplyr::bind_rows(.id = "measure")

  nat_contacts_data <- get_all_national_data("Contacts") |>
    pluck_data() |>
    prepare_plot_data("Contacts") |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = tidyselect::all_of(sum_cols)
    )
  nat_patients_data <- get_all_national_data("Patients") |>
    pluck_data() |>
    prepare_plot_data("Patients")

  nat_data <- list(
    contacts = nat_contacts_data,
    patients = nat_patients_data
  ) |>
    dplyr::bind_rows(.id = "measure")

  list(nat_data, icb_data) |>
    rlang::set_names(c("England", icb_name)) |>
    dplyr::bind_rows(.id = "type") |>
    dplyr::mutate(dplyr::across("type", forcats::fct_inorder)) |>
    dplyr::filter(dplyr::if_any("fin_year", \(x) x == "2022_23")) |>
    add_age_groups() |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = c("type", "measure", "age_group_cat")
    ) |>
    dplyr::rename(count = "projected_count") |>
    tidyr::pivot_wider(values_from = "count", names_from = "measure") |>
    dplyr::mutate(
      rate = .data[["contacts"]] / .data[["patients"]],
      .keep = "unused"
    ) |>
    ggplot2::ggplot(ggplot2::aes(.data$age_group_cat, .data$rate)) +
    ggplot2::geom_col(
      ggplot2::aes(fill = .data$type),
      position = "dodge",
      width = 0.75
    ) +
    ggplot2::labs(x = "Age group", y = "Contacts / patient") +
    ggplot2::scale_fill_manual(
      name = NULL,
      values = duo_colours(icb_name)
    ) +
    su_chart_theme()
}


plot_percent_change_by_service <- function(icb_data, measure, horizon) {
  light_blue <- "#5881c1"
  bind_national_icb_data(icb_data, measure) |>
    dplyr::filter(.data$fin_year %in% c("2022_23", horizon)) |>
    tidyr::unnest("data") |>
    dplyr::mutate(
      dplyr::across("service", \(x) tidyr::replace_na(x, "Not recorded")),
      dplyr::across("service", \(x) sub(" Service", "", x)),
      dplyr::across("service", \(x) sub("Adult's", "Adults", x)),
      dplyr::across("service", \(x) sub("Children's", "Children", x))
    ) |>
    dplyr::summarise(
      value = sum(.data[["projected_count"]]),
      .by = c("type", "fin_year", "service")
    ) |>
    # Introduces NAs, which we can then use to remove services from the chart
    # completely if either value (national or ICB) is missing.
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
    ggplot2::geom_vline(xintercept = 0, linewidth = 0.7, colour = light_blue) +
    ggplot2::labs(x = NULL, y = NULL) +
    ggplot2::scale_x_continuous(labels = scales::label_percent(suffix = "%")) +
    ggplot2::scale_fill_manual(
      name = NULL,
      values = duo_colours(icb_data[["icb22nm"]])
    ) +
    su_chart_theme() +
    ggplot2::theme(axis.text.y = ggplot2::element_text(size = 14))
}
