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
    add_broad_age_groups() |>
    dplyr::summarise(
      dplyr::across("projected_count", sum),
      .by = c("fin_year", "broad_age_cat")
    )
}

bind_national_icb_data <- function(icb_data, measure) {
  list(get_all_national_data(measure), icb_data) |>
    purrr::map(pluck_data) |>
    rlang::set_names(c("England", icb_data[["icb22nm"]])) |>
    dplyr::bind_rows(.id = "type") |>
    dplyr::mutate(dplyr::across("type", forcats::fct_inorder))
}


plot_percent_change_by_age <- function(icb_data, measure, horizon) {
  y_axis_title <- glue::glue(
    "% change in {tolower(measure)} between 2022/23 and horizon year"
  )
  light_blue <- "#5881c1"
  bind_national_icb_data(icb_data, measure) |>
    dplyr::filter(.data$fin_year %in% c("2022_23", horizon)) |>
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
    ggplot2::labs(x = "Age group", y = y_axis_title) +
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

  icb_contacts_data <- get_all_icb_data("Contacts") |>
    dplyr::filter(.data$icb22cdh == {{ icb }}) |>
    dplyr::collect() |>
    pluck_data()
  icb_patients_data <- get_all_icb_data("Patients") |>
    dplyr::filter(.data$icb22cdh == {{ icb }}) |>
    dplyr::collect() |>
    pluck_data()

  icb_data <- list(
    contacts = icb_contacts_data,
    patients = icb_patients_data
  ) |>
    dplyr::bind_rows(.id = "measure")

  nat_contacts_data <- get_all_national_data("Contacts") |>
    pluck_data()
  nat_patients_data <- get_all_national_data("Patients") |>
    pluck_data()

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
      value = sum(.data[["projected_count"]]),
      .by = c("type", "measure", "age_group_cat")
    ) |>
    tidyr::pivot_wider(names_from = "measure") |>
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


plot_percent_change_by_service <- function(icb_data, measure, horizon) {
  x_axis_title <- glue::glue(
    "% change in {tolower(measure)} between 2022/23 and horizon year"
  )
  light_blue <- "#5881c1"
  prepare_service_data(icb_data, measure, horizon) |>
    dplyr::mutate(
      type_ind = dplyr::if_else(.data[["type"]] == "England", 1L, 0L),
      pct_change_mod = .data[["pct_change"]] * .data[["type_ind"]],
      dplyr::across("service", \(x) {
        forcats::fct_reorder(x, .data[["pct_change_mod"]], .na_rm = FALSE)
      })
    ) |>
    ggplot2::ggplot(ggplot2::aes(.data$pct_change, .data$service)) +
    ggplot2::geom_col(
      ggplot2::aes(fill = .data$type),
      position = "dodge",
      width = 0.75
    ) +
    ggplot2::geom_vline(xintercept = 0, linewidth = 0.7, colour = light_blue) +
    ggplot2::labs(x = x_axis_title, y = NULL) +
    ggplot2::scale_x_continuous(labels = scales::label_percent(suffix = "%")) +
    ggplot2::scale_fill_manual(
      name = NULL,
      values = duo_colours(icb_data[["icb22nm"]])
    ) +
    su_chart_theme() +
    ggplot2::theme(axis.text.y = ggplot2::element_text(size = 14))
}


render_percent_change_by_service_table <- function(icb_data, measure, horizon) {
  tbl_data <- prepare_service_data(icb_data, measure, horizon) |>
    dplyr::mutate(
      dplyr::across("type", \(x) dplyr::if_else(x == "England", "eng", "icb"))
    ) |>
    tidyr::pivot_wider(names_from = "type", values_from = "pct_change") |>
    dplyr::mutate(
      relative_to_national = .data[["icb"]] - .data[["eng"]]
    ) |>
    dplyr::select(c("service", "icb", "eng", "relative_to_national"))
  tbl_data |>
    reactable::reactable(
      bordered = TRUE,
      striped = TRUE,
      defaultPageSize = 15,
      defaultSorted = c("icb"),
      defaultSortOrder = "desc",
      showSortable = TRUE,
      columns = list(
        service = reactable::colDef(
          "Service",
          searchable = TRUE,
          filterable = TRUE,
          sortable = TRUE
        ),
        icb = reactable::colDef(
          "ICB % change (projected)",
          cell = function(value) {
            width <- paste0(value * 80 / max(tbl_data[["icb"]]), "%")
            value <- paste0(round(value * 100, 1), "%")
            bar <- htmltools::div(
              htmltools::span(value, style = list(visibility = "hidden")),
              style = list(
                width = width,
                marginLeft = "0.375rem",
                height = "0.875rem",
                color = "#5881c1",
                backgroundColor = "#5881c1"
              )
            )
            htmltools::div(
              htmltools::span(
                value,
                style = list(textAlign = "right", flex = "0 0 20%")
              ),
              htmltools::div(
                bar,
                style = list(flex = "1 1 80%")
              ),
              style = list(alignItems = "center", display = "flex")
            )
          },
          html = TRUE,
          align = "left"
        ),
        eng = reactable::colDef(
          "National % change (projected)",
          cell = function(value) paste0(round(value * 100, 1), "%")
        ),
        relative_to_national = reactable::colDef(
          "Relative to National change",
          cell = function(value) {
            value_rnd <- round(value * 100, 1)
            value_new <- if (value > 0) paste0("+", value_rnd) else value_rnd
            paste0(value_new, "%")
          },
          style = function(value) {
            color <- if (value > 0) {
              "#008000"
            } else if (value < 0) {
              "#e00000"
            }
            list(fontWeight = 600, color = color)
          }
        )
      )
    )
}


prepare_service_data <- function(icb_data, measure, horizon) {
  list(get_all_national_data(measure), icb_data) |>
    purrr::map(pluck_service_data) |>
    rlang::set_names(c("England", icb_data[["icb22nm"]])) |>
    dplyr::bind_rows(.id = "type") |>
    dplyr::filter(.data[["fin_year"]] %in% c("2022_23", horizon)) |>
    dplyr::mutate(
      dplyr::across("type", forcats::fct_inorder),
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
    # completely if the ICB value is missing.
    tidyr::complete(.data[["type"]], .data[["fin_year"]], .data[["service"]]) |>
    dplyr::filter(!any(is.na(.data[["value"]])), .by = "service") |>
    tidyr::pivot_wider(names_from = "fin_year", names_prefix = "yr_") |>
    dplyr::mutate(
      pct_change = (.data[[glue::glue("yr_{horizon}")]] -
        .data[["yr_2022_23"]]) /
        .data[["yr_2022_23"]],
      .keep = "unused"
    )
}
