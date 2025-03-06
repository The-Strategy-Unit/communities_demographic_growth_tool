create_data_quality_table <- function(dat, icb = TRUE, label) {
  category_names <- c(
    "All initial contacts",
    "Contacts not attributed to an ICB",
    "Contacts with unknown patient gender",
    "Contacts with missing patient age",
    "Contacts from inconsistent submitters",
    "Contacts where the patient did not attend",
    "Contacts where the appointment was cancelled",
    "Total contacts excluded",
    "Remaining contacts included in projections"
  )
  if (icb) category_names <- purrr::discard_at(category_names, 2)
row_cancelled <- stringr::str_which(category_names, "appointment was cancelled")
  dat |>
    dplyr::select(!"data") |>
    dplyr::rename_with(\(x) category_names) |>
    tidyr::pivot_longer(
      cols = tidyselect::everything(),
      names_to = "Category",
      values_to = "n"
    ) |>
    dplyr::mutate(`%` = round(.data[["n"]] * 100 / .data[["n"]][[1]], 1)) |>
    gt::gt() |>
    gt::tab_header(glue::glue("CSDS Data Quality Summary - {label}")) |>
    gt::tab_source_note("Source: CSDS 2022/23, NHS England") |>
    gt::tab_footnote(
      "This category includes appointments with unknown status",
      gt::cells_body(1, row_cancelled)
    ) |>
    gt::fmt_number(columns = "n", decimals = 0) |>
    gt::opt_footnote_marks("standard") |>
    gt::opt_table_font("Segoe UI", size = 18, color = "#3e3f3a") |>
    gt::tab_options(table.width = "92%")
}

create_national_dq_table <- function() {
  create_data_quality_table(get_national_data(), FALSE, "National")
}

create_icb_dq_table <- function(icb_data) {
  icb_data |>
    dplyr::select(!tidyselect::starts_with("icb22")) |>
    create_data_quality_table(label = icb_data[["icb22nm"]])
}
