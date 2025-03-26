create_contacts_dq_table <- function(dat, icb = TRUE, label) {
  category_names <- c(
    "All contacts in intial dataset",
    "Contacts not attributed to an ICB",
    "Contacts from inconsistent submitters",
    "Contacts where the patient did not attend",
    "Contacts where the appointment was cancelled",
    "Contacts with missing patient age",
    "Contacts with unknown patient gender",
    "Total contacts excluded",
    "Remaining contacts included in projections"
  )
  if (icb) category_names <- category_names[-2]
  row_cancelled <- stringr::str_which(
    category_names,
    "appointment was cancelled"
  )
  dat |>
    dplyr::select(!"data") |>
    dplyr::rename_with(\(x) category_names) |>
    tidyr::pivot_longer(
      cols = tidyselect::everything(),
      names_to = "Category",
      values_to = "n"
    ) |>
    dplyr::mutate(
      `%` = round(.data[["n"]] * 100 / .data[["n"]][[1]], 1),
      across("n", round)
    ) |>
    gt::gt() |>
    gt::tab_header(glue::glue("CSDS Data Quality Summary - {label}")) |>
    gt::tab_footnote(
      "This category also includes appointments with unknown status",
      gt::cells_body(1, row_cancelled)
    ) |>
    # gt::fmt_number(columns = "n", decimals = 0) |>
    gt::opt_footnote_marks("standard") |>
    gt::opt_table_font("Segoe UI", size = 18)
}


create_patients_dq_table <- function(dat, icb = TRUE, label) {
  category_names <- c(
    "All patients in initial dataset",
    "Patients not attributed to an ICB",
    "Patients from inconsistent submitters",
    "Patients who did not attend any appointments",
    "Patients with missing patient age",
    "Patients with unknown patient gender",
    "Total patients excluded",
    "Remaining patients included in projections"
  )
  if (icb) category_names <- category_names[-2]
  dat |>
    dplyr::select(!"data") |>
    dplyr::rename_with(\(x) category_names) |>
    tidyr::pivot_longer(
      cols = tidyselect::everything(),
      names_to = "Category",
      values_to = "n"
    ) |>
    dplyr::mutate(
      `%` = round(.data[["n"]] * 100 / .data[["n"]][[1]], 1),
      across("n", round)
    ) |>
    gt::gt() |>
    gt::tab_header(glue::glue("CSDS Data Quality Summary - {label}")) |>
    # gt::fmt_number(columns = "n", decimals = 0) |>
    gt::opt_footnote_marks("standard")
}

create_icb_dq_summary_table < function(dat, measure) {
  dat2 <- dplyr::select(dat, !tidyselect::starts_with("icb22"))
  if (measure = "Contacts") {
    create_contacts_dq_table(dat2, label = dat[["icb22nm"]])
  }
  if (measure = "Patients") {
    create_patients_dq_table(dat2, label = dat[["icb22nm"]])
  }
}


create_national_contacts_dq_table <- function() {
  csds_nat_contacts |>
    dplyr::select(!"national") |>
    create_contacts_dq_table(FALSE, label = "National")
}

create_icb_contacts_dq_table <- function(icb_data) {
  icb_data |>
     |>
    create_contacts_dq_table(label = icb_data[["icb22nm"]])
}

create_national_patients_dq_table <- function() {
  csds_nat_patients |>
    dplyr::select(!"national") |>
    create_patients_dq_table(FALSE, label = "National")
}

create_icb_patients_dq_table <- function(icb_data) {
  icb_data |>
    dplyr::select(!tidyselect::starts_with("icb22")) |>
    create_patients_dq_table(label = icb_data[["icb22nm"]])
}
