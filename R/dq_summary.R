get_category_names <- function(measure, icb) {
  category_names <- switch(
    measure,
    Contacts = c(
      "All contacts in initial dataset",
      "Contacts not attributed to an ICB",
      "Contacts from inconsistent submitters",
      "Contacts where the patient did not attend",
      "Contacts where the appointment was cancelled",
      "Contacts where the appointment status is unknown",
      "Contacts with unknown patient age",
      "Contacts with unknown patient gender",
      "Total contacts excluded",
      "Remaining contacts included in projections"
    ),
    Patients = c(
      "All patients in initial dataset",
      "Patients not attributed to an ICB",
      "Patients from inconsistent submitters",
      "Patients who did not attend any appointments",
      "Patients with unknown patient age",
      "Patients with unknown patient gender",
      "Total patients excluded",
      "Remaining patients included in projections"
    )
  )
  icb_cat <- stringr::str_which(category_names, "ICB$")
  if (icb) category_names <- category_names[-icb_cat]
  category_names
}

create_dq_summary_table <- function(dat, category_names, label) {
  dat |>
    dplyr::select(!c("data", "service_data")) |>
    dplyr::rename_with(\(x) category_names) |>
    tidyr::pivot_longer(
      cols = tidyselect::everything(),
      names_to = "Category",
      values_to = "n"
    ) |>
    dplyr::mutate(
      `%` = round(.data[["n"]] * 100 / .data[["n"]][[1]], 1),
      dplyr::across("n", round)
    ) |>
    gt::gt() |>
    gt::tab_header(glue::glue("CSDS Data Quality Summary - {label}")) |>
    gt::fmt_number(columns = "n", decimals = 0) |>
    gt::tab_options(
      table.font.names = c("Open Sans", "Segoe UI", "Roboto", "sans-serif"),
      table.font.size = "16px",
      table.background.color = "#fff0"
    )
}

create_contacts_dq_table <- function(dat, icb = TRUE, label) {
  create_dq_summary_table(dat, get_category_names("Contacts", icb), label)
}

create_patients_dq_table <- function(dat, icb = TRUE, label) {
  create_dq_summary_table(dat, get_category_names("Patients", icb), label)
}

create_icb_dq_summary_table <- function(dat, measure) {
  dat2 <- dplyr::select(dat, !tidyselect::starts_with("icb22"))
  switch(
    measure,
    Contacts = create_contacts_dq_table(dat2, label = dat[["icb22nm"]]),
    Patients = create_patients_dq_table(dat2, label = dat[["icb22nm"]])
  )
}

create_nat_dq_summary_table <- function(measure) {
  dat <- get_all_national_data(measure)
  switch(
    measure,
    Contacts = create_contacts_dq_table(dat, icb = FALSE, label = "National"),
    Patients = create_patients_dq_table(dat, icb = FALSE, label = "National")
  )
}
