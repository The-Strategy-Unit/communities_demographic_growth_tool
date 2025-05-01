get_all_icb_data <- function(measure) {
  switch(
    measure,
    Contacts = arrow::open_dataset("icb_contacts_final"),
    Patients = arrow::open_dataset("icb_patients_final")
  )
}
get_all_national_data <- function(measure) {
  switch(
    measure,
    Contacts = arrow::open_dataset("nat_contacts_final"),
    Patients = arrow::open_dataset("nat_patients_final")
  ) |>
    dplyr::collect()
}

pull_unique <- \(dat, col) unique(dat[[col]])
pluck_data <- \(dat) dat[["data"]][[1]]

icb_list <- function() {
  get_all_icb_data("Contacts") |>
    dplyr::select(c("icb22nm", "icb22cdh")) |>
    dplyr::collect() |>
    dplyr::arrange(.data$icb22nm) |>
    tibble::deframe()
}

year_list <- function() {
  get_all_national_data("Contacts") |>
    pluck_data() |>
    pull_unique("fin_year") |>
    rlang::set_names(\(x) stringr::str_replace(x, "_", "/"))
}
