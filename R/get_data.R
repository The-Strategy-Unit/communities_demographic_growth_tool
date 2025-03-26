tidy_icb_names <- function(dat) {
  dat |>
    dplyr::mutate(
      dplyr::across("icb22nm", \(x) sub("^(NHS )(.*)( Integ.*)$", "\\2 ICB", x))
    )
}

get_all_icb_data <- function(measure) {
  if (measure == "Contacts") tidy_icb_names(csds_icb_contacts)
  if (measure == "Patients") tidy_icb_names(csds_icb_patients)
}
get_all_national_data <- function(measure) {
  if (measure == "Contacts") csds_nat_contacts
  if (measure == "Patients") csds_nat_patients
}

pull_unique <- \(dat, col) unique(dat[[col]])
pluck_data <- \(dat) dat[["data"]][[1]]

icb_list <- function() {
  get_icb_contacts_data() |>
    dplyr::select(c("icb22nm", "icb22cdh")) |>
    tibble::deframe()
}

year_list <- function() {
  get_national_contacts_data() |>
    pull_unique("fin_year") |>
    rlang::set_names(\(x) stringr::str_replace(x, "_", "/"))
}
