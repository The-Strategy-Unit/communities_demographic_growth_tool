tidy_icb_names <- function(dat) {
  dat |>
    dplyr::mutate(
      dplyr::across("icb22nm", \(x) sub("^(NHS )(.*)( Integ.*)$", "\\2 ICB", x))
    )
}

get_all_icb_data <- function(measure) {
  switch(
    measure,
    Contacts = tidy_icb_names(CSDSDemographicGrowthApp::icb_contacts_data),
    Patients = tidy_icb_names(CSDSDemographicGrowthApp::icb_patients_data)
  )
}
get_all_national_data <- function(measure) {
  switch(
    measure,
    Contacts = CSDSDemographicGrowthApp::nat_contacts_data,
    Patients = CSDSDemographicGrowthApp::nat_patients_data
  )
}

pull_unique <- \(dat, col) unique(dat[[col]])
pluck_data <- \(dat) dat[["data"]][[1]]

icb_list <- function() {
  get_all_icb_data("Contacts") |>
    dplyr::select(c("icb22nm", "icb22cdh")) |>
    dplyr::arrange(dplyr::pick("icb22nm")) |>
    tibble::deframe()
}

year_list <- function() {
  get_all_national_data("Contacts") |>
    pluck_data() |>
    pull_unique("fin_year") |>
    rlang::set_names(\(x) stringr::str_replace(x, "_", "/"))
}
