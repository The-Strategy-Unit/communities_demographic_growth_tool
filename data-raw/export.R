read_db_dataset <- function(rds_file) {
  db_vol <- "/Volumes/strategyunit/csds_fb/csds_patients/"
  filename <- paste0(rds_file, ".rds")
  brickster::db_volume_read(paste0(db_vol, filename), filename)
  readr::read_rds(filename)
}

nat_contacts_data <- read_db_dataset("nat_contacts_final")
icb_contacts_data <- read_db_dataset("icb_contacts_final")
nat_patients_data <- read_db_dataset("nat_patients_final")
icb_patients_data <- read_db_dataset("icb_patients_final")

usethis::use_data(
  nat_contacts_data,
  icb_contacts_data,
  nat_patients_data,
  icb_patients_data,
  compress = "xz"
)
