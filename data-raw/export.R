read_db_dataset <- function(rds_file) {
  db_vol <- "/Volumes/strategyunit/csds_fb/csds_patients/"
  filename <- paste0(rds_file, ".rds")
  brickster::db_volume_read(paste0(db_vol, filename), filename)
  readr::read_rds(filename)
}

csds_nat_contacts <- read_db_dataset("csds_nat_contacts_count")
csds_icb_contacts <- read_db_dataset("csds_icb_contacts_count")
csds_nat_patients <- read_db_dataset("csds_nat_patients_count")
csds_icb_patients <- read_db_dataset("csds_icb_patients_count")

usethis::use_data(
  csds_nat_contacts,
  csds_icb_contacts,
  csds_nat_patients,
  csds_icb_patients,
  compress = "xz"
)
