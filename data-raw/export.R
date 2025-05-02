write_db_dataset_to_rds <- function(rds_file) {
  db_vol <- "/Volumes/strategyunit/csds_fb/csds_patients/"
  filename <- paste0(rds_file, ".rds")
  brickster::db_volume_read(paste0(db_vol, filename), filename)
  invisible(TRUE)
}

write_icb_rds_to_parquet <- function(x) {
  paste0(x, ".rds") |>
    readr::read_rds() |>
    dplyr::mutate(
      dplyr::across("icb22nm", \(x) sub("^(NHS )(.*)( Integ.*)$", "\\2 ICB", x))
    ) |>
    arrow::write_dataset(x, partitioning = "icb22cdh")
}

write_nat_rds_to_parquet <- function(x) {
  paste0(x, ".rds") |>
    readr::read_rds() |>
    arrow::write_dataset(x)
}


write_db_dataset_to_rds("nat_contacts_final")
write_db_dataset_to_rds("icb_contacts_final")
write_db_dataset_to_rds("nat_patients_final")
write_db_dataset_to_rds("icb_patients_final")

write_nat_rds_to_parquet("nat_contacts_final")
write_icb_rds_to_parquet("icb_contacts_final")
write_nat_rds_to_parquet("nat_patients_final")
write_icb_rds_to_parquet("icb_patients_final")
