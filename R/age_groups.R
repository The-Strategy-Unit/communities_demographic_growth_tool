add_age_groups <- function(.data) {
  .data |>
    dplyr::arrange(dplyr::pick("age_int")) |>
    dplyr::mutate(
      age_group_cat = dplyr::case_when(
        age_int == 0L ~ "0",
        age_int %in% seq(1L, 4L) ~ "1-4",
        age_int %in% seq(5L, 9L) ~ "5-9",
        age_int %in% seq(10L, 15L) ~ "10-15",
        age_int %in% seq(16L, 17L) ~ "16-17",
        age_int %in% seq(18L, 34L) ~ "18-34",
        age_int %in% seq(35L, 49L) ~ "35-49",
        age_int %in% seq(50L, 64L) ~ "50-64",
        age_int %in% seq(65L, 74L) ~ "65-74",
        age_int %in% seq(75L, 84L) ~ "75-84",
        .default = "85+"
      ),
      dplyr::across(.data$age_group_cat, forcats::fct_inorder),
      .keep = "unused"
    )
}

add_broad_age_groups <- function(.data) {
  .data |>
    dplyr::arrange(dplyr::pick("age_int")) |>
    dplyr::mutate(
      broad_age_cat = dplyr::case_when(
        age_int %in% seq(0L, 15L) ~ "0-15",
        age_int %in% seq(16L, 49L) ~ "16-49",
        age_int %in% seq(50L, 74L) ~ "50-74",
        .default = "75+"
      ),
      dplyr::across(broad_age_cat, forcats::fct_inorder),
      .keep = "unused"
    )
}
