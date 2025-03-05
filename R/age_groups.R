add_age_groups <- function(.data) {
  .data |>
    dplyr::arrange(dplyr::pick("age_int")) |>
    dplyr::mutate(
      age_group_cat = dplyr::case_when(
        age_int == 0L ~ "0",
        age_int %in% seq(1L, 4L) ~ "1-4",
        age_int %in% seq(5L, 17L) ~ "5-17",
        age_int %in% seq(18L, 34L) ~ "18-34",
        age_int %in% seq(35L, 49L) ~ "35-49",
        age_int %in% seq(50L, 64L) ~ "50-64",
        age_int %in% seq(65L, 74L) ~ "65-74",
        age_int %in% seq(75L, 84L) ~ "75-84",
        .default = "85+"
      ),
      across(age_group_cat, forcats::fct_inorder),
      .keep = "unused"
    )
}

add_broad_age_groups <- function(.data) {
  .data |>
    dplyr::arrange(dplyr::pick("age_int")) |>
    dplyr::mutate(
      broad_age_cat = dplyr::case_when(
        age_int %in% seq(0L, 4L) ~ "0-4",
        age_int %in% seq(5L, 17L) ~ "5-17",
        age_int %in% seq(18L, 64L) ~ "18-64",
        age_int %in% seq(65L, 84L) ~ "65-84",
        .default = "85+"
      ),
      across(broad_age_cat, forcats::fct_inorder),
      .keep = "unused"
    )
}
