

broad_age_group_colours <- function() {
  broad_age_groups <- c("0-4", "5-17", "18-64", "65-84", "85+")
  c(
    orange = "#f9bf07",
    charcoal = "#2c2825",
    light_slate = "#b2b7b9",
    blue = "#5881c1",
    red = "#ec6555"
  ) |>
    rlang::set_names(broad_age_groups)
}

duo_colours <- function(icb_name) {
  rlang::set_names(c("#686f73", "#f9bd07"), c("England", icb_name))
}
