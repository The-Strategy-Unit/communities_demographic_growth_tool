age_group_colours <- function() {
  age_groups <- c(
    "0",
    "1-4",
    "5-17",
    "18-34",
    "35-49",
    "50-64",
    "65-74",
    "75-84",
    "85+"
  )
  c(
    orange = "#f9bf07",
    dark_red = "#901d10",
    charcoal = "#2c2825",
    light_blue = "#abc0e0",
    slate = "#686f73",
    dark_charcoal = "#151412",
    blue = "#5881c1",
    light_orange = "#fcdf83",
    red = "#ec6555",
  ) |>
    rlang::set_names(age_groups)
}


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
