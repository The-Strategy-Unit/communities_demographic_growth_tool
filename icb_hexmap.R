# icb_rgn_lookup <- boundr::lookup("icb", "nhser", lookup_year = 2023, opts = boundr::opts(return_width = "full", query_option = 1)) |>
#   dplyr::select(tidyselect::matches("^(icb|nhs)")) |>
#   dplyr::distinct() |>
#   readr::write_rds("icb_rgn_lookup.rds")

# icb_bounds <- boundr::bounds(
#   "icb",
#   lookup_year = 2023,
#   opts = boundr::opts("BSC", crs = 27700)
# ) |>
#   dplyr::left_join(icb_rgn_lookup, c("icb23cd", "icb23nm")) |>
#   dplyr::relocate("geometry", .after = dplyr::last_col()) |>
#   readr::write_rds("icb_bounds.rds")

icb_rgn_lookup <- readr::read_rds("icb_rgn_lookup.rds")
icb_bounds <- readr::read_rds("icb_bounds.rds")

england_shape <- icb_bounds |>
  sf::st_union() |>
  smoothr::drop_crumbs(threshold = units::set_units(100, km^2)) |>
  sf::st_simplify(preserveTopology = TRUE, dTolerance = 4000) |>
  smoothr::smooth()

icb_centroids <- sf::st_centroid(icb_bounds)

icb_list <- icb_centroids |>
  dplyr::select(!tidyselect::starts_with("nhser23cd")) |>
  tidyr::nest(.by = "nhser23nm")


allocate_hex <- function(results, icb_centroid) {
  grid <- results[["grid"]]
  previous <- results[["allocated"]]
  if (is.null(previous)) {
    touching <- grid
  } else {
    touching <- grid[which(sf::st_touches(grid, sf::st_union(previous), FALSE))]
  }
  chosen_hex <- touching[sf::st_nearest_feature(icb_centroid, touching)]
  new_hex <- sf::st_set_geometry(icb_centroid, chosen_hex)
  allocatd <- dplyr::bind_rows(previous, new_hex)
  new_grid <- sf::st_difference(grid, chosen_hex)
  list(grid = new_grid, allocated = allocatd)
}

allocate_region <- function(results, region) {
  base_grid <- results[["grid"]]
  region_results <- region |>
    dplyr::mutate(r = dplyr::row_number()) |>
    tidyr::nest(.by = "r") |>
    tibble::deframe() |>
    purrr::reduce(allocate_hex, .init = list(grid = base_grid))
  grid <- region_results[["grid"]]
  rgn_hexes <- region_results[["allocated"]]
  allocatd <- dplyr::bind_rows(results[["allocated"]], rgn_hexes)
  margin <- grid[which(sf::st_touches(grid, sf::st_union(rgn_hexes), FALSE))]
  new_grid <- if (is.null(margin)) grid else sfext::st_erase(grid, margin)
  list(grid = new_grid, allocated = allocatd)
}

# Place Devon and Cornwall earlier, to reduce elongation of region
cluster_sw <- function(swdf) {
  nested <- dplyr::nest_by(swdf, dplyr::pick("icb23cdh"), .keep = TRUE)
  c("QR1", "QUY", "QSL", "QJK", "QT6", "QVV", "QOX") |>
    tibble::as_tibble_col("icb23cdh") |>
    dplyr::left_join(nested, "icb23cdh") |>
    dplyr::pull("data") |>
    dplyr::bind_rows()
}

base_grid <- icb_bounds |>
  sf::st_make_grid(cellsize = 50000, square = FALSE, flat_topped = FALSE)

# Manually selected order, to improve shape of clusters and country overall
icb_hexes <- c(
  "London",
  "Midlands",
  "East of England",
  "South West",
  "South East",
  "North West",
  "North East and Yorkshire"
) |>
  tibble::as_tibble_col("nhser23nm") |>
  dplyr::left_join(icb_list, "nhser23nm") |>
  tibble::deframe() |>
  purrr::modify_at("South West", cluster_sw) |>
  purrr::reduce(allocate_region, .init = list(grid = base_grid)) |>
  purrr::pluck("allocated") |>
  dplyr::left_join(icb_rgn_lookup, c("icb23cd", "icb23nm", "icb23cdh"))

# Shift slightly north and west
icb_hexes <- sfext::st_nudge(icb_hexes, nudge_x = 9e3, nudge_y = -3e3)
region_clusters <- icb_hexes |>
  dplyr::summarise(across("geometry", sf::st_union), .by = "nhser23nm")

icb_hexes <- icb_hexes |>
  dplyr::select(!c(tidyselect::starts_with("nhser"), "icb23cd", "icb23nm"))
