app_ui <- function() {
  bslib::page_navbar(
    title = "Community services demographic growth",
    lang = "en",
    theme = bslib::bs_theme(brand = TRUE),
    header = shiny::tagList(
      add_external_resources(),
      shiny::div(
        class = "alert alert-info alert-dismissible mb-3",
        role = "alert",
        "Please see data quality tabs when using these plots.",
        shiny::tags$button(
          type = "button",
          class = "btn-close",
          `data-bs-dismiss` = "alert",
          `aria-label` = "Close"
        )
      )
    ),
    bslib::nav_panel(title = "National", national_ui("national")),
    bslib::nav_panel(title = "ICB", icb_ui("icb")),
    bslib::nav_panel(
      title = "About",
      bslib::layout_columns(
        col_widths = c(-3, 6, -3),
        shiny::includeMarkdown(app_sys("www/methodology.md"))
      )
    )
  )
}
