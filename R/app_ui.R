app_ui <- function() {
  bslib::page_navbar(
    title = "Community services demographic growth",
    lang = "en",
    theme = bslib::bs_theme(brand = TRUE),
    header = add_external_resources(),
    bslib::nav_panel(title = "National", national_ui("national")),
    bslib::nav_panel(title = "ICB", icb_ui("icb")),
    bslib::nav_panel(
      title = "About",
      bslib::layout_column_wrap(
        width = 1 / 3,
        bslib::card(
          bslib::card_header("About the tool"),
          shiny::includeMarkdown(app_sys("www/about.md"))
        ),
        bslib::card(
          bslib::card_header("Community Services Data Set (CSDS)"),
          shiny::includeMarkdown(app_sys("www/csds.md"))
        ),
        bslib::card(
          bslib::card_header("Provider exclusions"),
          shiny::includeMarkdown(app_sys("www/provider-exclusions.md"))
        ),
        bslib::card(
          bslib::card_header("Methodology"),
          shiny::includeMarkdown(app_sys("www/methodology.md"))
        ),
        bslib::card(
          bslib::card_header("Population projections"),
          shiny::includeMarkdown(app_sys("www/projections.md"))
        )
      )
    )
  )
}
