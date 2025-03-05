app_ui = function() {
  bslib::page_sidebar(
    title = "Communities demographic growth tool",
    lang = "en",
    theme = bslib::bs_theme(bootswatch = "sketchy"),
    sidebar = bslib::sidebar(
      shiny::selectInput(
        "icb",
        "Select ICB",
        choices = icb_list()
      )
    ),
    bslib::layout_column_wrap(
      width = 0.5,
      height = 300,
      bslib::card(
        full_screen = TRUE,
        bslib::card_header(
          "Projected contacts (national) by broad age group, 2022/23 - 2042/43"
        ),
        shiny::plotOutput("national_contacts_by_year"),
      ),

      bslib::card(
        full_screen = TRUE,
        bslib::card_header("ICB-level contacts projection by financial year"),
        shiny::plotOutput("icb_contacts_by_year")
      ),

      bslib::card(
        full_screen = TRUE,
        bslib::card_header(
          "Projected % change in total contacts over period, by age group"
        ),
        shiny::plotOutput("percent_change_by_age")
      ),

      bslib::card(
        full_screen = TRUE,
        bslib::card_header(
          "Contacts per 1000 population, 2022/23, by age group"
        ),
        shiny::plotOutput("contacts_per_population")
      )
    )
  )
}
