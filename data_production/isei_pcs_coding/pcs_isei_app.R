library(shiny)
library(tidyverse)

# --- Load data ---
results <- read_csv("candidates.csv")

# Build lookup: one row per observation with nested candidates
observations <- results %>%
  group_by(id, reponse) %>%
  arrange(rank) %>%
  summarise(
    candidates = list(tibble(rank, code, libelle, similarity)),
    .groups = "drop"
  )

n_obs <- nrow(observations)

# Full label list for search (deduplicated)
all_labels <- read_csv("labels_pcs.csv")
# Expects columns: code, libelle_m, libelle_f — adjust if different
all_labels_vec <- all_labels %>%
  pivot_longer(cols = c(libelle_m, libelle_f),
               names_to = "genre", values_to = "libelle") %>%
  filter(!is.na(libelle)) %>%
  distinct(libelle) %>%
  arrange(libelle) %>%
  pull(libelle)

# --- UI ---
ui <- fluidPage(
  tags$head(
    tags$style(HTML("
      body { background: #f5f5f0; font-family: 'Helvetica Neue', Helvetica, sans-serif; }
      .main-container { max-width: 900px; margin: 0 auto; padding: 20px; }
      .header-bar {
        display: flex; justify-content: space-between; align-items: center;
        flex-wrap: wrap; gap: 8px;
        margin-bottom: 20px; padding: 10px 0;
        border-bottom: 2px solid #333;
      }
      .progress-text { font-size: 14px; color: #666; }
      .controls-row {
        display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
        margin-bottom: 15px;
      }
      .filter-toggle {
        background: none; border: 1px solid #999; border-radius: 4px;
        padding: 6px 14px; cursor: pointer; color: #666; font-size: 13px;
      }
      .filter-toggle:hover { background: #eee; }
      .filter-toggle.active {
        background: #fff3cd; border-color: #d4a017; color: #856404;
      }
      .goto-input {
        width: 70px; padding: 5px 8px; border: 1px solid #999;
        border-radius: 4px; font-size: 13px; text-align: center;
      }
      .goto-btn {
        background: none; border: 1px solid #999; border-radius: 4px;
        padding: 6px 12px; cursor: pointer; color: #666; font-size: 13px;
      }
      .goto-btn:hover { background: #eee; }
      .reponse-label {
        font-size: 32px; font-weight: 700; color: #1a1a1a;
        padding: 20px 0 25px 0; text-align: center;
      }
      .candidates-grid {
        display: grid; grid-template-columns: 1fr 1fr; gap: 10px;
      }
      .candidate-btn {
        background: #fff; border: 1px solid #ccc; border-radius: 6px;
        padding: 14px 16px; cursor: pointer; text-align: left;
        font-size: 15px; color: #333; transition: all 0.15s;
        width: 100%;
      }
      .candidate-btn:hover {
        background: #e8f0fe; border-color: #4a7dfc;
      }
      .similarity { color: #999; font-size: 13px; }
      .skip-btn {
        background: none; border: 1px solid #999; border-radius: 4px;
        padding: 8px 16px; cursor: pointer; color: #666; font-size: 13px;
      }
      .skip-btn:hover { background: #eee; }
      .save-btn {
        background: #2d6a2e; color: white; border: none; border-radius: 4px;
        padding: 8px 20px; cursor: pointer; font-size: 14px;
      }
      .save-btn:hover { background: #245524; }
      .done-box {
        text-align: center; padding: 60px; font-size: 20px; color: #333;
      }
      .annotated-badge {
        display: inline-block; background: #d4edda; color: #155724;
        font-size: 12px; padding: 2px 8px; border-radius: 10px;
        margin-left: 8px;
      }
      .filter-info {
        font-size: 13px; color: #856404; background: #fff3cd;
        padding: 6px 12px; border-radius: 4px; display: inline-block;
      }
      .search-section {
        margin-top: 20px; padding-top: 15px;
        border-top: 1px dashed #bbb;
      }
      .search-section h4 {
        font-size: 14px; color: #666; margin-bottom: 8px; font-weight: 600;
      }
      .search-input {
        width: 100%; padding: 10px 12px; border: 1px solid #999;
        border-radius: 4px; font-size: 15px; box-sizing: border-box;
      }
      .search-results {
        max-height: 300px; overflow-y: auto; margin-top: 8px;
      }
      .search-result-btn {
        background: #fafafa; border: 1px solid #ddd; border-radius: 4px;
        padding: 10px 14px; cursor: pointer; text-align: left;
        font-size: 14px; color: #333; transition: all 0.15s;
        width: 100%; margin-bottom: 4px; display: block;
      }
      .search-result-btn:hover {
        background: #e8f0fe; border-color: #4a7dfc;
      }
      .search-hint {
        font-size: 12px; color: #999; margin-top: 4px;
      }
    ")),
    tags$script(HTML("
      Shiny.addCustomMessageHandler('toggle_class', function(msg) {
        var el = document.getElementById(msg.id);
        if (msg.active) el.classList.add('active');
        else el.classList.remove('active');
      });
    "))
  ),
  
  div(class = "main-container",
      div(class = "header-bar",
          div(class = "progress-text", textOutput("progress", inline = TRUE)),
          div(
            fileInput("import_btn", label = NULL, accept = ".csv",
                      buttonLabel = "Importer CSV", placeholder = ""),
            tags$style(HTML("
          #import_btn { display: inline-block; margin: 0; width: auto; }
          #import_btn .input-group { display: flex; align-items: center; }
          #import_btn .form-control { display: none; }
          #import_btn .btn { background: #2c5282; color: white; border: none;
            border-radius: 4px; padding: 8px 16px; font-size: 14px; }
          #import_btn .btn:hover { background: #1a365d; }
          #import_btn .progress { display: none; }
        ")),
            downloadButton("save_btn", "Sauvegarder CSV", class = "save-btn")
          )
      ),
      div(class = "controls-row",
          actionButton("prev_btn", "< Précédent", class = "skip-btn"),
          actionButton("skip_btn", "Passer >", class = "skip-btn"),
          tags$span(style = "margin-left: 10px; color: #999;", "|"),
          tags$input(id = "goto_num", type = "number", class = "goto-input",
                     min = 1, max = n_obs, placeholder = "n°"),
          actionButton("goto_btn", "Aller", class = "goto-btn"),
          tags$span(style = "margin-left: 10px; color: #999;", "|"),
          actionButton("filter_unannotated", "Non annotées seulement",
                       class = "filter-toggle")
      ),
      uiOutput("filter_info_ui"),
      uiOutput("main_ui"),
      div(class = "search-section",
          h4("Rechercher dans la liste complète"),
          textInput("search_label", label = NULL,
                    placeholder = "Tapez au moins 2 caractères..."),
          tags$style(HTML("#search_label { width: 100%; padding: 10px 12px; border: 1px solid #999; border-radius: 4px; font-size: 15px; }")),
          uiOutput("search_results_ui")
      )
  )
)

# --- Server ---
server <- function(input, output, session) {
  current <- reactiveVal(1)
  annotations <- reactiveVal(setNames(rep(NA_character_, n_obs), observations$id))
  show_only_unannotated <- reactiveVal(FALSE)
  
  # Import previous annotations
  observeEvent(input$import_btn, {
    req(input$import_btn)
    imported <- read_csv(input$import_btn$datapath, show_col_types = FALSE)
    ann <- annotations()
    for (r in seq_len(nrow(imported))) {
      obs_id <- as.character(imported$id[r])
      if (obs_id %in% names(ann)) {
        val <- imported$chosen_libelle[r]
        ann[obs_id] <- if (is.na(val)) "__NA__" else val
      }
    }
    annotations(ann)
    showNotification(
      paste0(nrow(imported), " annotations importées."),
      type = "message", duration = 3
    )
  })
  
  # Filtered indices
  filtered_indices <- reactive({
    if (show_only_unannotated()) {
      ann <- annotations()
      which(is.na(ann))
    } else {
      seq_len(n_obs)
    }
  })
  
  # Position within filtered list
  filter_pos <- reactiveVal(1)
  
  # Current real index (into observations)
  current_real <- reactive({
    fi <- filtered_indices()
    pos <- filter_pos()
    if (pos < 1 || pos > length(fi)) return(NA_integer_)
    fi[pos]
  })
  
  # Toggle filter
  observeEvent(input$filter_unannotated, {
    new_val <- !show_only_unannotated()
    show_only_unannotated(new_val)
    filter_pos(1)
    session$sendCustomMessage("toggle_class", list(
      id = "filter_unannotated", active = new_val
    ))
  })
  
  output$progress <- renderText({
    ann <- annotations()
    done <- sum(!is.na(ann))
    fi <- filtered_indices()
    pos <- filter_pos()
    if (show_only_unannotated()) {
      paste0(pos, " / ", length(fi), " restantes  —  ",
             done, " / ", n_obs, " annotée(s)")
    } else {
      i <- current_real()
      if (is.na(i)) i <- 0
      paste0("Observation ", i, " / ", n_obs, "  —  ",
             done, " / ", n_obs, " annotée(s)")
    }
  })
  
  output$filter_info_ui <- renderUI({
    if (show_only_unannotated()) {
      remaining <- length(filtered_indices())
      div(class = "filter-info", style = "margin-bottom: 12px;",
          paste0("Filtre actif : ", remaining, " observations non annotées"))
    }
  })
  
  # Search results
  search_matches <- reactive({
    query <- input$search_label
    if (is.null(query) || nchar(trimws(query)) < 2) return(character(0))
    query <- tolower(trimws(query))
    matches <- all_labels_vec[grepl(query, tolower(all_labels_vec), fixed = TRUE)]
    head(matches, 30)
  })
  
  output$main_ui <- renderUI({
    fi <- filtered_indices()
    pos <- filter_pos()
    
    if (length(fi) == 0) {
      return(div(class = "done-box", "Toutes les observations ont été annotées."))
    }
    if (pos > length(fi)) {
      return(div(class = "done-box",
                 "Fin de la liste. Sauvegardez ou désactivez le filtre."))
    }
    
    i <- fi[pos]
    obs <- observations[i, ]
    cands <- obs$candidates[[1]]
    ann <- annotations()
    current_annotation <- ann[as.character(obs$id)]
    
    buttons <- lapply(seq_len(nrow(cands)), function(j) {
      btn_style <- if (!is.na(current_annotation) &&
                       current_annotation == cands$libelle[j]) {
        "background: #d4edda; border-color: #2d6a2e; font-weight: 600;"
      } else {
        ""
      }
      tags$button(
        class = "candidate-btn", style = btn_style,
        onclick = paste0("Shiny.setInputValue('chosen', {index: ", j,
                         ", nonce: Math.random()})"),
        HTML(paste0(
          cands$libelle[j],
          ' <span class="similarity">(', cands$similarity[j], ")</span>"
        ))
      )
    })
    
    badge <- if (!is.na(current_annotation)) {
      span(class = "annotated-badge", "✓ annoté")
    } else {
      NULL
    }
    
    obs_num <- if (!show_only_unannotated()) {
      span(style = "font-size: 13px; color: #999;",
           paste0("(#", i, ")"))
    }
    
    na_style <- if (!is.na(current_annotation) && current_annotation == "__NA__") {
      "background: #f8d7da; border-color: #c0392b; font-weight: 600; color: #721c24;"
    } else {
      "background: #fff; border: 1px dashed #c0392b; color: #c0392b;"
    }
    
    tagList(
      div(class = "reponse-label", obs$reponse, badge, obs_num),
      div(class = "candidates-grid", buttons),
      div(style = "margin-top: 12px;",
          tags$button(
            class = "candidate-btn", style = na_style,
            onclick = "Shiny.setInputValue('chosen_na', {nonce: Math.random()})",
            "Aucune correspondance (NA)"
          )
      )
    )
  })
  
  # Search results (outside main_ui so the text input doesn't reset)
  output$search_results_ui <- renderUI({
    query <- input$search_label
    if (is.null(query) || nchar(trimws(query)) < 2) return(NULL)
    
    matches <- search_matches()
    if (length(matches) > 0) {
      div(class = "search-results",
          lapply(seq_along(matches), function(k) {
            tags$button(
              class = "search-result-btn",
              onclick = paste0("Shiny.setInputValue('search_chosen',",
                               "{label: '", gsub("'", "\\\\'", matches[k]),
                               "', nonce: Math.random()})"),
              matches[k]
            )
          })
      )
    } else {
      div(class = "search-hint", "Aucun résultat.")
    }
  })
  
  # Handle candidate click
  observeEvent(input$chosen, {
    fi <- filtered_indices()
    pos <- filter_pos()
    if (pos > length(fi)) return()
    
    i <- fi[pos]
    obs <- observations[i, ]
    cands <- obs$candidates[[1]]
    j <- input$chosen$index
    chosen_libelle <- cands$libelle[j]
    
    ann <- annotations()
    ann[as.character(obs$id)] <- chosen_libelle
    annotations(ann)
    
    if (show_only_unannotated()) {
      filter_pos(pos)
    } else {
      if (pos < length(fi)) filter_pos(pos + 1)
    }
  })
  
  # Handle search result click
  observeEvent(input$search_chosen, {
    fi <- filtered_indices()
    pos <- filter_pos()
    if (pos > length(fi)) return()
    
    i <- fi[pos]
    obs <- observations[i, ]
    chosen_libelle <- input$search_chosen$label
    
    ann <- annotations()
    ann[as.character(obs$id)] <- chosen_libelle
    annotations(ann)
    
    if (show_only_unannotated()) {
      filter_pos(pos)
    } else {
      if (pos < length(fi)) filter_pos(pos + 1)
    }
  })
  
  # Handle NA click
  observeEvent(input$chosen_na, {
    fi <- filtered_indices()
    pos <- filter_pos()
    if (pos > length(fi)) return()
    
    i <- fi[pos]
    obs <- observations[i, ]
    
    ann <- annotations()
    ann[as.character(obs$id)] <- "__NA__"
    annotations(ann)
    
    if (show_only_unannotated()) {
      filter_pos(pos)
    } else {
      if (pos < length(fi)) filter_pos(pos + 1)
    }
  })
  
  observeEvent(input$skip_btn, {
    fi <- filtered_indices()
    pos <- filter_pos()
    if (pos < length(fi)) filter_pos(pos + 1)
  })
  
  observeEvent(input$prev_btn, {
    if (filter_pos() > 1) filter_pos(filter_pos() - 1)
  })
  
  # Go to specific observation number
  observeEvent(input$goto_btn, {
    target <- as.integer(input$goto_num)
    if (is.na(target) || target < 1 || target > n_obs) return()
    
    if (show_only_unannotated()) {
      fi <- filtered_indices()
      pos <- match(target, fi)
      if (!is.na(pos)) {
        filter_pos(pos)
      }
    } else {
      filter_pos(target)
    }
  })
  
  output$save_btn <- downloadHandler(
    filename = function() {
      paste0("annotations_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
    },
    content = function(file) {
      ann <- annotations()
      out <- tibble(
        id = names(ann),
        reponse = observations$reponse,
        chosen_libelle = unname(ann)
      ) %>%
        filter(!is.na(chosen_libelle)) %>%
        mutate(chosen_libelle = na_if(chosen_libelle, "__NA__"))
      write_csv(out, file)
    }
  )
}

shinyApp(ui, server)