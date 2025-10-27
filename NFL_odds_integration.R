# ============================================================================
# NFL BETTING MODEL - SPORTSDATA.IO ODDS INTEGRATION (ENHANCED)
# ============================================================================
# Purpose: Fetch real-time odds and compare against model predictions
# 
# WORKFLOW:
#   1. Run NFL_model_master_pipeline.R first (creates predictions)
#   2. Then run this script (compares predictions vs Vegas odds)
#
# This script expects CSV files in: BASE_DIR/weekX/matchup_analysis/
#   - betting_recommendations_enhanced.csv
#   - matchup_summary_enhanced.csv
# ============================================================================

# Clear environment
rm(list = ls())
gc()

# ============================================================================
# CONFIGURATION
# ============================================================================

SPORTSDATA_API_KEY <- "39cfe83be78c4bcca9afb2eb9d6cf7ca"
SEASON <- 2025
WEEK <- 8
BASE_DIR <- "C:/Users/Patsc/Documents/nfl_model_v2"  # Fixed: use forward slashes or double-backslashes
MIN_EDGE_THRESHOLD <- 1.5

cat("\n")
cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║     NFL BETTING MODEL - SPORTSDATA.IO INTEGRATION              ║\n")
cat("║     Week:", WEEK, "                                                    ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n")
cat("\n")

# ============================================================================
# LOAD PACKAGES
# ============================================================================

cat("Loading required packages...\n")

required_packages <- c("httr", "jsonlite", "dplyr", "tidyr", "writexl", "ggplot2")

for (pkg in required_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
  suppressPackageStartupMessages(library(pkg, character.only = TRUE))
}

cat("✓ Packages loaded!\n\n")

# ============================================================================
# LOAD MODEL PREDICTIONS (ENHANCED VERSION)
# ============================================================================

cat("Loading model predictions...\n")

matchup_dir <- file.path(BASE_DIR, paste0('week', WEEK), 'matchup_analysis')

# Try to load enhanced version first, fall back to standard
enhanced_file <- file.path(matchup_dir, "betting_recommendations_enhanced.csv")
standard_file <- file.path(matchup_dir, "betting_recommendations.csv")

if (file.exists(enhanced_file)) {
  model_predictions <- read.csv(enhanced_file)
  cat("✓ Using ENHANCED predictions with weather/injury adjustments\n")
  using_enhanced <- TRUE
} else if (file.exists(standard_file)) {
  model_predictions <- read.csv(standard_file)
  cat("✓ Using standard predictions\n")
  using_enhanced <- FALSE
} else {
  stop("\n❌ ERROR: Run NFL_model_master_pipeline.R first!\n")
}

# Load matchup summary for additional context
matchup_summary_enhanced <- file.path(matchup_dir, "matchup_summary_enhanced.csv")
matchup_summary_standard <- file.path(matchup_dir, "matchup_summary.csv")

if (file.exists(matchup_summary_enhanced)) {
  matchup_summary <- read.csv(matchup_summary_enhanced)
  cat("✓ Loaded enhanced matchup data\n")
} else if (file.exists(matchup_summary_standard)) {
  matchup_summary <- read.csv(matchup_summary_standard)
  cat("✓ Loaded standard matchup data\n")
} else {
  matchup_summary <- NULL
  cat("⚠ Matchup summary not found\n")
}

cat("✓ Loaded", nrow(model_predictions), "predictions\n\n")

# ============================================================================
# FETCH ODDS FROM SPORTSDATA.IO
# ============================================================================

cat("Fetching odds from SportsData.io...\n")

fetch_sportsdata_odds <- function(api_key, season, week) {
  
  # SportsData.io endpoint for odds by week
  base_url <- "https://api.sportsdata.io/v3/nfl/odds/json/GameOddsByWeek"
  url <- paste0(base_url, "/", season, "/", week)
  
  response <- httr::GET(
    url = url,
    httr::add_headers("Ocp-Apim-Subscription-Key" = api_key)
  )
  
  if (httr::status_code(response) != 200) {
    stop("API Error: Status ", httr::status_code(response), 
         "\nMessage: ", httr::content(response, "text"))
  }
  
  # Parse response
  odds_data <- httr::content(response, "text", encoding = "UTF-8") %>%
    jsonlite::fromJSON(flatten = TRUE)
  
  return(odds_data)
}

tryCatch({
  odds_raw <- fetch_sportsdata_odds(SPORTSDATA_API_KEY, SEASON, WEEK)
  
  if (is.null(odds_raw) || length(odds_raw) == 0 || nrow(odds_raw) == 0) {
    stop("No data returned from API for Week ", WEEK)
  }
  
  cat("✓ Fetched odds data for", nrow(odds_raw), "games\n\n")
  
}, error = function(e) {
  stop("\n❌ Error fetching odds:\n", e$message, 
       "\n\nTroubleshooting:\n",
       "  1. Check your API key is valid\n",
       "  2. Verify you have access to NFL Odds data (may require paid subscription)\n",
       "  3. Check if Week ", WEEK, " data is available yet\n",
       "  4. Try: https://api.sportsdata.io/v3/nfl/scores/json/Scores/", SEASON, "/", WEEK, "\n")
})

# ============================================================================
# PROCESS SPORTSDATA.IO ODDS
# ============================================================================

cat("Processing odds data...\n")

# SportsData.io returns nested PregameOdds - we need to extract spreads
process_game_odds <- function(game_row) {
  
  # Basic game info - use correct field names from SportsData.io
  game_info <- list(
    game_key = game_row$GameId,
    home_team = game_row$HomeTeamName,
    away_team = game_row$AwayTeamName,
    game_date = game_row$DateTime,
    status = game_row$Status
  )
  
  # Extract odds from PregameOdds
  if (is.null(game_row$PregameOdds) || length(game_row$PregameOdds) == 0) {
    return(NULL)
  }
  
  pregame_odds <- game_row$PregameOdds
  if (!is.data.frame(pregame_odds)) {
    pregame_odds <- as.data.frame(pregame_odds)
  }
  
  # Find consensus or major sportsbook odds
  priority_books <- c("Consensus", "DraftKings", "FanDuel", "BetMGM", "Caesars")
  
  best_odds <- NULL
  for (book in priority_books) {
    book_odds <- pregame_odds[pregame_odds$Sportsbook == book, ]
    if (nrow(book_odds) > 0) {
      best_odds <- book_odds[1, ]
      break
    }
  }
  
  # If no priority book found, use first available
  if (is.null(best_odds) && nrow(pregame_odds) > 0) {
    best_odds <- pregame_odds[1, ]
    if (best_odds$Sportsbook == "Scrambled") {
      best_odds$Sportsbook <- "Market Average (Free Tier)"
    }
  }
  
  if (is.null(best_odds)) {
    return(NULL)
  }
  
  # Extract spread data
  game_info$sportsbook <- best_odds$Sportsbook
  game_info$home_spread <- as.numeric(best_odds$HomePointSpread)
  game_info$away_spread <- as.numeric(best_odds$AwayPointSpread)
  game_info$home_spread_odds <- as.numeric(best_odds$HomePointSpreadPayout)
  game_info$away_spread_odds <- as.numeric(best_odds$AwayPointSpreadPayout)
  game_info$over_under <- as.numeric(best_odds$OverUnder)
  
  # Validate we got actual numeric values
  if (is.na(game_info$home_spread) || is.na(game_info$away_spread)) {
    return(NULL)
  }
  
  return(game_info)
}

# Process all games
all_games <- list()
failed_games <- 0

for (i in 1:nrow(odds_raw)) {
  game_odds <- process_game_odds(odds_raw[i, ])
  if (!is.null(game_odds)) {
    all_games[[length(all_games) + 1]] <- game_odds
  } else {
    failed_games <- failed_games + 1
  }
}

cat("Processed", nrow(odds_raw), "games -", length(all_games), "with odds,", failed_games, "without odds\n")

if (length(all_games) == 0) {
  stop("\n❌ No spread data available. Week ", WEEK, " odds may not be posted yet.\n")
}

# Convert to dataframe
odds_df <- do.call(rbind, lapply(all_games, function(x) {
  data.frame(
    game_key = if (is.null(x$game_key)) NA else x$game_key,
    home_team = if (is.null(x$home_team)) NA else x$home_team,
    away_team = if (is.null(x$away_team)) NA else x$away_team,
    sportsbook = if (is.null(x$sportsbook)) "Unknown" else x$sportsbook,
    home_spread = if (is.null(x$home_spread) || length(x$home_spread) == 0) NA else as.numeric(x$home_spread),
    away_spread = if (is.null(x$away_spread) || length(x$away_spread) == 0) NA else as.numeric(x$away_spread),
    home_spread_odds = if (is.null(x$home_spread_odds) || length(x$home_spread_odds) == 0) NA else as.numeric(x$home_spread_odds),
    away_spread_odds = if (is.null(x$away_spread_odds) || length(x$away_spread_odds) == 0) NA else as.numeric(x$away_spread_odds),
    over_under = if (is.null(x$over_under) || length(x$over_under) == 0) NA else as.numeric(x$over_under),
    stringsAsFactors = FALSE
  )
}))

# Remove rows with missing critical data
odds_df <- odds_df %>%
  filter(!is.na(home_team), !is.na(away_team), !is.na(home_spread))

cat("✓ Extracted spreads for", nrow(odds_df), "games from", 
    length(unique(odds_df$sportsbook)), "sportsbook(s)\n")
cat("   Using:", paste(unique(odds_df$sportsbook), collapse = ", "), "\n\n")

# ============================================================================
# STANDARDIZE TEAM NAMES
# ============================================================================

cat("Matching team names...\n")

odds_df <- odds_df %>%
  mutate(matchup_key = paste(away_team, "at", home_team))

cat("✓ Team names ready for matching\n")
cat("   Sample matchups from odds:\n")
for (i in 1:min(3, nrow(odds_df))) {
  cat("   -", odds_df$matchup_key[i], "\n")
}
cat("\n")

# ============================================================================
# MERGE WITH MODEL PREDICTIONS
# ============================================================================

cat("Merging with model predictions...\n")

# The CSV already has matchup_key, but we need to ensure format matches odds_df
# Odds uses "at" while CSV might use "@" - standardize to "at" for matching
model_predictions <- model_predictions %>%
  mutate(matchup_key = paste(away_team, "at", home_team))

betting_analysis <- model_predictions %>%
  inner_join(odds_df, by = "matchup_key")

if (nrow(betting_analysis) == 0) {
  cat("\n⚠️  Could not match odds with model predictions.\n")
  cat("   Model predictions:\n")
  print(head(model_predictions$matchup_key))
  cat("\n   Odds matchups:\n")
  print(head(odds_df$matchup_key))
  stop("\n❌ Team names don't match. Check abbreviations.\n")
}

cat("✓ Matched", nrow(betting_analysis), "games\n\n")

# Add enhanced context if available
if (!is.null(matchup_summary) && nrow(matchup_summary) > 0) {
  # Standardize matchup_key format in matchup_summary to match
  matchup_summary <- matchup_summary %>%
    mutate(matchup_key = paste(away_team, "at", home_team))
  
  # Check if enhanced columns exist
  if ("weather_adj" %in% names(matchup_summary)) {
    betting_analysis <- betting_analysis %>%
      left_join(
        matchup_summary %>% select(matchup_key, weather_adj, rest_differential, travel_adj),
        by = "matchup_key"
      )
    cat("✓ Added weather and rest context\n\n")
  }
}

betting_analysis <- betting_analysis %>%
  mutate(
    vegas_line = home_spread,
    # model_line already exists from the CSV - no need to create it
    edge = model_line - vegas_line,
    abs_edge = abs(edge),
    
    # Use the correct column names from the join
    bet_side = case_when(
      edge >= MIN_EDGE_THRESHOLD ~ home_team.y,
      edge <= -MIN_EDGE_THRESHOLD ~ away_team.y,
      TRUE ~ "PASS"
    ),
    
    bet_spread = case_when(
      edge >= MIN_EDGE_THRESHOLD ~ home_spread,
      edge <= -MIN_EDGE_THRESHOLD ~ away_spread,
      TRUE ~ NA_real_
    ),
    
    bet_odds = case_when(
      edge >= MIN_EDGE_THRESHOLD ~ home_spread_odds,
      edge <= -MIN_EDGE_THRESHOLD ~ away_spread_odds,
      TRUE ~ NA_real_
    ),
    
    ev_tier = case_when(
      abs_edge >= 5 ~ "🔥 ELITE",
      abs_edge >= 3.5 ~ "⭐ STRONG",
      abs_edge >= MIN_EDGE_THRESHOLD ~ "✓ GOOD",
      TRUE ~ "❌ PASS"
    ),
    
    bet_recommendation = if_else(
      bet_side != "PASS",
      paste0("BET ", bet_side, " ", sprintf("%+.1f", bet_spread), " (", sprintf("%+.0f", bet_odds), ")"),
      "NO BET"
    )
  ) %>%
  # Clean up the column names
  rename(
    away_team = away_team.y,
    home_team = home_team.y
  )

# ============================================================================
# FILTER PROFITABLE BETS WITH CONTEXT
# ============================================================================

profitable_bets <- betting_analysis %>%
  filter(ev_tier != "❌ PASS") %>%
  arrange(desc(abs_edge))

# Add context notes if available
if ("weather_impact" %in% names(profitable_bets)) {
  # Add is_thursday if it doesn't exist
  if (!"is_thursday" %in% names(profitable_bets)) {
    profitable_bets$is_thursday <- FALSE
  }
  
  profitable_bets <- profitable_bets %>%
    mutate(
      context_notes = case_when(
        !is.na(weather_impact) & weather_impact >= 2 ~ "⛈️ Weather impact",
        !is.na(is_thursday) & is_thursday ~ "📅 Thursday game",
        !is.na(home_days_rest) & !is.na(away_days_rest) & 
          abs(home_days_rest - away_days_rest) > 3 ~ "😴 Rest mismatch",
        TRUE ~ ""
      )
    )
}

profitable_bets <- profitable_bets %>%
  select(
    matchup_key, ev_tier, confidence,
    bet_recommendation, edge, vegas_line, model_line,
    sportsbook, home_team, away_team, over_under,
    contains("context"), contains("weather"), contains("rest"), contains("notes")
  )

cat("✓ Found", nrow(profitable_bets), "profitable bets\n\n")

# ============================================================================
# CREATE SUMMARY
# ============================================================================

summary_stats <- data.frame(
  Metric = c(
    "Total Games Analyzed",
    "Profitable Bets Found",
    "Elite Value Bets",
    "Strong Value Bets",
    "Good Value Bets",
    "Average Edge (Profitable)",
    "Largest Edge"
  ),
  Value = c(
    nrow(betting_analysis),
    nrow(profitable_bets),
    sum(betting_analysis$ev_tier == "🔥 ELITE"),
    sum(betting_analysis$ev_tier == "⭐ STRONG"),
    sum(betting_analysis$ev_tier == "✓ GOOD"),
    if(nrow(profitable_bets) > 0) round(mean(profitable_bets$abs_edge, na.rm = TRUE), 2) else 0,
    round(max(betting_analysis$abs_edge), 2)
  )
)

# ============================================================================
# EXPORT RESULTS
# ============================================================================

cat("Exporting results...\n")

odds_output_dir <- file.path(BASE_DIR, paste0('week', WEEK), 'odds_analysis')
if (!dir.exists(odds_output_dir)) {
  dir.create(odds_output_dir, recursive = TRUE)
}

# Excel file
write_xlsx(
  list(
    Summary = summary_stats,
    Profitable_Bets = profitable_bets,
    All_Games = betting_analysis %>% 
      select(matchup_key, ev_tier, edge, vegas_line, model_line, 
             bet_recommendation, confidence, sportsbook, over_under,
             contains("weather"), contains("rest")) %>%
      arrange(desc(abs(edge)))
  ),
  file.path(odds_output_dir, paste0("betting_picks_week", WEEK, ".xlsx"))
)

# CSV files
write.csv(profitable_bets, 
          file.path(odds_output_dir, "profitable_bets.csv"), 
          row.names = FALSE)

write.csv(betting_analysis %>% 
            select(matchup_key, ev_tier, edge, vegas_line, model_line, 
                   bet_recommendation, confidence, sportsbook) %>%
            arrange(desc(abs(edge))), 
          file.path(odds_output_dir, "all_games_analysis.csv"), 
          row.names = FALSE)

cat("✓ Files exported\n\n")

# ============================================================================
# CREATE ENHANCED VISUALIZATION
# ============================================================================

cat("Creating visualization...\n")

if (nrow(betting_analysis) > 0) {
  # Add weather/context markers if available
  if ("weather_impact" %in% names(betting_analysis)) {
    # Add is_thursday if it doesn't exist
    if (!"is_thursday" %in% names(betting_analysis)) {
      betting_analysis$is_thursday <- FALSE
    }
    betting_analysis <- betting_analysis %>%
      mutate(has_context = (weather_impact > 0) | (is_thursday))
  } else {
    betting_analysis <- betting_analysis %>%
      mutate(has_context = FALSE)
  }
  
  edge_plot <- betting_analysis %>%
    mutate(
      game_label = paste0(away_team, " @ ", home_team),
      has_value = abs_edge >= MIN_EDGE_THRESHOLD
    ) %>%
    arrange(desc(abs(edge))) %>%
    head(min(12, nrow(betting_analysis))) %>%
    ggplot(aes(x = reorder(game_label, edge), y = edge, fill = has_value)) +
    geom_col() +
    geom_hline(yintercept = c(-MIN_EDGE_THRESHOLD, MIN_EDGE_THRESHOLD), 
               linetype = "dashed", color = "red", linewidth = 1) +
    geom_hline(yintercept = 0, color = "black") +
    scale_fill_manual(
      values = c("TRUE" = "#00AA00", "FALSE" = "#CCCCCC"),
      labels = c("TRUE" = "Value Bet", "FALSE" = "No Value")
    ) +
    coord_flip() +
    labs(
      title = paste("Week", WEEK, "- Model vs Vegas Spreads (Enhanced)"),
      subtitle = paste("Positive = Bet Home | Negative = Bet Away | Min Edge:", MIN_EDGE_THRESHOLD, "pts"),
      x = NULL,
      y = "Edge (Model Line - Vegas Line)",
      fill = NULL,
      caption = paste("Source:", unique(betting_analysis$sportsbook)[1], "| Includes weather/rest adjustments")
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold", size = 16, hjust = 0.5),
      plot.subtitle = element_text(size = 10, hjust = 0.5),
      plot.caption = element_text(size = 9, hjust = 1),
      legend.position = "bottom"
    )
  
  ggsave(file.path(odds_output_dir, "betting_edges.png"), 
         edge_plot, width = 12, height = 8, dpi = 300)
  
  cat("✓ Visualization saved\n\n")
}

# ============================================================================
# PRINT RESULTS
# ============================================================================

cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║  BETTING ANALYSIS COMPLETE                                     ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n\n")

cat("📊 SUMMARY:\n")
print(summary_stats, row.names = FALSE)

cat("\n\n🎯 TOP BETTING PICKS:\n")
cat("════════════════════════════════════════════════════════════════\n\n")

if (nrow(profitable_bets) > 0) {
  for (i in 1:min(5, nrow(profitable_bets))) {
    bet <- profitable_bets[i, ]
    cat(sprintf("%s  %s\n", bet$ev_tier, bet$matchup_key))
    cat(sprintf("   %s\n", bet$bet_recommendation))
    cat(sprintf("   Edge: %.2f pts | Vegas: %.2f | Model: %.2f\n", 
                bet$edge, bet$vegas_line, bet$model_line))
    cat(sprintf("   Model Confidence: %s | Sportsbook: %s\n", 
                bet$confidence, bet$sportsbook))
    cat(sprintf("   Over/Under: %.2f\n", bet$over_under))
    
    # Show context if available
    if ("weather_impact" %in% names(bet) && !is.na(bet$weather_impact) && bet$weather_impact > 0) {
      cat(sprintf("   ⛈️  Weather Impact: %d/3\n", bet$weather_impact))
    }
    if ("is_thursday" %in% names(bet) && !is.na(bet$is_thursday) && bet$is_thursday) {
      cat("   📅 Thursday Night Game\n")
    }
    if ("context_notes" %in% names(bet) && !is.na(bet$context_notes) && bet$context_notes != "") {
      cat(sprintf("   ℹ️  %s\n", bet$context_notes))
    }
    cat("\n")
  }
} else {
  cat("No profitable bets found this week.\n")
  cat("The model and Vegas are in close agreement.\n\n")
}

cat("════════════════════════════════════════════════════════════════\n")
cat("\n📁 Files saved to:", odds_output_dir, "\n")
cat("   - betting_picks_week", WEEK, ".xlsx\n", sep = "")
cat("   - profitable_bets.csv\n")
cat("   - all_games_analysis.csv\n")
cat("   - betting_edges.png\n")

if (using_enhanced) {
  cat("\n✨ Using ENHANCED model with weather, injuries, and rest adjustments!\n")
} else {
  cat("\n⚠️  Using standard model. Run enhanced pipeline for better predictions.\n")
}

cat("\n✓ Done! Good luck! 🏈💰\n\n")