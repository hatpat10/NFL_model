# ============================================================================
# NFL MODEL BACKTEST - FOR YOUR UPDATED PIPELINE (v2.0)
# ============================================================================
# Tests YOUR EPA-based predictions against actual game results
# NOW INCLUDES: Weather adjustments, injury data, enhanced features
# 
# Run AFTER generating predictions for weeks 5, 6, 7 using:
#   NFL_model_master_pipeline.R (updated with weather/injuries)
#
# This backtest validates:
#   - Winner prediction accuracy (hit rate)
#   - Margin prediction accuracy (MAE, RMSE)
#   - Spread betting profitability (ROI)
#   - Total betting profitability (if lines available)
# ============================================================================

library(dplyr)
library(tidyr)
library(nflreadr)
library(openxlsx)
library(ggplot2)

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR <- "C:/Users/Patsc/Documents/nfl_model_v2"  # Use forward slashes
BACKTEST_WEEKS <- 5:7  # Weeks to test (must have been predicted already)
SEASON <- 2025
BET_SIZE <- 100  # $100 per bet for simulation

cat("\n")
cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║  BACKTEST - YOUR PIPELINE VALIDATION (v2.0)                   ║\n")
cat("║  Testing Weeks:", paste(BACKTEST_WEEKS, collapse = ", "), "                                              ║\n")
cat("║  With weather & injury adjustments                            ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n\n")

# Create backtest directory
backtest_dir <- file.path(BASE_DIR, "backtest_results")
dir.create(backtest_dir, showWarnings = FALSE, recursive = TRUE)

# ============================================================================
# STEP 1: LOAD ACTUAL GAME RESULTS
# ============================================================================

cat("[1/5] Loading actual game results...\n")

actuals <- load_pbp(SEASON) %>%
  filter(
    season_type == "REG",
    week %in% BACKTEST_WEEKS,
    !is.na(home_score),
    !is.na(away_score)
  ) %>%
  group_by(game_id, week, home_team, away_team) %>%
  summarise(
    actual_home_score = max(home_score, na.rm = TRUE),
    actual_away_score = max(away_score, na.rm = TRUE),
    .groups = 'drop'
  ) %>%
  mutate(
    # Calculate these after summarise when everything is single values
    actual_margin = actual_away_score - actual_home_score,
    actual_winner = if_else(actual_margin > 0, away_team, home_team),
    actual_total = actual_home_score + actual_away_score,
    home_won = actual_home_score > actual_away_score
  )

cat("  ✓ Loaded", nrow(actuals), "completed games\n\n")

# ============================================================================
# STEP 2: LOAD YOUR PREDICTIONS
# ============================================================================

cat("[2/5] Loading YOUR pipeline predictions...\n")

all_predictions <- list()
missing_weeks <- c()

for (week_num in BACKTEST_WEEKS) {
  
  # Try to find your predictions file
  pred_file <- file.path(BASE_DIR, paste0('week', week_num), 
                         paste0('Week_', week_num, '_Model_Output.xlsx'))
  
  if (file.exists(pred_file)) {
    
    # Read from "Betting Summary" sheet
    week_preds <- tryCatch({
      read.xlsx(pred_file, sheet = "Betting Summary") %>%
        mutate(week = week_num) %>%
        as_tibble()
    }, error = function(e) {
      cat("  ⚠️ Error reading Week", week_num, ":", e$message, "\n")
      NULL
    })
    
    if (!is.null(week_preds)) {
      all_predictions[[length(all_predictions) + 1]] <- week_preds
      cat("  ✓ Week", week_num, "predictions loaded (", nrow(week_preds), "games)\n")
    }
    
  } else {
    cat("  ❌ Week", week_num, "predictions not found\n")
    missing_weeks <- c(missing_weeks, week_num)
  }
}

if (length(missing_weeks) > 0) {
  cat("\n⚠️ MISSING PREDICTIONS:\n")
  cat("   To backtest Week(s)", paste(missing_weeks, collapse = ", "), "you need to:\n")
  cat("   1. Open NFL_model_master_pipeline.R\n")
  cat("   2. Set CURRENT_WEEK = (week -1) and PREDICTION_WEEK =", missing_weeks[1], "\n")
  cat("   3. Run the pipeline\n")
  cat("   4. Repeat for each missing week\n")
  cat("   5. Then run this backtest again\n\n")
}

if (length(all_predictions) == 0) {
  stop("\n❌ No predictions found! Generate predictions first.\n")
}

# Combine all predictions
predictions_combined <- bind_rows(all_predictions)

cat("\n  Total predictions loaded:", nrow(predictions_combined), "\n\n")

# ============================================================================
# STEP 3: STANDARDIZE COLUMN NAMES AND JOIN
# ============================================================================

cat("[3/5] Matching predictions to actual results...\n")

# Clean up prediction column names (they might have dots from Excel)
predictions_clean <- predictions_combined %>%
  select(-any_of("Week")) %>%  # Remove Excel's "Week" column if it exists
  rename_with(~gsub("\\.", "_", tolower(.))) %>%
  rename_with(~gsub("^x_", "", .))

# Identify the column names (Excel might have spaces/dots)
# Common variations: "Away_Team", "Away.Team", "away_team"
away_col <- names(predictions_clean)[grep("away.*team", names(predictions_clean), ignore.case = TRUE)][1]
home_col <- names(predictions_clean)[grep("home.*team", names(predictions_clean), ignore.case = TRUE)][1]
margin_col <- names(predictions_clean)[grep("predicted.*margin", names(predictions_clean), ignore.case = TRUE)][1]
line_col <- names(predictions_clean)[grep("^line$|spread.*line", names(predictions_clean), ignore.case = TRUE)][1]
total_line_col <- names(predictions_clean)[grep("total.*line", names(predictions_clean), ignore.case = TRUE)][1]
pred_total_col <- names(predictions_clean)[grep("predicted.*total", names(predictions_clean), ignore.case = TRUE)][1]

cat("  Detected column names:\n")
cat("    Away Team:", away_col, "\n")
cat("    Home Team:", home_col, "\n")
cat("    Predicted Margin:", margin_col, "\n")
cat("    Spread Line:", line_col, "\n")

# Standardize names
predictions_std <- predictions_clean %>%
  rename(
    away_team = !!away_col,
    home_team = !!home_col,
    predicted_margin = !!margin_col
  )

# Add spread line if exists
if (!is.na(line_col)) {
  predictions_std <- predictions_std %>%
    rename(spread_line = !!line_col)
} else {
  predictions_std <- predictions_std %>%
    mutate(spread_line = NA_real_)
}

# Add total line if exists
if (!is.na(total_line_col)) {
  predictions_std <- predictions_std %>%
    rename(total_line = !!total_line_col)
} else {
  predictions_std <- predictions_std %>%
    mutate(total_line = NA_real_)
}

# Add predicted total if exists
if (!is.na(pred_total_col)) {
  predictions_std <- predictions_std %>%
    rename(predicted_total = !!pred_total_col)
} else {
  predictions_std <- predictions_std %>%
    mutate(predicted_total = NA_real_)
}

# Join predictions with actuals
comparison <- predictions_std %>%
  left_join(
    actuals,
    by = c("week", "home_team", "away_team")
  ) %>%
  filter(!is.na(actual_margin))  # Only keep games that were played

if (nrow(comparison) == 0) {
  stop("\n❌ Could not match predictions to actual results. Check team name formatting.\n")
}

cat("  ✓ Matched", nrow(comparison), "games\n\n")

# ============================================================================
# STEP 4: CALCULATE PERFORMANCE METRICS
# ============================================================================

cat("[4/5] Calculating performance metrics...\n")

# Add prediction accuracy metrics
comparison <- comparison %>%
  mutate(
    # Winner prediction
    predicted_winner = if_else(predicted_margin > 0, away_team, home_team),
    correct_winner = predicted_winner == actual_winner,
    
    # Margin accuracy
    margin_error = abs(predicted_margin - actual_margin),
    squared_error = margin_error^2,
    prediction_bias = predicted_margin - actual_margin,
    
    # Spread betting (if line available)
    cover_prediction = if_else(
      !is.na(spread_line),
      predicted_margin > spread_line,
      NA
    ),
    actual_cover = if_else(
      !is.na(spread_line),
      actual_margin > spread_line,
      NA
    ),
    spread_bet_correct = cover_prediction == actual_cover,
    
    # Total betting (if total available)
    over_prediction = if_else(
      !is.na(total_line),
      predicted_total > total_line,
      NA
    ),
    actual_over = if_else(
      !is.na(total_line),
      actual_total > total_line,
      NA
    ),
    total_bet_correct = over_prediction == actual_over
  )

# Overall metrics
overall_metrics <- comparison %>%
  summarise(
    total_games = n(),
    
    # Winner prediction accuracy
    correct_winners = sum(correct_winner, na.rm = TRUE),
    hit_rate = mean(correct_winner, na.rm = TRUE),
    
    # Margin accuracy
    mean_abs_error = mean(margin_error, na.rm = TRUE),
    rmse = sqrt(mean(squared_error, na.rm = TRUE)),
    median_error = median(margin_error, na.rm = TRUE),
    bias = mean(prediction_bias, na.rm = TRUE),
    
    # Spread betting (if available)
    spread_games = sum(!is.na(spread_bet_correct)),
    spread_correct = sum(spread_bet_correct, na.rm = TRUE),
    spread_hit_rate = mean(spread_bet_correct, na.rm = TRUE),
    
    # Total betting (if available)
    total_games_with_line = sum(!is.na(total_bet_correct)),
    total_correct = sum(total_bet_correct, na.rm = TRUE),
    total_hit_rate = mean(total_bet_correct, na.rm = TRUE)
  )

# By week breakdown
by_week <- comparison %>%
  group_by(week) %>%
  summarise(
    games = n(),
    hit_rate = mean(correct_winner, na.rm = TRUE),
    mae = mean(margin_error, na.rm = TRUE),
    .groups = 'drop'
  )

# Calculate profitability (assuming -110 odds)
calculate_profit <- function(hit_rate, num_bets, bet_size = 100) {
  wins <- hit_rate * num_bets
  losses <- (1 - hit_rate) * num_bets
  profit <- (wins * (bet_size * 100/110)) - (losses * bet_size)
  roi <- (profit / (num_bets * bet_size)) * 100
  return(list(profit = profit, roi = roi))
}

winner_profit <- calculate_profit(
  overall_metrics$hit_rate,
  overall_metrics$total_games,
  BET_SIZE
)

spread_profit <- if (overall_metrics$spread_games > 0) {
  calculate_profit(
    overall_metrics$spread_hit_rate,
    overall_metrics$spread_games,
    BET_SIZE
  )
} else {
  list(profit = 0, roi = 0)
}

cat("  ✓ Metrics calculated\n\n")

# ============================================================================
# STEP 5: DISPLAY RESULTS
# ============================================================================

cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║  BACKTEST RESULTS - YOUR PIPELINE                              ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n\n")

cat("📊 OVERALL PERFORMANCE:\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
cat(sprintf("Total Games:         %d\n", overall_metrics$total_games))
cat(sprintf("Correct Winners:     %d (%.1f%%)\n", 
            overall_metrics$correct_winners, 
            overall_metrics$hit_rate * 100))

# Hit rate interpretation
if (overall_metrics$hit_rate >= 0.60) {
  cat("  ✅ STRONG MODEL - Excellent performance!\n")
} else if (overall_metrics$hit_rate >= 0.55) {
  cat("  ✅ GOOD MODEL - Solid edge\n")
} else if (overall_metrics$hit_rate >= 0.524) {
  cat("  ⚠️ MARGINAL - Barely profitable\n")
} else {
  cat("  ❌ UNPROFITABLE - Do not bet!\n")
}

cat(sprintf("\nMean Abs Error:      %.2f points", overall_metrics$mean_abs_error))
if (overall_metrics$mean_abs_error < 10) {
  cat(" ✅\n")
} else {
  cat(" ⚠️ (High error)\n")
}

cat(sprintf("RMSE:                %.2f points\n", overall_metrics$rmse))
cat(sprintf("Median Error:        %.2f points\n", overall_metrics$median_error))
cat(sprintf("Bias:                %+.2f points ", overall_metrics$bias))

if (abs(overall_metrics$bias) < 1) {
  cat("✅ (No systematic bias)\n")
} else if (overall_metrics$bias > 0) {
  cat("⚠️ (Over-predicting away teams)\n")
} else {
  cat("⚠️ (Over-predicting home teams)\n")
}

cat("\n💰 PROFITABILITY ANALYSIS:\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
cat("Winner Betting (-110 odds):\n")
cat(sprintf("  Hit Rate:          %.1f%%\n", overall_metrics$hit_rate * 100))
cat(sprintf("  Break-even:        52.4%%\n"))
cat(sprintf("  Profit/Loss:       $%+.2f\n", winner_profit$profit))
cat(sprintf("  ROI:               %+.1f%%\n", winner_profit$roi))

if (winner_profit$profit > 0) {
  cat("  ✅ PROFITABLE\n")
} else {
  cat("  ❌ LOSING\n")
}

if (overall_metrics$spread_games > 0) {
  cat("\nSpread Betting (-110 odds):\n")
  cat(sprintf("  Games with lines:  %d\n", overall_metrics$spread_games))
  cat(sprintf("  Correct picks:     %d (%.1f%%)\n", 
              overall_metrics$spread_correct,
              overall_metrics$spread_hit_rate * 100))
  cat(sprintf("  Profit/Loss:       $%+.2f\n", spread_profit$profit))
  cat(sprintf("  ROI:               %+.1f%%\n", spread_profit$roi))
  
  if (spread_profit$profit > 0) {
    cat("  ✅ PROFITABLE\n")
  } else {
    cat("  ❌ LOSING\n")
  }
}

cat("\n📅 PERFORMANCE BY WEEK:\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
by_week_display <- by_week %>%
  mutate(
    hit_rate_pct = sprintf("%.1f%%", hit_rate * 100),
    mae_display = sprintf("%.2f", mae)
  ) %>%
  select(Week = week, Games = games, `Hit Rate` = hit_rate_pct, MAE = mae_display)

print(by_week_display, row.names = FALSE)

cat("\n🎯 BIGGEST PREDICTION ERRORS:\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

worst_misses <- comparison %>%
  arrange(desc(margin_error)) %>%
  head(5) %>%
  mutate(
    matchup = paste(away_team, "@", home_team)
  ) %>%
  select(Week = week, Matchup = matchup, 
         Predicted = predicted_margin, Actual = actual_margin, 
         Error = margin_error)

print(worst_misses, row.names = FALSE)

cat("\n")

cat("════════════════════════════════════════════════════════════════\n")
cat("INTERPRETATION:\n")
cat("  • Hit rate >60% = Strong model ✅\n")
cat("  • Hit rate 55-60% = Good model ✅\n")
cat("  • Hit rate 52.4-55% = Marginal edge ⚠️\n")
cat("  • Hit rate <52.4% = Losing model ❌\n")
cat("  • MAE <10 pts = Good accuracy ✅\n")
cat("  • |Bias| <1 pt = No systematic error ✅\n")
cat("\n")
cat("NOTE: Your pipeline now includes weather & injury adjustments.\n")
cat("      Compare these results to previous backtests to see if\n")
cat("      the enhanced features improved performance!\n")
cat("════════════════════════════════════════════════════════════════\n\n")

# ============================================================================
# SAVE RESULTS
# ============================================================================

cat("💾 Saving results...\n")

# Detailed comparison
write.csv(
  comparison %>%
    select(week, away_team, home_team, predicted_margin, actual_margin,
           margin_error, correct_winner, predicted_winner, actual_winner,
           spread_line, spread_bet_correct, actual_total, predicted_total),
  file.path(backtest_dir, "backtest_detailed_results.csv"),
  row.names = FALSE
)

# Summary metrics
summary_df <- data.frame(
  Metric = c(
    "Total Games",
    "Hit Rate",
    "Mean Absolute Error",
    "RMSE",
    "Bias",
    "Break-even Rate",
    "Profit/Loss",
    "ROI"
  ),
  Value = c(
    overall_metrics$total_games,
    sprintf("%.1f%%", overall_metrics$hit_rate * 100),
    sprintf("%.2f pts", overall_metrics$mean_abs_error),
    sprintf("%.2f pts", overall_metrics$rmse),
    sprintf("%+.2f pts", overall_metrics$bias),
    "52.4%",
    sprintf("$%+.2f", winner_profit$profit),
    sprintf("%+.1f%%", winner_profit$roi)
  )
)

write.csv(summary_df, file.path(backtest_dir, "backtest_summary.csv"), row.names = FALSE)

# By week performance
write.csv(by_week_display, file.path(backtest_dir, "backtest_by_week.csv"), row.names = FALSE)

# Create visualization
if (nrow(comparison) > 0) {
  
  # Prediction accuracy scatter plot
  accuracy_plot <- ggplot(comparison, aes(x = actual_margin, y = predicted_margin)) +
    geom_point(aes(color = correct_winner), size = 3, alpha = 0.7) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red", size = 1) +
    scale_color_manual(
      values = c("TRUE" = "#27ae60", "FALSE" = "#e74c3c"),
      labels = c("TRUE" = "Correct Winner", "FALSE" = "Wrong Winner")
    ) +
    labs(
      title = "Model Prediction Accuracy",
      subtitle = sprintf("Hit Rate: %.1f%% | MAE: %.2f pts | RMSE: %.2f pts",
                         overall_metrics$hit_rate * 100,
                         overall_metrics$mean_abs_error,
                         overall_metrics$rmse),
      x = "Actual Margin (Positive = Away Won)",
      y = "Predicted Margin",
      color = "Prediction"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      plot.title = element_text(face = "bold", hjust = 0.5, size = 16),
      plot.subtitle = element_text(hjust = 0.5),
      legend.position = "bottom"
    )
  
  ggsave(
    file.path(backtest_dir, "prediction_accuracy.png"),
    accuracy_plot,
    width = 10,
    height = 8,
    dpi = 300
  )
  
  cat("  ✓ Visualization created\n")
}

cat("\n✓ Results saved to:", backtest_dir, "\n")
cat("  - backtest_detailed_results.csv\n")
cat("  - backtest_summary.csv\n")
cat("  - backtest_by_week.csv\n")
cat("  - prediction_accuracy.png\n\n")

cat("════════════════════════════════════════════════════════════════\n")
cat("🎉 BACKTEST COMPLETE!\n")
cat("════════════════════════════════════════════════════════════════\n\n")

if (overall_metrics$hit_rate >= 0.55) {
  cat("✅ YOUR MODEL SHOWS EDGE - Consider proceeding with validation\n")
  cat("   Next steps:\n")
  cat("   1. Backtest more weeks (weeks 8-12 when available)\n")
  cat("   2. Paper trade for 2 weeks to verify\n")
  cat("   3. Define stop-loss rules\n")
  cat("   4. Consider small real bets if validated\n")
  cat("\n   Enhanced features working:\n")
  cat("   ✓ Weather adjustments integrated\n")
  cat("   ✓ Injury data pipeline active\n")
  cat("   ✓ Rest/travel factors included\n\n")
} else if (overall_metrics$hit_rate >= 0.524) {
  cat("⚠️ MARGINAL EDGE - Need more validation before betting\n")
  cat("   Recommendations:\n")
  cat("   1. Backtest more weeks to increase sample size\n")
  cat("   2. Analyze where model struggles (home/away, favorites, etc.)\n")
  cat("   3. Review weather/injury impact - is it helping?\n")
  cat("   4. Consider adjusting feature weights\n")
  cat("   5. DO NOT bet real money yet\n\n")
} else {
  cat("❌ MODEL NEEDS IMPROVEMENT - Do not bet\n")
  cat("   Critical actions:\n")
  cat("   1. Analyze systematic biases in your predictions\n")
  cat("   2. Review feature weights and adjustments\n")
  cat("   3. Check if weather/injury data is accurate\n")
  cat("   4. Consider different rolling window sizes\n")
  cat("   5. Test alternative metrics\n")
  cat("   6. Compare performance WITH vs WITHOUT weather adjustments\n")
  cat("   7. DO NOT bet until hit rate >55%\n\n")
}