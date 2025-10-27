# ============================================================================
# NFL BETTING MODEL - ENHANCED MASTER PIPELINE (DATABASE VERSION)
# ============================================================================
# Following methodology from: https://themockup.blog/posts/2019-04-28-nflfastr-dbplyr-rsqlite/

# Clear environment
rm(list = ls())
gc()

# ============================================================================
# CONFIGURATION
# ============================================================================

CURRENT_SEASON <- 2025
CURRENT_WEEK <- 7  # Up to end of week 7
PREDICTION_WEEK <- 8  # Week we're predicting
START_SEASON <- 2021  # Start from 2021 season
BASE_DIR <- "C:/Users/Patsc/Documents/nfl_model_v2"

cat("\n")
cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║        NFL BETTING MODEL - ENHANCED PIPELINE                   ║\n")
cat("║        Seasons:", START_SEASON, "-", CURRENT_SEASON, "| Predicting Week:", PREDICTION_WEEK, "              ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n")
cat("\n")

# ============================================================================
# LOAD PACKAGES
# ============================================================================

cat("Loading required packages...\n")

required_packages <- c(
  "nflverse", "nflreadr", "nflfastR", "nflplotR", "nflseedR", "nfl4th",
  "dplyr", "tidyr", "stringr", "lubridate", "janitor", "readr",
  "writexl", "openxlsx", "ggplot2", "gt", "gtExtras",
  "gridExtra", "zoo", "ggrepel", "geosphere", "DBI", "RSQLite", "glue", "dbplyr"
)

# Safe utility functions
safe_mean <- function(x, na.rm = TRUE) {
  if (length(x) == 0 || all(is.na(x))) return(NA_real_)
  mean(x, na.rm = na.rm)
}

safe_sum <- function(x, na.rm = TRUE) {
  if (length(x) == 0) return(0)
  sum(x, na.rm = na.rm)
}

safe_rollmean <- function(x, k = 3, fill = NA, align = "right") {
  if (length(x) < 2) return(rep(NA_real_, length(x)))
  actual_k <- min(k, length(x))
  if (actual_k < 2) return(rep(NA_real_, length(x)))
  tryCatch({
    zoo::rollmean(x, k = actual_k, fill = fill, align = align)
  }, error = function(e) {
    rep(NA_real_, length(x))
  })
}

safe_rate <- function(numerator, denominator) {
  if (is.na(denominator) || denominator == 0) return(NA_real_)
  numerator / denominator
}

for (pkg in required_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat("  Installing", pkg, "...\n")
    install.packages(pkg)
  }
  suppressPackageStartupMessages(library(pkg, character.only = TRUE))
}

cat("✓ All packages loaded successfully!\n\n")

# ============================================================================
# LOAD TEAM VISUAL DATA
# ============================================================================

cat("Loading team visual data...\n")
teams_colors_logos <- nflreadr::load_teams() %>%
  select(team_abbr, team_name, team_color, team_color2, team_logo_espn, 
         team_wordmark, team_conf, team_division)
cat("✓ Team visual data loaded for", nrow(teams_colors_logos), "teams\n\n")

# ============================================================================
# PHASE 1: DATABASE SETUP & DATA PREPARATION
# ============================================================================

cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║  PHASE 1: DATABASE SETUP & DATA PREPARATION                   ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n\n")
start_time_phase1 <- Sys.time()

# Create output directory
output_dir <- file.path(BASE_DIR, paste0('week', PREDICTION_WEEK))
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
  cat("Created output directory:", output_dir, "\n")
}


# ============================================================================
# STEP 1: BUILD DATABASE WITH 2021-2025 DATA ONLY
# ============================================================================

cat("[1/12] Building nflfastR database (2021:2025)...\n")

db_path <- file.path(BASE_DIR, "pbp_db_2021_2025.sqlite")

# Function to check if database is valid
db_is_valid <- function(path) {
  if (!file.exists(path)) return(FALSE)
  
  tryCatch({
    conn <- DBI::dbConnect(RSQLite::SQLite(), path)
    has_table <- DBI::dbExistsTable(conn, "nflfastR_pbp")
    if (has_table) {
      # Check if table has data
      count <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM nflfastR_pbp")
      has_data <- count$n > 0
    } else {
      has_data <- FALSE
    }
    DBI::dbDisconnect(conn)
    return(has_table && has_data)
  }, error = function(e) {
    return(FALSE)
  })
}

# Check if we need to build the database
if (db_is_valid(db_path)) {
  cat("      ✓ Valid database found at:", db_path, "\n")
} else {
  if (file.exists(db_path)) {
    cat("      Removing invalid/incomplete database...\n")
    file.remove(db_path)
  }
  
  cat("      Creating new database...\n")
  cat("      Loading seasons 2021-2025 (this will take 2-3 minutes)...\n")
  
  # Load each season's data
  seasons_to_load <- START_SEASON:CURRENT_SEASON
  
  pbp_data <- purrr::map_dfr(seasons_to_load, function(season) {
    cat("        • Season", season, "... ")
    data <- nflreadr::load_pbp(season)
    cat(nrow(data), "plays loaded\n")
    return(data)
  })
  
  cat("      Total plays loaded:", nrow(pbp_data), "\n")
  
  # Create database connection
  cat("      Creating SQLite database...\n")
  connection <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  
  # Write data to database
  cat("      Writing to database table...\n")
  DBI::dbWriteTable(connection, "nflfastR_pbp", pbp_data, overwrite = TRUE)
  
  # Create indexes for faster queries
  cat("      Creating indexes...\n")
  DBI::dbExecute(connection, "CREATE INDEX idx_season ON nflfastR_pbp(season)")
  DBI::dbExecute(connection, "CREATE INDEX idx_week ON nflfastR_pbp(week)")
  DBI::dbExecute(connection, "CREATE INDEX idx_season_type ON nflfastR_pbp(season_type)")
  DBI::dbExecute(connection, "CREATE INDEX idx_posteam ON nflfastR_pbp(posteam)")
  DBI::dbExecute(connection, "CREATE INDEX idx_defteam ON nflfastR_pbp(defteam)")
  
  # Disconnect
  DBI::dbDisconnect(connection)
  
  # Clean up
  rm(pbp_data)
  gc()
  
  cat("      ✓ Database created successfully!\n")
}

# ============================================================================
# STEP 2: CONNECT TO DATABASE AND CREATE TABLE REFERENCE
# ============================================================================

cat("[2/12] Connecting to database...\n")

# Connect to database (following blog post method)
connection <- DBI::dbConnect(RSQLite::SQLite(), db_path)

# Create table reference using dplyr (this is the key step from the blog)
pbp_db <- tbl(connection, "nflfastR_pbp")

cat("      ✓ Connected to database\n")

# Verify what's in the database
db_summary <- pbp_db %>%
  group_by(season) %>%
  summarise(games = n_distinct(game_id), plays = n()) %>%
  collect()

cat("      Database contents:\n")
print(db_summary)

# ==============================================================
# NFL Weather & Injuries – Clean and Save Separately
# ==============================================================

cat("[2.5/12] Cleaning weather and injuries data...\n")

# Packages already loaded at top: stringr, lubridate, janitor, readr

# -----------------------------
# 1️⃣ Clean Weather Data
# -----------------------------
weather_file <- file.path(BASE_DIR, "nflweather_games.csv")

if (file.exists(weather_file)) {
  cat("      Loading weather data...\n")
  weather <- read_csv(weather_file, show_col_types = FALSE) |> 
    clean_names()
  
  cat("      Weather Columns: ", paste(names(weather), collapse = ", "), "\n")
  
  # Parse date and numeric columns
  date_col <- if("datetime_text" %in% names(weather)) "datetime_text" else names(weather)[1]
  weather <- weather |> 
    mutate(
      game_date = mdy(str_extract(.data[[date_col]], "\\d{1,2}/\\d{1,2}/\\d{2,4}")),
      temp_f = as.numeric(str_extract(temp_f, "-?\\d+")),
      rain_pct = as.numeric(str_extract(rain_pct, "\\d+"))
    )
  
  # Save cleaned weather file
  weather_clean_file <- file.path(BASE_DIR, "nflweather_games_clean.csv")
  write_csv(weather, weather_clean_file)
  cat("      ✓ Cleaned weather data saved to nflweather_games_clean.csv\n")
  cat("      Preview (first 3 rows):\n")
  print(head(weather, 3))
} else {
  cat("      ⚠ Weather file not found:", weather_file, "\n")
  cat("      Skipping weather data cleaning...\n")
}

# -----------------------------
# 2️⃣ Clean Injuries Data
# -----------------------------
injuries_file <- file.path(BASE_DIR, "nfl_injuries.csv")

if (file.exists(injuries_file)) {
  cat("      Loading injuries data...\n")
  injuries <- read_csv(injuries_file, show_col_types = FALSE) |> 
    clean_names()
  
  cat("      Injuries Columns: ", paste(names(injuries), collapse = ", "), "\n")
  
  # Trim whitespace in all columns
  injuries <- injuries |> mutate(across(everything(), str_squish))
  
  # Save cleaned injuries file
  injuries_clean_file <- file.path(BASE_DIR, "nfl_injuries_clean.csv")
  write_csv(injuries, injuries_clean_file)
  cat("      ✓ Cleaned injuries data saved to nfl_injuries_clean.csv\n")
  cat("      Preview (first 3 rows):\n")
  print(head(injuries, 3))
} else {
  cat("      ⚠ Injuries file not found:", injuries_file, "\n")
  cat("      Skipping injuries data cleaning...\n")
}

cat("      ✓ Weather and injuries cleaning complete\n\n")

# ============================================================================
# STEP 3: FILTER DATA USING DPLYR (Query stays in database)
# ============================================================================

cat("[3/12] Preparing filtered query...\n")

# Create filtered query - THIS STAYS IN THE DATABASE (doesn't load into R yet)
pbp_filtered <- pbp_db %>%
  filter(
    season >= !!START_SEASON,
    season <= !!CURRENT_SEASON,
    season_type == "REG",
    !is.na(play_type),
    play_type %in% c("pass", "run", "field_goal", "punt", "qb_kneel", "qb_spike")
  ) %>%
  # For current season, only up to CURRENT_WEEK
  filter(
    (season < !!CURRENT_SEASON) | (season == !!CURRENT_SEASON & week <= !!CURRENT_WEEK)
  ) %>%
  # Remove garbage time
  filter(
    !(qtr %in% c(2, 4) & game_seconds_remaining <= 120 & abs(score_differential_post) > 14)
  ) %>%
  # Handle penalties
  filter(
    is.na(penalty_team) | penalty_type %in% c("Defensive Offside", "Neutral Zone Infraction", "Defensive Delay of Game")
  )

cat("      ✓ Filter query prepared (data still in database)\n")

# ============================================================================
# STEP 4: ADD CALCULATED COLUMNS (Still in database)
# ============================================================================

cat("[4/12] Adding calculated columns to query...\n")

# Add calculated fields - query still in database
pbp_prep <- pbp_filtered %>%
  mutate(
    offense_team = posteam,
    defense_team = defteam,
    
    # Situational markers (using SQL-compatible syntax)
    red_zone = ifelse(yardline_100 <= 20 & yardline_100 > 0, 1L, 0L),
    explosive_pass = ifelse(play_type == "pass" & yards_gained >= 20, 1L, 0L),
    explosive_run = ifelse(play_type == "run" & yards_gained >= 10, 1L, 0L),
    explosive_play = ifelse((play_type == "pass" & yards_gained >= 20) | 
                              (play_type == "run" & yards_gained >= 10), 1L, 0L),
    stuffed = ifelse(yards_gained <= 0, 1L, 0L),
    turnover = ifelse(interception == 1 | fumble_lost == 1, 1L, 0L),
    is_third_down = ifelse(down == 3, 1L, 0L),
    is_fourth_down = ifelse(down == 4, 1L, 0L),
    two_minute_drill = ifelse(half_seconds_remaining <= 120, 1L, 0L)
  )

cat("      ✓ Calculated columns added to query\n")

# You can see the SQL that will be executed (optional - for debugging)
# cat("\nGenerated SQL Query:\n")
# show_query(pbp_prep)

# ============================================================================
# STEP 5: COLLECT DATA INTO R (Now we load from database)
# ============================================================================

cat("[5/12] Executing query and loading data into R...\n")
cat("      This may take 30-60 seconds...\n")

# NOW we execute the query and bring data into R memory
pbp_analysis <- pbp_prep %>%
  collect() %>%
  # Do additional R-specific transformations after collecting
  mutate(
    game_id = as.character(game_id),
    season = as.integer(season),
    week = as.integer(week),
    game_date = as.Date(game_date),
    
    # Clean up play type
    play_type_clean = case_when(
      play_type == "pass" ~ "pass",
      play_type == "run" & qb_scramble == 1 ~ "qb_scramble",
      play_type == "run" ~ "run",
      play_type == "field_goal" ~ "field_goal",
      play_type == "punt" ~ "punt",
      TRUE ~ "other"
    )
  )

cat("      ✓ Data loaded:", nrow(pbp_analysis), "plays\n")
cat("      ✓ Seasons:", paste(unique(sort(pbp_analysis$season)), collapse = ", "), "\n")
cat("      ✓ Date range:", min(pbp_analysis$game_date), "to", max(pbp_analysis$game_date), "\n")

# Disconnect from database (we have our data now)
DBI::dbDisconnect(connection)
cat("      ✓ Database connection closed\n")

# Clean up
rm(pbp_db, pbp_filtered, pbp_prep)
gc()

cat("      ✓ Memory cleaned\n")

# ============================================================================
# PHASE 2: ADVANCED METRICS CALCULATION
# ============================================================================

cat("\n╔════════════════════════════════════════════════════════════════╗\n")
cat("║  PHASE 2: CALCULATING ADVANCED METRICS                        ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n\n")
start_time_phase2 <- Sys.time()

# ============================================================================
# STEP 6: PACE & PLAY-CALLING METRICS
# ============================================================================

cat("[6/12] Calculating pace & play-calling metrics...\n")

pace_metrics <- pbp_analysis %>%
  filter(!is.na(offense_team), !is.na(posteam_score), !is.na(defteam_score)) %>%
  # Convert to numeric if needed
  mutate(
    drive_time_of_possession = as.numeric(drive_time_of_possession),
    drive_play_count = as.numeric(drive_play_count)
  ) %>%
  group_by(season, offense_team, week, game_id) %>%
  summarise(
    total_plays = n(),
    pass_plays = sum(play_type == "pass", na.rm = TRUE),
    run_plays = sum(play_type == "run", na.rm = TRUE),
    pass_rate = safe_rate(pass_plays, total_plays),
    
    # Pace metrics
    avg_seconds_per_snap = safe_mean(drive_time_of_possession / drive_play_count),
    plays_per_minute = safe_rate(60, safe_mean(drive_time_of_possession / drive_play_count)),
    
    # Situational play calling
    pass_rate_neutral = safe_mean(play_type == "pass" & abs(score_differential) <= 7),
    pass_rate_winning = safe_mean(play_type == "pass" & score_differential > 7),
    pass_rate_losing = safe_mean(play_type == "pass" & score_differential < -7),
    pass_rate_early_down = safe_mean(play_type == "pass" & down %in% c(1, 2)),
    pass_rate_third_down = safe_mean(play_type == "pass" & down == 3),
    
    .groups = 'drop'
  ) %>%
  group_by(season, offense_team, week) %>%
  summarise(across(where(is.numeric), safe_mean), .groups = 'drop')

cat("      ✓ Pace metrics calculated\n")

# ============================================================================
# STEP 7: CORE OFFENSIVE STATISTICS
# ============================================================================

cat("[7/12] Calculating offensive statistics...\n")

offense_base <- pbp_analysis %>%
  filter(!is.na(offense_team)) %>%
  group_by(season, offense_team, week) %>%
  summarise(
    games = n_distinct(game_id),
    
    # Play counts
    total_plays = n(),
    pass_plays = sum(play_type == "pass", na.rm = TRUE),
    run_plays = sum(play_type == "run", na.rm = TRUE),
    
    # Core efficiency
    epa_per_play = safe_mean(epa),
    epa_per_pass = safe_mean(epa[play_type == "pass"]),
    epa_per_rush = safe_mean(epa[play_type == "run"]),
    success_rate = safe_mean(success),
    
    # Explosiveness
    explosive_play_rate = safe_mean(explosive_play),
    explosive_pass_rate = safe_rate(sum(explosive_pass, na.rm = TRUE), pass_plays),
    explosive_run_rate = safe_rate(sum(explosive_run, na.rm = TRUE), run_plays),
    
    # Yards
    yards_per_play = safe_mean(yards_gained),
    pass_yards_per_att = safe_mean(yards_gained[play_type == "pass"]),
    rush_yards_per_att = safe_mean(yards_gained[play_type == "run"]),
    
    # Negative plays
    stuff_rate = safe_mean(stuffed),
    sack_rate = safe_rate(sum(sack, na.rm = TRUE), pass_plays),
    sacks_taken = sum(sack, na.rm = TRUE),
    
    # Turnovers
    turnover_rate = safe_mean(turnover),
    interceptions = sum(interception, na.rm = TRUE),
    fumbles_lost = sum(fumble_lost, na.rm = TRUE),
    
    # Scoring
    total_touchdowns = sum(touchdown, na.rm = TRUE),
    pass_touchdowns = sum(pass_touchdown, na.rm = TRUE),
    rush_touchdowns = sum(rush_touchdown, na.rm = TRUE),
    
    # Red Zone
    rz_attempts = sum(red_zone, na.rm = TRUE),
    rz_success_rate = safe_mean(success[red_zone == 1]),
    rz_td_rate = safe_rate(sum(touchdown[red_zone == 1], na.rm = TRUE), rz_attempts),
    
    # Critical downs
    third_down_attempts = sum(is_third_down, na.rm = TRUE),
    third_down_conv_rate = safe_mean(success[is_third_down == 1]),
    fourth_down_attempts = sum(is_fourth_down, na.rm = TRUE),
    fourth_down_conv_rate = safe_mean(success[is_fourth_down == 1]),
    
    # Two-minute offense
    two_min_plays = sum(two_minute_drill, na.rm = TRUE),
    two_min_epa = safe_mean(epa[two_minute_drill == 1]),
    two_min_success = safe_mean(success[two_minute_drill == 1]),
    
    .groups = 'drop'
  )

# Merge with pace metrics
offense_stats <- offense_base %>%
  left_join(pace_metrics, by = c("season", "offense_team", "week"))

cat("      ✓ Offensive stats calculated\n")

rm(offense_base, pace_metrics)
gc()

# ============================================================================
# STEP 8: CORE DEFENSIVE STATISTICS
# ============================================================================

cat("[8/12] Calculating defensive statistics...\n")

defense_stats <- pbp_analysis %>%
  filter(!is.na(defense_team)) %>%
  group_by(season, defense_team, week) %>%
  summarise(
    games = n_distinct(game_id),
    
    # Play counts
    total_plays = n(),
    pass_plays_faced = sum(play_type == "pass", na.rm = TRUE),
    run_plays_faced = sum(play_type == "run", na.rm = TRUE),
    
    # Core efficiency (defensive perspective)
    def_epa_per_play = safe_mean(epa),
    def_epa_per_pass = safe_mean(epa[play_type == "pass"]),
    def_epa_per_rush = safe_mean(epa[play_type == "run"]),
    def_success_rate = safe_mean(success),
    
    # Explosiveness allowed
    def_explosive_rate = safe_mean(explosive_play),
    def_explosive_pass_rate = safe_rate(sum(explosive_pass, na.rm = TRUE), pass_plays_faced),
    def_explosive_run_rate = safe_rate(sum(explosive_run, na.rm = TRUE), run_plays_faced),
    
    # Yards allowed
    def_yards_per_play = safe_mean(yards_gained),
    def_pass_yards_per_att = safe_mean(yards_gained[play_type == "pass"]),
    def_rush_yards_per_att = safe_mean(yards_gained[play_type == "run"]),
    
    # Positive defensive plays
    def_stuff_rate = safe_mean(stuffed),
    sacks_generated = sum(sack, na.rm = TRUE),
    def_sack_rate = safe_rate(sum(sack, na.rm = TRUE), pass_plays_faced),
    
    # Turnovers forced
    total_turnovers_forced = sum(turnover, na.rm = TRUE),
    interceptions_forced = sum(interception, na.rm = TRUE),
    fumbles_forced = sum(fumble_lost, na.rm = TRUE),
    turnover_rate_forced = safe_mean(turnover),
    
    # Scoring allowed
    touchdowns_allowed = sum(touchdown, na.rm = TRUE),
    
    # Red zone defense
    def_rz_attempts = sum(red_zone, na.rm = TRUE),
    def_rz_success_rate = safe_mean(success[red_zone == 1]),
    def_rz_td_rate = safe_rate(sum(touchdown[red_zone == 1], na.rm = TRUE), def_rz_attempts),
    
    # Critical downs
    def_third_down_faced = sum(is_third_down, na.rm = TRUE),
    def_third_down_conv_rate = safe_mean(success[is_third_down == 1]),
    
    .groups = 'drop'
  )

cat("      ✓ Defensive stats calculated\n")

# ============================================================================
  # STEP 9: ROLLING AVERAGES & TRENDS
# ============================================================================

cat("[9/12] Calculating rolling averages and trends...\n")

# Offensive rolling averages (3-game, 5-game windows)
offense_rolling <- offense_stats %>%
  group_by(season, offense_team) %>%
  arrange(week) %>%
  mutate(
    # 3-game rolling
    roll3_epa = safe_rollmean(epa_per_play, k = 3),
    roll3_success = safe_rollmean(success_rate, k = 3),
    roll3_explosive = safe_rollmean(explosive_play_rate, k = 3),
    roll3_ypp = safe_rollmean(yards_per_play, k = 3),
    roll3_turnover = safe_rollmean(turnover_rate, k = 3),
    roll3_third_down = safe_rollmean(third_down_conv_rate, k = 3),
    roll3_rz_td = safe_rollmean(rz_td_rate, k = 3),
    roll3_pass_rate = safe_rollmean(pass_rate, k = 3),
    
    # 5-game rolling
    roll5_epa = safe_rollmean(epa_per_play, k = 5),
    roll5_success = safe_rollmean(success_rate, k = 5),
    
    # Trends (comparing recent 3 games to previous 3)
    trend_epa = roll3_epa - lag(roll3_epa, 3),
    trend_success = roll3_success - lag(roll3_success, 3)
  ) %>%
  ungroup()

# Defensive rolling averages
defense_rolling <- defense_stats %>%
  group_by(season, defense_team) %>%
  arrange(week) %>%
  mutate(
    # 3-game rolling
    roll3_def_epa = safe_rollmean(def_epa_per_play, k = 3),
    roll3_def_success = safe_rollmean(def_success_rate, k = 3),
    roll3_def_explosive = safe_rollmean(def_explosive_rate, k = 3),
    roll3_def_ypp = safe_rollmean(def_yards_per_play, k = 3),
    roll3_turnovers_forced = safe_rollmean(total_turnovers_forced, k = 3),
    roll3_def_third = safe_rollmean(def_third_down_conv_rate, k = 3),
    roll3_sacks = safe_rollmean(sacks_generated, k = 3),
    
    # 5-game rolling
    roll5_def_epa = safe_rollmean(def_epa_per_play, k = 5),
    roll5_def_success = safe_rollmean(def_success_rate, k = 5),
    
    # Trends
    trend_def_epa = roll3_def_epa - lag(roll3_def_epa, 3),
    trend_def_success = roll3_def_success - lag(roll3_def_success, 3)
  ) %>%
  ungroup()

cat("      ✓ Rolling averages and trends calculated\n")

# Continue with rest of the model (Steps 10-12 and Phases 3-4 remain the same as before)

# ============================================================================
# STEP 10: LOAD SCHEDULE & EXTERNAL DATA
# ============================================================================

cat("[10/12] Loading schedule and external data...\n")

# Load schedule for prediction week
schedules <- nflreadr::load_schedules(CURRENT_SEASON) %>%
  filter(week == PREDICTION_WEEK) %>%
  select(game_id, season, week, gameday, gametime, weekday,
         away_team, home_team, away_score, home_score, 
         location, roof, surface, temp, wind,
         spread_line, total_line, away_rest, home_rest, 
         away_moneyline, home_moneyline, div_game)

cat("      ✓ Schedule loaded:", nrow(schedules), "games for week", PREDICTION_WEEK, "\n")

# Load injuries
cat("      Loading injury data...\n")
injuries <- tryCatch({
  nflreadr::load_injuries(CURRENT_SEASON) %>%
    filter(week == PREDICTION_WEEK) %>%
    group_by(team) %>%
    summarise(
      total_injuries = n(),
      players_out = sum(report_status == "Out", na.rm = TRUE),
      players_doubtful = sum(report_status == "Doubtful", na.rm = TRUE),
      players_questionable = sum(report_status == "Questionable", na.rm = TRUE),
      key_injuries_out = sum(report_status %in% c("Out", "Doubtful") & 
                               position %in% c("QB", "RB", "WR", "TE", "LT", "LG", "C", "RG", "RT", 
                                               "DE", "DT", "LB", "CB", "S"), na.rm = TRUE),
      qb_injured = sum(position == "QB" & report_status %in% c("Out", "Doubtful"), na.rm = TRUE),
      .groups = 'drop'
    )
}, error = function(e) {
  cat("      Note: Injury data not available\n")
  data.frame(team = character(), total_injuries = numeric(), 
             players_out = numeric(), players_doubtful = numeric(),
             players_questionable = numeric(), key_injuries_out = numeric(),
             qb_injured = numeric())
})

if (nrow(injuries) > 0) {
  cat("      ✓ Injury data loaded\n")
}

# ============================================================================
# STEP 11: TRAVEL & SITUATIONAL FACTORS
# ============================================================================

cat("[11/12] Calculating travel and situational factors...\n")

# Stadium coordinates
stadium_coords <- tribble(
  ~team, ~lat, ~lon,
  "ARI", 33.5276, -112.2626, "ATL", 33.7554, -84.4009,
  "BAL", 39.2780, -76.6227, "BUF", 42.7738, -78.7870,
  "CAR", 35.2258, -80.8529, "CHI", 41.8623, -87.6167,
  "CIN", 39.0954, -84.5160, "CLE", 41.5061, -81.6995,
  "DAL", 32.7473, -97.0945, "DEN", 39.7439, -105.0201,
  "DET", 42.3400, -83.0456, "GB", 44.5013, -88.0622,
  "HOU", 29.6847, -95.4107, "IND", 39.7601, -86.1639,
  "JAX", 30.3240, -81.6373, "KC", 39.0489, -94.4839,
  "LV", 36.0909, -115.1833, "LAC", 33.9534, -118.3390,
  "LAR", 33.9534, -118.3390, "MIA", 25.9580, -80.2389,
  "MIN", 44.9737, -93.2577, "NE", 42.0909, -71.2643,
  "NO", 29.9511, -90.0812, "NYG", 40.8128, -74.0742,
  "NYJ", 40.8128, -74.0742, "PHI", 39.9008, -75.1675,
  "PIT", 40.4468, -80.0158, "SF", 37.4032, -121.9698,
  "SEA", 47.5952, -122.3316, "TB", 27.9759, -82.5033,
  "TEN", 36.1665, -86.7713, "WAS", 38.9076, -76.8645
)

# Calculate travel distances
schedules <- schedules %>%
  left_join(stadium_coords %>% rename(away_team = team, away_lat = lat, away_lon = lon), 
            by = "away_team") %>%
  left_join(stadium_coords %>% rename(home_team = team, home_lat = lat, home_lon = lon), 
            by = "home_team") %>%
  rowwise() %>%
  mutate(
    travel_distance_miles = if_else(
      !is.na(away_lat) & !is.na(home_lat),
      geosphere::distHaversine(c(away_lon, away_lat), c(home_lon, home_lat)) / 1609.34,
      NA_real_
    ),
    long_travel = if_else(travel_distance_miles > 1500, 1, 0, missing = 0),
    cross_country = if_else(travel_distance_miles > 2500, 1, 0, missing = 0)
  ) %>%
  ungroup() %>%
  select(-away_lat, -away_lon, -home_lat, -home_lon)

cat("      ✓ Travel and situational factors calculated\n")

# ============================================================================
# STEP 12: CREATE MATCHUP DATASET
# ============================================================================

cat("[12/12] Creating final matchup dataset...\n")

# Get most recent week's stats for CURRENT_SEASON only
latest_offense <- offense_rolling %>%
  filter(season == CURRENT_SEASON, week == CURRENT_WEEK)

latest_defense <- defense_rolling %>%
  filter(season == CURRENT_SEASON, week == CURRENT_WEEK)

# Build matchups
matchups <- schedules %>%
  # Add away team offense
  left_join(
    latest_offense %>% 
      select(-season, -week, -games) %>%
      rename_with(~paste0("away_off_", .), -offense_team),
    by = c("away_team" = "offense_team")
  ) %>%
  # Add away team defense
  left_join(
    latest_defense %>% 
      select(-season, -week, -games) %>%
      rename_with(~paste0("away_def_", .), -defense_team),
    by = c("away_team" = "defense_team")
  ) %>%
  # Add home team offense
  left_join(
    latest_offense %>% 
      select(-season, -week, -games) %>%
      rename_with(~paste0("home_off_", .), -offense_team),
    by = c("home_team" = "offense_team")
  ) %>%
  # Add home team defense
  left_join(
    latest_defense %>% 
      select(-season, -week, -games) %>%
      rename_with(~paste0("home_def_", .), -defense_team),
    by = c("home_team" = "defense_team")
  )

# Add injuries if available
if (nrow(injuries) > 0) {
  matchups <- matchups %>%
    left_join(injuries %>% rename_with(~paste0("away_", .), -team), 
              by = c("away_team" = "team")) %>%
    left_join(injuries %>% rename_with(~paste0("home_", .), -team), 
              by = c("home_team" = "team"))
}

cat("      ✓ Matchup dataset created:", nrow(matchups), "games\n")

end_time_phase2 <- Sys.time()
cat("\n✓ PHASE 2 COMPLETE\n")
cat(" Time elapsed:", round(difftime(end_time_phase2, start_time_phase2, units = "secs"), 1), "seconds\n\n")

source("sportsdata_injuries.R")
injury_data <- fetch_and_process_injuries(season = CURRENT_SEASON, week = PREDICTION_WEEK)
matchups <- add_injuries_to_matchups(matchups, injury_data$summary)

# ============================================================================
# PHASE 3: MATCHUP ANALYSIS & PREDICTIONS
# ============================================================================

cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║  PHASE 3: MATCHUP ANALYSIS & PREDICTIONS                      ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n\n")
start_time_phase3 <- Sys.time()

cat("Calculating matchup advantages...\n")

# Calculate comprehensive matchup metrics
predictions <- matchups %>%
  mutate(
    # ========== OFFENSIVE MATCHUPS ==========
    # Away offense vs Home defense
    away_epa_matchup = away_off_roll3_epa - home_def_roll3_def_epa,
    away_success_matchup = away_off_roll3_success - home_def_roll3_def_success,
    away_explosive_matchup = away_off_roll3_explosive - home_def_roll3_def_explosive,
    away_ypp_matchup = away_off_roll3_ypp - home_def_roll3_def_ypp,
    
    # Home offense vs Away defense
    home_epa_matchup = home_off_roll3_epa - away_def_roll3_def_epa,
    home_success_matchup = home_off_roll3_success - away_def_roll3_def_success,
    home_explosive_matchup = home_off_roll3_explosive - away_def_roll3_def_explosive,
    home_ypp_matchup = home_off_roll3_ypp - away_def_roll3_def_ypp,
    
    # ========== COMPOSITE SCORES ==========
    # Offensive power ratings (0-100 scale)
    away_offense_rating = (away_off_roll3_epa * 20) + (away_off_roll3_success * 50) + 
      (away_off_roll3_explosive * 30),
    home_offense_rating = (home_off_roll3_epa * 20) + (home_off_roll3_success * 50) + 
      (home_off_roll3_explosive * 30),
    
    # Defensive power ratings (0-100 scale, inverted)
    away_defense_rating = -(away_def_roll3_def_epa * 20) - (away_def_roll3_def_success * 50) - 
      (away_def_roll3_def_explosive * 30) + (away_def_roll3_turnovers_forced * 10),
    home_defense_rating = -(home_def_roll3_def_epa * 20) - (home_def_roll3_def_success * 50) - 
      (home_def_roll3_def_explosive * 30) + (home_def_roll3_turnovers_forced * 10),
    
    # Total team ratings
    away_total_rating = away_offense_rating + away_defense_rating,
    home_total_rating = home_offense_rating + home_defense_rating,
    
    # ========== SITUATIONAL ADJUSTMENTS ==========
    # Home field advantage (2.5 points baseline)
    home_field_adj = case_when(
      location == "Home" ~ 2.5,
      location == "Neutral" ~ 0,
      TRUE ~ 2.5
    ),
    
    # Rest advantage (0.5 points per day)
    rest_differential = home_rest - away_rest,
    rest_adj = rest_differential * 0.5,
    
    # Travel penalty
    travel_adj = case_when(
      is.na(travel_distance_miles) ~ 0,
      cross_country == 1 ~ -2.0,
      long_travel == 1 ~ -1.0,
      travel_distance_miles > 750 ~ -0.5,
      TRUE ~ 0
    ),
    
    # Weather adjustment (cold weather affects passing games)
    weather_adj = case_when(
      is.na(temp) ~ 0,
      temp < 20 ~ -1.5,
      temp < 32 ~ -0.75,
      wind > 20 ~ -1.0,
      wind > 15 ~ -0.5,
      TRUE ~ 0
    ),
    
    # Division game adjustment (typically closer)
    division_adj = if_else(div_game == 1, -1.0, 0),
    
    # Injury adjustment (set to 0 for now - we'll integrate SportsData.io separately)
    injury_adj = case_when(
      away_qb_injured == 1 ~ -4.0,
      home_qb_injured == 1 ~ 4.0,
      TRUE ~ (home_key_injuries_out - away_key_injuries_out) * 0.75
    ),
    
    # Momentum/trend adjustment
    momentum_adj = if_else(
      !is.na(away_off_trend_epa) & !is.na(home_off_trend_epa),
      (away_off_trend_epa - home_off_trend_epa) * 5,
      0
    ),
    
    # ========== FINAL PREDICTION ==========
    # Base point differential (from team ratings)
    base_prediction = (away_total_rating - home_total_rating) * 0.15,
    
    # Apply all adjustments
    total_adjustments = home_field_adj + rest_adj + travel_adj + weather_adj + 
      division_adj + injury_adj + momentum_adj,
    
    # Final predicted margin (negative = home favored, positive = away favored)
    predicted_margin = base_prediction - total_adjustments,
    
    # Predicted winner
    predicted_winner = if_else(predicted_margin > 0, away_team, home_team),
    predicted_margin_abs = abs(predicted_margin),
    
    # ========== BETTING ANALYSIS ==========
    # Line comparison (if available)
    line_value = if_else(!is.na(spread_line), 
                         predicted_margin - spread_line, 
                         NA_real_),
    line_value_abs = abs(line_value),
    
    # Confidence tiers
    confidence_tier = case_when(
      predicted_margin_abs > 14 ~ "Very High",
      predicted_margin_abs > 10 ~ "High",
      predicted_margin_abs > 6 ~ "Medium",
      predicted_margin_abs > 3 ~ "Low",
      TRUE ~ "Very Low"
    ),
    
    # Betting recommendation
    betting_edge = case_when(
      is.na(line_value) ~ "No line available",
      line_value_abs < 2 ~ "No bet (edge too small)",
      line_value > 3.5 ~ paste0("STRONG BET: ", away_team, " (", round(line_value, 1), " point edge)"),
      line_value > 2.5 ~ paste0("BET: ", away_team, " (", round(line_value, 1), " point edge)"),
      line_value < -3.5 ~ paste0("STRONG BET: ", home_team, " (", round(abs(line_value), 1), " point edge)"),
      line_value < -2.5 ~ paste0("BET: ", home_team, " (", round(abs(line_value), 1), " point edge)"),
      TRUE ~ "Small lean only"
    ),
    
    # Over/Under analysis (if total available)
    predicted_total = 44 + (away_off_roll3_ypp * 2) + (home_off_roll3_ypp * 2) - 
      (away_def_roll3_def_ypp * 1.5) - (home_def_roll3_def_ypp * 1.5),
    
    total_value = if_else(!is.na(total_line),
                          predicted_total - total_line,
                          NA_real_),
    
    total_recommendation = case_when(
      is.na(total_value) ~ "No total available",
      abs(total_value) < 3 ~ "No bet on total",
      total_value > 4 ~ paste0("BET OVER ", total_line, " (", round(total_value, 1), " point edge)"),
      total_value < -4 ~ paste0("BET UNDER ", total_line, " (", round(abs(total_value), 1), " point edge)"),
      TRUE ~ "Small lean on total"
    )
  ) %>%
  # Sort by betting edge (best bets first)
  arrange(desc(line_value_abs))

cat("✓ Predictions and betting analysis complete\n\n")

end_time_phase3 <- Sys.time()
cat("✓ PHASE 3 COMPLETE\n")
cat("  Time elapsed:", round(difftime(end_time_phase3, start_time_phase3, units = "secs"), 1), "seconds\n\n")

# ============================================================================
# PHASE 4: OUTPUT & VISUALIZATION
# ============================================================================

cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║  PHASE 4: GENERATING OUTPUTS                                  ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n\n")

# Create summary table
summary_table <- predictions %>%
  select(
    Week = week,
    `Game Day` = gameday,
    `Away Team` = away_team,
    `Home Team` = home_team,
    `Line` = spread_line,
    `Predicted Margin` = predicted_margin,
    `Line Value` = line_value,
    `Winner` = predicted_winner,
    Confidence = confidence_tier,
    `Spread Bet` = betting_edge,
    `Total Line` = total_line,
    `Predicted Total` = predicted_total,
    `Total Value` = total_value,
    `Total Bet` = total_recommendation
  ) %>%
  mutate(
    `Predicted Margin` = round(`Predicted Margin`, 1),
    `Line Value` = round(`Line Value`, 1),
    `Predicted Total` = round(`Predicted Total`, 1),
    `Total Value` = round(`Total Value`, 1)
  )

# Export to Excel
output_file <- file.path(output_dir, paste0("Week_", PREDICTION_WEEK, "_Model_Output.xlsx"))

wb <- createWorkbook()

# Sheet 1: Summary
addWorksheet(wb, "Betting Summary")
writeData(wb, "Betting Summary", summary_table)

# Sheet 2: Detailed Predictions
addWorksheet(wb, "Detailed Analysis")
writeData(wb, "Detailed Analysis", predictions)

# Sheet 3: Team Offense Rankings
addWorksheet(wb, "Offense Rankings")
offense_rankings <- latest_offense %>%
  arrange(desc(roll3_epa)) %>%
  select(offense_team, roll3_epa, roll3_success, roll3_explosive, 
         roll3_ypp, roll3_third_down, roll3_rz_td)
writeData(wb, "Offense Rankings", offense_rankings)

# Sheet 4: Team Defense Rankings
addWorksheet(wb, "Defense Rankings")
defense_rankings <- latest_defense %>%
  arrange(roll3_def_epa) %>%
  select(defense_team, roll3_def_epa, roll3_def_success, roll3_def_explosive,
         roll3_def_ypp, roll3_def_third, roll3_turnovers_forced)
writeData(wb, "Defense Rankings", defense_rankings)

saveWorkbook(wb, output_file, overwrite = TRUE)

cat("✓ Excel file saved:", output_file, "\n\n")

# ============================================================================
# EXPORT CSV FILES FOR ODDS INTEGRATION SCRIPT
# ============================================================================

cat("Exporting CSV files for odds integration...\n")

# Create matchup_analysis subdirectory for odds integration compatibility
matchup_dir <- file.path(output_dir, "matchup_analysis")
if (!dir.exists(matchup_dir)) {
  dir.create(matchup_dir, recursive = TRUE)
}

# Export betting recommendations (what the odds script expects)
betting_recommendations <- predictions %>%
  select(
    matchup_key = game_id,
    away_team, home_team, 
    model_line = predicted_margin,
    confidence = confidence_tier,
    weather_impact = weather_adj,
    home_days_rest = home_rest,
    away_days_rest = away_rest
  ) %>%
  mutate(
    matchup_key = paste0(away_team, " @ ", home_team),
    # Add Thursday game indicator (can be enhanced later with actual game day logic)
    is_thursday = FALSE,
    # Convert weather adjustment to impact scale (0-3)
    weather_impact = case_when(
      abs(weather_impact) >= 1.5 ~ 3,
      abs(weather_impact) >= 1.0 ~ 2,
      abs(weather_impact) >= 0.5 ~ 1,
      TRUE ~ 0
    )
  )

write.csv(betting_recommendations, 
          file.path(matchup_dir, "betting_recommendations_enhanced.csv"), 
          row.names = FALSE)

# Export matchup summary
matchup_summary <- predictions %>%
  select(
    matchup_key = game_id,
    away_team, home_team, gameday,
    predicted_margin, confidence_tier,
    away_off_roll3_epa, home_off_roll3_epa,
    away_def_roll3_def_epa, home_def_roll3_def_epa,
    weather_adj, rest_differential, travel_adj
  ) %>%
  mutate(matchup_key = paste0(away_team, " @ ", home_team))

write.csv(matchup_summary, 
          file.path(matchup_dir, "matchup_summary_enhanced.csv"), 
          row.names = FALSE)

cat("✓ CSV files exported to:", matchup_dir, "\n")
cat("  - betting_recommendations_enhanced.csv\n")
cat("  - matchup_summary_enhanced.csv\n\n")

# ============================================================================
# CONSOLE OUTPUT
# ============================================================================

cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║                    WEEK", PREDICTION_WEEK, "PREDICTIONS SUMMARY                    ║\n")
cat("║                    Based on 2021-2025 Data                     ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n\n")

# Show best betting opportunities
best_bets <- summary_table %>%
  filter(!grepl("No bet|No line|Small lean", `Spread Bet`)) %>%
  select(`Game Day`, `Away Team`, `Home Team`, `Line`, `Line Value`, `Spread Bet`)

if (nrow(best_bets) > 0) {
  cat("TOP BETTING OPPORTUNITIES:\n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  print(best_bets, n = Inf, row.names = FALSE)
  cat("\n")
} else {
  cat("No strong betting edges identified this week.\n\n")
}

# Show all games
cat("\nALL GAMES:\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
print(summary_table %>% 
        select(`Game Day`, `Away Team`, `Home Team`, `Line`, 
               `Predicted Margin`, Confidence), 
      n = Inf, row.names = FALSE)

cat("\n")
cat("════════════════════════════════════════════════════════════════\n")
cat("MODEL RUN COMPLETE\n")
cat("Data Range: 2021-2025 Seasons (through Week", CURRENT_WEEK, ")\n")
cat("Total plays analyzed:", nrow(pbp_analysis), "\n")
cat("Total execution time:", 
    round(difftime(Sys.time(), start_time_phase1, units = "mins"), 2), 
    "minutes\n")
cat("Output saved to:", output_dir, "\n")
cat("════════════════════════════════════════════════════════════════\n\n")