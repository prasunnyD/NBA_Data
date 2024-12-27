# NBA Player Stats Analysis

This repository provides a comprehensive NBA analytics platform that collects, processes and analyzes player and team statistics. It features automated data pipelines built with Prefect that extract data from the NBA API, store it efficiently in DuckDB databases, and transform it using Polars for high-performance data manipulation. The platform enables analysis of player performance, team matchups, and game outcomes through statistical modeling and machine learning techniques.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [Models](#models)
- [FastAPI Integration](#fastapi-integration)
  - [API Endpoints](#api-endpoints)
  - [Running the API](#running-the-api)
  - [CORS Configuration](#cors-configuration)
- [Prefect](#prefect)
  - [Pipeline Features](#pipeline-features)
  - [Running Prefect Workflows](#running-prefect-workflows)
  - [Monitoring](#monitoring)
- [Contributing](#contributing)
- [License](#license)

## Features

- Automated data extraction from NBA API for player and team statistics
- High-performance data processing using Polars and DuckDB
- Player statistics tracking including:
  - Career stats and season averages
  - Game-by-game box scores and advanced metrics
  - Rolling averages and trends
- Team analytics including:
  - Roster management
  - Opponent matchup analysis
  - Advanced team metrics (pace, efficiency, etc.)
- Scheduled data pipelines with Prefect for automated updates
- Database integration with DuckDB for efficient querying
- Statistical modeling capabilities:
  - Player performance prediction
  - Game outcome analysis
  - Custom metric calculations

## Installation

To set up the project, follow these steps:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/nba-player-stats-analysis.git
   cd nba-player-stats-analysis
   ```

2. **Install python3.11:**
   Make sure you have Python 3.11 or later installed. If not, install it using Homebrew:
   ```bash
   brew install python@3.11
   ```

3. **Install required packages and create a virtual environment:**
   ```bash
   poetry install
   ```

4. **Start virtual environment:**
   ```bash
   poetry shell
   ```

## Usage

### Example
```
antman = Player('Anthony Edwards','Minnesota')
predictors=["OPP_EFG_PCT","OPP_FTA_RATE","OPP_OREB_PCT",'PACE','MIN']
query = f"SELECT GAME_DATE,PTS,MIN FROM player_boxscores WHERE Player_ID = '{antman.id}' LIMIT 10"
conn = duckdb.connect("player_boxscores.db")
pts_df = conn.sql(query).pl()
create_model(year='22022', stats=pts_df, predictors=predictors,stat='PTS',model_filename="anthony_edwards_points_model.sav")
results = predict_result_polars('anthony_edwards_points_model.sav','Atlanta',37.8)
```

Eventually the api will be built out and that will be the main form of usage. 

## Data Sources

Most of the data is sourced from the nba api
https://github.com/swar/nba_api

## Models

Models are stored in s3


## FastAPI Integration

This project includes a REST API built with FastAPI that provides endpoints for NBA player and team statistics analysis.

### API Endpoints

#### Player Predictions
- `POST /points-prediction/{player_name}`
  - Predicts player points based on opponent and minutes
  - Parameters:
    - `player_name`: Player's full name
    - `city`: Team city
    - `opp_city`: Opponent's city
    - `minutes`: Projected minutes

#### Statistical Analysis
- `POST /poisson_dist`
  - Calculates probability distributions for over/under betting lines
  - Parameters:
    - `predictedPoints`: Model's point prediction
    - `bookLine`: Betting line to compare against

#### Historical Data
- `GET /player-last-{x}-games/{name}`
  - Retrieves player's last X games statistics
  - Returns: points, assists, rebounds, and minutes per game
  
- `GET /team-last-10-games/{city}`
  - Retrieves team's last 10 games scoring data
  
- `GET /opponent-team-stats/{city}/{number_of_days}`
  - Retrieves opponent team's advanced statistics

### Running the API

Start the FastAPI server with:
```bash
uvicorn api:app --reload
```

The API will be available at `http://localhost:8000`. Access the interactive API documentation at `http://localhost:8000/docs`.

### CORS Configuration

The API is configured to allow requests from the frontend application running on `http://localhost:5173` (Vite's default port).

// ... existing code ...

## Prefect

This project uses Prefect for workflow orchestration and scheduling of data pipelines. The main pipeline (`pipelines.py`) handles automated data collection for NBA teams and players.

### Pipeline Features
- Automated team and player data population
- Scheduled daily updates at 8:00 AM
- Error handling and logging for data collection
- Rate-limited API calls to prevent throttling

### Running Prefect Workflows

1. **Start the Prefect server:**
   ```bash
   prefect server start
   ```

2. **Deploy the pipeline:**
   ```bash
   python pipelines.py
   ```

This will create a deployment that runs daily at 8:00 AM, populating player boxscore data for all NBA teams.

### Monitoring

You can monitor workflow runs through the Prefect UI at `http://localhost:4200` after starting the server.



