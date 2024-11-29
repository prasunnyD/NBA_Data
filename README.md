# NBA Player Stats Analysis

This repository contains a Python project for analyzing NBA player statistics using various machine learning models and data processing techniques. The project utilizes libraries such as Polars, Scikit-learn, and DuckDB for efficient data handling and analysis.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [Models](#models)
- [Contributing](#contributing)
- [License](#license)

## Features

- Data extraction from the NBA API
- Data processing using Polars and DuckDB
- Machine learning models for player performance prediction (Linear Regression, Ridge Regression)
- Statistical analysis using Poisson distribution
- Model saving and uploading to AWS S3

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

