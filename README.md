# Sleeper-Sheets
## Overview
The SleeperSheets script fetches fantasy football data from the Sleeper API, processes it, and uploads it to Google Sheets. The script supports multiple seasons and generates a summary of the highest-scoring user across all seasons.

## Features
Fetches user, matchup, and roster data from the Sleeper API.
Processes weekly points for each user and calculates the season total.
Uploads processed data to Google Sheets.
Creates a summary sheet to highlight the highest-scoring user across seasons.
Prerequisites
Python 3.x
Google Sheets API access
requests, pandas, gspread, google-auth libraries

## Setup
Clone the Repository
- git clone https://github.com/yourusername/your-repo.git

- cd your-repo

## Install Dependencies

Ensure you have the required Python libraries installed:
pip install requests pandas gspread google-auth
