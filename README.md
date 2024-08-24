# Sleeper-Sheets
Sleeper-Sheets is a Python script that integrates with the Sleeper fantasy football API and Google Sheets. It fetches league data, processes it, and uploads the results to Google Sheets for tracking and analysis.

## Features
Fetches user, roster, and matchup data from the Sleeper API.
Processes weekly and seasonal fantasy football data.
Filters out users with excessive weeks of zero points.
Uploads processed data to Google Sheets.
Creates or updates a summary sheet with league records.

## Installation
Clone the Repository

- git clone https://github.com/yourusername/Sleeper-Sheets.git
- cd Sleeper-Sheets

Create a Virtual Environment

- python -m venv venv
- source venv/bin/activate  # On Windows use `venv\Scripts\activate`

Install Dependencies 

- pip install -r requirements.txt 

Setup Configuration
- Copy the config-example.json to config.json and fill in your configuration details:
{
    "spreadsheet_id": "your_google_sheet_id",
    "service_account_file": "path_to_your_service_account_file.json",
    "players_file": "path_to_players_data.json",
    "league_ids": {
        "2021": "your_league_id_2021",
        "2022": "your_league_id_2022",
        "2023": "your_league_id_2023",
        "2024": "your_league_id_2024"
    }
}
## Usage
Set Up Google Sheets API
- Ensure you have a Google service account and have shared your Google Sheets with the service account email address.

Run the Script
- python main.py 
- This will initialize the script, process the season data for each league, and upload the results to Google Sheets.

## Script Overview

### SleeperSheets Class
The SleeperSheets class handles the following operations:

- Initialization (__init__): Loads configuration settings from config.json.
- Load Player Data (load_player_data): Loads player data from a JSON file.
- Fetch Users (fetch_users): Retrieves user data for a given league.
- Fetch Matchups (fetch_matchups): Retrieves matchup data for a specific week.
- Fetch Rosters (fetch_rosters): Retrieves roster data for a league.
- Filter Rows (filter_rows): Filters out users with more than 5 weeks of zero points.
- Process Season (process_season): Processes the season data and returns a DataFrame.
- Upload to Google Sheets (upload_to_google_sheets): Uploads a DataFrame to a specified Google Sheets sheet.
- Create Summary Sheet (create_summary_sheet): Creates or updates the "League Records" sheet with the highest-scoring user details.
- Run (run): Main method to execute data processing and uploading tasks.

### Configuration File

The configuration file config.json includes:
- spreadsheet_id: Google Sheets ID where data will be uploaded.
- service_account_file: Path to the Google service account credentials JSON file.
- players_file: Path to the JSON file containing player data.
- league_ids: Dictionary mapping years to Sleeper league IDs.
### Google Sheets Setup

Ensure that:

- You have a valid Google Sheets ID.
- Your service account has access to the Google Sheet

## Contributing
Feel free to submit issues or pull requests if you have improvements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.

Replace placeholders like yourusername, your_google_sheet_id, and path_to_your_service_account_file.json with your actual details.