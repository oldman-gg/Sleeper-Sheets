import requests
import pandas as pd
import gspread
from google.oauth2 import service_account
import json
import os
from datetime import datetime

CONFIG_FILE = 'config-test.json'  # Path to your configuration file

# Load the configuration from config.json
with open(CONFIG_FILE, 'r') as config_file:
    config = json.load(config_file)

class SleeperSheets:
    """
    This class handles all operations related to fetching data from the Sleeper API,
    processing that data, and uploading it to Google Sheets.
    """

    def __init__(self, config):
        """
        Initializes the class using the provided configuration.

        Args:
            config (dict): Configuration settings.
        """
        self.config = config
        self.spreadsheet_id = self.config['spreadsheet_id']
        self.service_account_file = self.config['service_account_file']
        self.players_file = self.config.get('players_file')
        self.league_ids = self.config['league_ids']
        self.current_year = datetime.now().year  # Get the current calendar year
        print(f"Initialized SleeperSheets with config: {self.config}")

    def load_player_data(self):
        """
        Loads player data from a JSON file and extracts player names.

        Returns:
            dict: A dictionary where keys are player IDs and values are player names.
        """
        print(f"Loading player data from {self.players_file}...")
        if self.players_file and os.path.exists(self.players_file):
            with open(self.players_file, 'r') as file:
                players_data = json.load(file)
            player_names = {pid: pdata.get('full_name', 'Unknown Player') for pid, pdata in players_data.items()}
            print(f"Player data loaded successfully. Found {len(player_names)} players.")
            return player_names
        else:
            print(f"Error: Player data file {self.players_file} not found.")
            return {}

    def fetch_users(self, league_id, year):
        """
        Fetches user data for a specific league.

        Args:
            league_id (str): The ID of the league to fetch users from.
            year (str): The year of the league for error reporting.

        Returns:
            list: A list of user data dictionaries.
        """
        print(f"Fetching user data for league ID {league_id} (Year: {year})...")
        url = f'https://api.sleeper.app/v1/league/{league_id}/users'
        response = requests.get(url)
        if response.status_code == 200:
            users = response.json()
            print(f"Fetched {len(users)} users.")
            return users
        else:
            print(f"Error: Unable to fetch user data for league year {year} (Status code: {response.status_code})")
            return []

    def fetch_matchups(self, league_id, week, year):
        """
        Fetches matchup data for a specific week of a league.

        Args:
            league_id (str): The ID of the league to fetch matchups from.
            week (int): The week number to fetch data for.
            year (str): The year of the league for error reporting.

        Returns:
            list: A list of matchup data dictionaries.
        """
        print(f"Fetching matchup data for league ID {league_id}, week {week} (Year: {year})...")
        url = f'https://api.sleeper.app/v1/league/{league_id}/matchups/{week}'
        response = requests.get(url)
        if response.status_code == 200:
            matchups = response.json()
            print(f"Fetched {len(matchups)} matchups for week {week}.")
            return matchups
        else:
            print(f"Error: Unable to fetch matchup data for league year {year}, week {week} (Status code: {response.status_code})")
            return []

    def fetch_rosters(self, league_id, year):
        """
        Fetches roster data for a specific league.

        Args:
            league_id (str): The ID of the league to fetch rosters from.
            year (str): The year of the league for error reporting.

        Returns:
            list: A list of roster data dictionaries.
        """
        print(f"Fetching roster data for league ID {league_id} (Year: {year})...")
        url = f'https://api.sleeper.app/v1/league/{league_id}/rosters'
        response = requests.get(url)
        if response.status_code == 200:
            rosters = response.json()
            print(f"Fetched {len(rosters)} rosters.")
            return rosters
        else:
            print(f"Error: Unable to fetch roster data for league year {year} (Status code: {response.status_code})")
            return []

    def filter_rows(self, df):
        """
        Filters out rows where more than 5 weeks have a value of 0.

        Args:
            df (pd.DataFrame): The DataFrame to filter.

        Returns:
            pd.DataFrame: The filtered DataFrame.
        """
        print("Filtering rows with more than 5 weeks of zero points...")
        week_columns = [col for col in df.columns if col.startswith('Week')]
        filtered_df = df[df[week_columns].eq(0).sum(axis=1) <= 5]
        print(f"Filtered DataFrame has {len(filtered_df)} rows.")
        return filtered_df

    def process_season(self, league_id, year):
        """
        Processes the season data for a specific league and year.

        Args:
            league_id (str): The ID of the league to process.
            year (str): The year of the league.

        Returns:
            pd.DataFrame: A DataFrame containing weekly points and total points for each user.
        """
        print(f"Processing season data for league ID {league_id} (Year: {year})...")
        users = self.fetch_users(league_id, year)
        rosters = self.fetch_rosters(league_id, year)

        if not users or not rosters:
            print(f"No data available for league year {year}.")
            return pd.DataFrame()

        roster_to_user = {roster['roster_id']: roster.get('owner_id', None) for roster in rosters}
        user_ids = {user['user_id']: user.get('display_name', 'Unknown') for user in users}
        user_data = {user_id: {'display_name': display_name} for user_id, display_name in user_ids.items()}

        weeks = range(1, 19)  # Assuming 18 weeks in the season

        for week in weeks:
            print(f"Processing week {week}...")
            matchups = self.fetch_matchups(league_id, week, year)
            if matchups:
                for matchup in matchups:
                    roster_id = matchup['roster_id']
                    points = matchup['points']
                    user_id = roster_to_user.get(roster_id)
                    if user_id and user_id in user_data:
                        user_data[user_id][f"Week {week}"] = points

        rows = []
        for user_id, data in user_data.items():
            row = [user_id, data.get('display_name', 'N/A')]
            total_points = 0
            for week in weeks:
                week_points = data.get(f"Week {week}", 0)
                row.append(week_points)
                total_points += week_points
            row.append(total_points)
            rows.append(row)

        df = pd.DataFrame(rows,
                          columns=['User ID', 'Display Name'] + [f"Week {week}" for week in weeks] + ['Season Total'])

        if year != str(self.current_year):
            df = self.filter_rows(df)

        print(f"Processed data for league year {year}. DataFrame shape: {df.shape}.")
        return df

    def upload_to_google_sheets(self, sheet_name, df):
        """
        Uploads a DataFrame to a specific sheet in Google Sheets.

        Args:
            sheet_name (str): The name of the sheet to upload data to.
            df (pd.DataFrame): The DataFrame to upload.
        """
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_file,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(self.spreadsheet_id)

        try:
            sheet = spreadsheet.worksheet(sheet_name)
            sheet.clear()  # Clear existing data
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")  # Create new sheet if not found

        # Update sheet with DataFrame data
        data = [df.columns.values.tolist()] + df.values.tolist()
        sheet.update('A1', data)

    def run(self):
        """
        Main method to run the data processing and uploading tasks.
        """
        print("Starting SleeperSheets execution...")

        # Process each season
        season_dfs = {}
        for year, league_id in self.league_ids.items():
            if league_id:  # Check if league_id is not empty
                print(f"Processing {year} season data...")
                season_df = self.process_season(league_id, year)
                sheet_name = f"{year} Season - Weekly Points"
                if not season_df.empty:
                    print(f"Uploading {year} season data to Google Sheets.")
                    self.upload_to_google_sheets(sheet_name, season_df)
                    season_dfs[year] = season_df
                else:
                    print(f"No data available for {year} season.")

        print("SleeperSheets execution completed.")

class MarginCalculator:
    """
    This class handles fetching matchup data from the Sleeper API,
    calculating the largest and smallest margins of victory,
    and uploading the results to Google Sheets.
    """

    def __init__(self, config, processed_weeks_file='processed_weeks_margin_calculator.log'):
        """
        Initializes the class by loading configuration settings from a dictionary.
        """
        self.config = config
        self.spreadsheet_id = self.config['spreadsheet_id']
        self.service_account_file = self.config['service_account_file']
        self.league_ids = self.config['league_ids']
        self.processed_weeks_file = processed_weeks_file
        self.gc = self.authorize_google_sheets()
        self.spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
        self.processed_weeks = self.load_processed_weeks()
        # Flags to determine if sheets exist
        self.largest_margin_sheet_exists = self.sheet_exists("Largest Margin")
        self.smallest_margin_sheet_exists = self.sheet_exists("Smallest Margin")
        # If either sheet does not exist, process all data
        self.process_all_data = not (self.largest_margin_sheet_exists and self.smallest_margin_sheet_exists)

    def authorize_google_sheets(self):
        """
        Authorizes and returns a gspread client for Google Sheets.
        """
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_file, scopes=scopes)
        return gspread.authorize(credentials)

    def sheet_exists(self, sheet_name):
        """
        Checks if a sheet exists in the spreadsheet.
        """
        try:
            self.spreadsheet.worksheet(sheet_name)
            return True
        except gspread.exceptions.WorksheetNotFound:
            return False

    def open_or_create_sheet(self, sheet_name):
        """
        Opens an existing worksheet or creates a new one if it doesn't exist.
        """
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="10")
        return worksheet

    def load_processed_weeks(self):
        """
        Loads the processed weeks from a log file in format 'year,week' per line.
        If the file does not exist, it creates an empty set.
        """
        processed_weeks = set()
        if os.path.exists(self.processed_weeks_file):
            with open(self.processed_weeks_file, 'r') as file:
                for line in file:
                    line = line.strip()
                    if line:
                        processed_weeks.add(line)
        return processed_weeks

    def save_processed_week(self, year, week):
        """
        Appends the processed week to the log file.
        """
        with open(self.processed_weeks_file, 'a') as file:
            file.write(f"{year},{week}\n")
        self.processed_weeks.add(f"{year},{week}")

    def get_user_mappings(self, league_id):
        """
        Retrieves mappings between user IDs and display names.
        """
        users_url = f'https://api.sleeper.app/v1/league/{league_id}/users'
        users = self.fetch_data(users_url)
        if users:
            return {user['user_id']: user.get('display_name', 'Unknown') for user in users}
        else:
            return {}

    def get_roster_mappings(self, league_id):
        """
        Retrieves mappings between roster IDs and owner IDs.
        """
        rosters_url = f'https://api.sleeper.app/v1/league/{league_id}/rosters'
        rosters = self.fetch_data(rosters_url)
        if rosters:
            return {roster['roster_id']: roster.get('owner_id', 'Unknown') for roster in rosters}
        else:
            return {}

    def fetch_data(self, url):
        """
        Fetches data from a given URL and returns the JSON response.
        """
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching data from {url} (Status code: {response.status_code})")
            return None

    def is_week_processed(self, year, week):
        """
        Checks if a particular week of a year has already been processed.
        """
        return f"{year},{week}" in self.processed_weeks

    def process_league(self, league_id, year, largest_margins, smallest_margins):
        """
        Processes a league to calculate largest and smallest margins of victory.
        """
        user_mapping = self.get_user_mappings(league_id)
        roster_mapping = self.get_roster_mappings(league_id)

        if not user_mapping or not roster_mapping:
            print(f"Skipping league {league_id} for year {year} due to missing data.")
            return

        stop_processing = False  # Flag to stop processing if both winner and loser points are zero

        for week in range(1, 19):  # Weeks 1 to 18
            if stop_processing:
                print(f"Stopping processing for year {year} as both teams scored zero in week {week - 1}.")
                break

            if not self.process_all_data and self.is_week_processed(year, week):
                print(f"Week {week} of Year {year} has already been processed. Skipping.")
                continue

            print(f"Processing Week {week} for Year {year}...")
            matchups_url = f'https://api.sleeper.app/v1/league/{league_id}/matchups/{week}'
            matchups = self.fetch_data(matchups_url)

            if not matchups:
                continue

            # Organize matchups by matchup_id
            matchup_dict = {}
            for matchup in matchups:
                matchup_id = matchup['matchup_id']
                if matchup_id not in matchup_dict:
                    matchup_dict[matchup_id] = []
                matchup_dict[matchup_id].append(matchup)

            margins = []
            for matchup_id, teams in matchup_dict.items():
                if len(teams) != 2:
                    # In case of bye weeks or incomplete data
                    continue

                team1, team2 = teams
                points1 = team1.get('points', 0)
                points2 = team2.get('points', 0)
                roster_id1 = team1['roster_id']
                roster_id2 = team2['roster_id']

                owner_id1 = roster_mapping.get(roster_id1, 'Unknown')
                owner_id2 = roster_mapping.get(roster_id2, 'Unknown')
                owner_name1 = user_mapping.get(owner_id1, 'Unknown')
                owner_name2 = user_mapping.get(owner_id2, 'Unknown')

                margin = abs(points1 - points2)
                if points1 >= points2:
                    winner = owner_name1
                    loser = owner_name2
                    winner_points = points1
                    loser_points = points2
                else:
                    winner = owner_name2
                    loser = owner_name1
                    winner_points = points2
                    loser_points = points1

                # Check if both winner and loser points are zero
                if winner_points == 0 and loser_points == 0:
                    print(f"Both teams scored zero points in Year {year}, Week {week}. Stopping further processing.")
                    stop_processing = True
                    break  # Break out of the matchup loop

                margins.append({
                    'Year': int(year),
                    'Week': week,
                    'Winner': winner,
                    'Loser': loser,
                    'Winner Points': winner_points,
                    'Loser Points': loser_points,
                    'Margin': margin
                })

            if stop_processing:
                break  # Break out of the week loop

            if margins:
                # Find largest and smallest margins for this week
                largest_margin = max(margins, key=lambda x: x['Margin'])
                smallest_margin = min(margins, key=lambda x: x['Margin'])
                largest_margins.append(largest_margin)
                smallest_margins.append(smallest_margin)

                # Mark the week as processed
                self.save_processed_week(year, week)

    def ensure_headers(self, worksheet, headers):
        """
        Ensures that the headers are present in the worksheet.
        If not, it inserts them at the top.
        """
        existing_headers = worksheet.row_values(1)
        if existing_headers != headers:
            print(f"Headers are missing or incorrect in sheet '{worksheet.title}'. Updating headers.")
            worksheet.insert_row(headers, index=1)

    def upload_results(self, sheet_name, data):
        """
        Uploads the results to a specified Google Sheet.
        """
        worksheet = self.open_or_create_sheet(sheet_name)
        headers = ['Year', 'Week', 'Winner', 'Loser', 'Winner Points', 'Loser Points', 'Margin']

        # Ensure headers are present
        self.ensure_headers(worksheet, headers)

        if self.process_all_data:
            # Clear the sheet if we are processing all data
            worksheet.clear()
            worksheet.append_row(headers)
            # Prepare all data for upload
            rows = []
            for entry in data:
                row = [
                    entry['Year'],
                    entry['Week'],
                    entry['Winner'],
                    entry['Loser'],
                    entry['Winner Points'],
                    entry['Loser Points'],
                    entry['Margin']
                ]
                rows.append(row)
            # Batch update to improve performance
            worksheet.append_rows(rows, value_input_option='USER_ENTERED')
        else:
            # Read existing data to prevent duplication
            existing_data = worksheet.get_all_records()
            existing_rows = [
                (int(row['Year']), int(row['Week'])) for row in existing_data
            ]

            new_rows = []
            for entry in data:
                key = (entry['Year'], entry['Week'])
                if key not in existing_rows:
                    row = [
                        entry['Year'],
                        entry['Week'],
                        entry['Winner'],
                        entry['Loser'],
                        entry['Winner Points'],
                        entry['Loser Points'],
                        entry['Margin']
                    ]
                    new_rows.append(row)

            if new_rows:
                # Append new rows to the worksheet
                worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            else:
                print(f"No new data to upload for sheet {sheet_name}.")

    def run(self):
        """
        Main execution method.
        """
        print("Starting MarginCalculator execution...")
        largest_margins = []
        smallest_margins = []
        for year, league_id in self.league_ids.items():
            if league_id:
                print(f"Processing League ID: {league_id} for Year: {year}")
                self.process_league(league_id, year, largest_margins, smallest_margins)
            else:
                print(f"No League ID provided for Year: {year}")

        # After processing all leagues, upload the aggregated results
        if largest_margins:
            # Sort the data by Year and Week before uploading
            largest_margins.sort(key=lambda x: (x['Year'], x['Week']))
            self.upload_results("Largest Margin", largest_margins)
        if smallest_margins:
            smallest_margins.sort(key=lambda x: (x['Year'], x['Week']))
            self.upload_results("Smallest Margin", smallest_margins)

        print("MarginCalculator execution completed.")

class HighestScorerProcessor:
    """
    This class handles fetching data from the Sleeper API to find the highest scoring rostered player
    for each week, and uploading the results to Google Sheets.
    """
    def __init__(self, config, processed_weeks_file='processed_weeks_highest_scorer.log'):
        self.config = config
        self.spreadsheet_id = self.config['spreadsheet_id']
        self.service_account_file = self.config['service_account_file']
        self.league_ids = self.config['league_ids']
        self.gc = self.authorize_google_sheets()
        self.spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
        self.SHEET_NAME = 'Most Points Generated by Rostered Player All-Time'
        self.processed_weeks_file = processed_weeks_file
        self.worksheet = self.open_or_create_sheet(self.SHEET_NAME)
        # Write headers if the sheet is new
        self.ensure_headers(self.worksheet, ['Year', 'Week', 'Display Name', 'Player Name', 'Points', 'Player ID'])
        self.processed_weeks = self.read_processed_weeks()

    def authorize_google_sheets(self):
        """
        Authorizes and returns a gspread client for Google Sheets.
        """
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_file, scopes=scopes)
        return gspread.authorize(credentials)

    def open_or_create_sheet(self, sheet_name):
        """
        Opens an existing worksheet or creates a new one if it doesn't exist.
        """
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="10")
        return worksheet

    def ensure_headers(self, worksheet, headers):
        """
        Ensures that the headers are present in the worksheet.
        If not, it inserts them at the top.
        """
        existing_headers = worksheet.row_values(1)
        if existing_headers != headers:
            print(f"Headers are missing or incorrect in sheet '{worksheet.title}'. Updating headers.")
            worksheet.insert_row(headers, index=1)

    def read_processed_weeks(self):
        """
        Reads processed weeks from the log file.
        """
        processed_weeks = set()
        if os.path.exists(self.processed_weeks_file):
            with open(self.processed_weeks_file, 'r') as file:
                for line in file:
                    line = line.strip()
                    if line:
                        processed_weeks.add(line)
        return processed_weeks

    def save_processed_week(self, year, week):
        """
        Appends the processed week to the log file.
        """
        with open(self.processed_weeks_file, 'a') as file:
            file.write(f"{year},{week}\n")
        self.processed_weeks.add(f"{year},{week}")

    def is_week_processed(self, year, week):
        """
        Checks if a particular week of a year has already been processed.
        """
        return f"{year},{week}" in self.processed_weeks

    def process_week(self, league_id, year, week):
        """
        Processes data for a given week to find the highest scoring rostered player.
        """
        # Define the API endpoints for the current week
        matchups_url = f'https://api.sleeper.app/v1/league/{league_id}/matchups/{week}'
        rosters_url = f'https://api.sleeper.app/v1/league/{league_id}/rosters'
        users_url = f'https://api.sleeper.app/v1/league/{league_id}/users'

        # Initialize the response variable
        highest_scorer = None

        try:
            # Make the GET request for matchups
            matchups_response = requests.get(matchups_url)
            matchups_response.raise_for_status()
            matchups = matchups_response.json()

            # Make the GET request for rosters
            rosters_response = requests.get(rosters_url)
            rosters_response.raise_for_status()
            rosters = rosters_response.json()

            # Make the GET request for users to get display names and user info
            users_response = requests.get(users_url)
            users_response.raise_for_status()
            users = users_response.json()

            # Create a mapping from user_id to user details
            user_mapping = {user['user_id']: {
                'display_name': user.get('display_name', 'Unknown'),
                'username': user.get('username', 'Unknown')
            } for user in users}

            # Create a mapping from roster_id to owner_id (user_id)
            roster_mapping = {roster['roster_id']: roster['owner_id'] for roster in rosters}

            # Fetch the player information from the Sleeper API
            players_response = requests.get('https://api.sleeper.app/v1/players/nfl')
            players_response.raise_for_status()
            players = players_response.json()

            # Create a mapping from player_id to player details (full name)
            player_mapping = {player_id: {
                'first_name': player.get('first_name', 'Unknown'),
                'last_name': player.get('last_name', 'Unknown'),
                'full_name': f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
            } for player_id, player in players.items()}

            # Variables to keep track of the highest scorer
            highest_points = float('-inf')

            # Process the matchups
            for matchup in matchups:
                roster_id = matchup['roster_id']
                starters = matchup['starters']
                starters_points = matchup.get('starters_points', [])

                # Get the user_id from the roster mapping
                user_id = roster_mapping.get(roster_id, 'Unknown')

                # Get the user details from the user mapping
                user_details = user_mapping.get(user_id, {})
                display_name = user_details.get('display_name', 'Unknown')

                # Create a list of dictionaries containing player IDs and their corresponding points
                starters_with_points = [
                    {'player_id': player_id, 'points': starters_points[idx]}
                    for idx, player_id in enumerate(starters)
                ]

                # Check if any starter has the highest points
                for starter in starters_with_points:
                    if starter['points'] > highest_points:
                        highest_points = starter['points']
                        highest_scorer = {
                            'player_id': starter['player_id'],
                            'points': starter['points'],
                            'user_id': user_id,
                            'display_name': display_name,
                            'player_name': player_mapping.get(starter['player_id'], {}).get('full_name', 'Unknown Player')
                        }
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        # If highest points are 0 or less, return None and stop processing
        if highest_points <= 0:
            return None

        return highest_scorer

    def process_year(self, year, league_id):
        """
        Processes data for all weeks in a given year and league.
        """
        weeks_to_process = range(1, 19)  # Process weeks 1 through 18

        for week in weeks_to_process:
            if not self.is_week_processed(year, week):
                print(f"Processing Year {year}, Week {week}...")
                highest_scorer = self.process_week(league_id, year, week)
                if highest_scorer:
                    # Append new record
                    self.worksheet.append_row([
                        int(year),
                        int(week),
                        highest_scorer['display_name'],
                        highest_scorer['player_name'],
                        float(highest_scorer['points']),
                        highest_scorer['player_id']
                    ])
                    self.save_processed_week(year, week)
                else:
                    print(f"No valid highest scorer for Year {year} - Week {week}. Stopping further processing.")
                    break  # Stop processing further weeks if no valid highest scorer is found
            else:
                print(f"Year {year} - Week {week} has already been processed.")

    def run(self):
        print("Starting HighestScorerProcessor execution...")
        for year, league_id in self.league_ids.items():
            if league_id:
                self.process_year(year, league_id)
            else:
                print(f"No League ID provided for Year: {year}")
        print("HighestScorerProcessor execution completed.")

if __name__ == '__main__':
    # Instantiate and run SleeperSheets
    sleeper_sheets = SleeperSheets(config)
    sleeper_sheets.run()

    # Instantiate and run MarginCalculator
    margin_calculator = MarginCalculator(config)
    margin_calculator.run()

    # Instantiate and run HighestScorerProcessor
    highest_scorer_processor = HighestScorerProcessor(config)
    highest_scorer_processor.run()
