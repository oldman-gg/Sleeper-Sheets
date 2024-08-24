import requests
import pandas as pd
import gspread
from google.oauth2 import service_account
import json
import os
from datetime import datetime

CONFIG_FILE = 'config.json'

class SleeperSheets:
    """
    This class handles all operations related to fetching data from the Sleeper API,
    processing that data, and uploading it to Google Sheets.
    """

    def __init__(self, config_file=CONFIG_FILE):
        """
        Initializes the class by loading configuration settings from a JSON file.

        Args:
            config_file (str): Path to the JSON configuration file.
        """
        self.config = self.load_config(config_file)
        self.spreadsheet_id = self.config['spreadsheet_id']
        self.service_account_file = self.config['service_account_file']
        self.players_file = self.config['players_file']
        self.league_ids = self.config['league_ids']
        self.current_year = datetime.now().year  # Get the current calendar year
        print(f"Initialized with config: {self.config}")

    def load_config(self, config_file):
        """
        Loads configuration settings from a JSON file.

        Args:
            config_file (str): Path to the JSON configuration file.

        Returns:
            dict: Configuration settings.
        """
        print(f"Loading configuration from {config_file}...")
        with open(config_file, 'r') as file:
            config = json.load(file)
        print("Configuration loaded successfully.")
        return config

    def load_player_data(self):
        """
        Loads player data from a JSON file and extracts player names.

        Returns:
            dict: A dictionary where keys are player IDs and values are player names.
        """
        print(f"Loading player data from {self.players_file}...")
        if os.path.exists(self.players_file):
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

        roster_to_user = {roster['roster_id']: roster['owner_id'] for roster in rosters}
        user_ids = {user['user_id']: user['display_name'] for user in users}
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
        sheet.update(range_name='A1', values=data)

        # Only add the "Weekly Winner" row if this is not "League Records" sheet
        if "League Records" not in sheet_name:
            # Prepare winner row
            weeks = [f"Week {i}" for i in range(1, 19)]  # Assuming 18 weeks
            winner_row = ['Weekly Winner'] + [''] * (len(df.columns) - 1)

            for week in weeks:
                if week in df.columns:
                    # Find the highest scorer for the week
                    week_data = df[['Display Name', week]].sort_values(by=week, ascending=False)
                    if not week_data.empty:
                        highest_score = week_data.iloc[0][week]
                        if highest_score > 0:
                            winner_name = week_data.iloc[0]['Display Name']
                            # Place the winner name in the correct column
                            week_index = df.columns.get_loc(week)
                            winner_row[week_index] = winner_name

            # Add an empty row for better spacing
            last_row = len(df) + 2  # Account for 1-based indexing and header row
            sheet.update(range_name=f'A{last_row}', values=[[''] * len(df.columns)])

            # Add winner row below the empty row
            sheet.update(range_name=f'A{last_row + 1}', values=[winner_row])

    def create_summary_sheet(self, season_dfs):
        """
        Creates or updates "League Records" sheet with the highest-scoring user details.

        Args:
            season_dfs (dict): A dictionary of DataFrames for different seasons.
        """
        print("Creating or updating 'League Records' sheet...")
        if not isinstance(season_dfs, dict):
            raise TypeError("season_dfs should be a dictionary of DataFrames")

        # Prepare a list of DataFrames, filtering out empty ones and dropping all-NA columns
        valid_dfs = []
        for year, df in season_dfs.items():
            if not df.empty:
                df_cleaned = df.dropna(axis=1, how='all')
                valid_dfs.append(df_cleaned)

        # Concatenate DataFrames from all seasons
        if valid_dfs:
            combined_df = pd.concat(valid_dfs, ignore_index=True)

            # Find the highest total points
            highest_total = combined_df['Season Total'].max()
            highest_row = combined_df[combined_df['Season Total'] == highest_total].iloc[0]

            # Identify the season with the highest total points
            highest_user_id = highest_row['User ID']
            season = next(year for year, df in season_dfs.items() if highest_user_id in df['User ID'].values)

            # Prepare summary DataFrame
            summary_df = pd.DataFrame({
                'Record': ['Most Points Scored In A Total Season'],
                'Season': [season],
                'User ID': [highest_row['User ID']],
                'Display Name': [highest_row['Display Name']],
                'Total Points': [highest_total]
            })
            self.upload_to_google_sheets('League Records', summary_df)
        else:
            print("No valid data available to create summary.")

    def run(self):
        """
        Main method to run the data processing and uploading tasks.
        """
        print("Starting the script execution...")

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

        # Create or update "League Records" sheet
        self.create_summary_sheet(season_dfs)
        print("Script execution completed.")


if __name__ == '__main__':
    # Use 'config-theleague.json' or 'config-worldleague.json' based on your needs
    config_file = CONFIG_FILE  # Example config file
    sleeper_sheets = SleeperSheets(config_file)
    sleeper_sheets.run()
