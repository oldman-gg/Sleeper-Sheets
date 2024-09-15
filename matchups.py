import requests
import json

# Load the configuration from config.json
with open('config.json') as config_file:
    config = json.load(config_file)

# Extract the league ID for the 2024 season from the config
league_id = config['league_ids']['2024']

# Define the API endpoints
players_url = 'https://api.sleeper.app/v1/players/nfl'

# Function to process data for a given week
def process_week(week):
    # Define the API endpoints for the current week
    matchups_url = f'https://api.sleeper.app/v1/league/{league_id}/matchups/{week}'
    rosters_url = f'https://api.sleeper.app/v1/league/{league_id}/rosters'
    users_url = f'https://api.sleeper.app/v1/league/{league_id}/users'

    # Make the GET request for matchups
    matchups_response = requests.get(matchups_url)

    # Check if the request was successful for matchups
    if matchups_response.status_code == 200:
        matchups = matchups_response.json()  # Parse the JSON response for matchups

        # Make the GET request for rosters
        rosters_response = requests.get(rosters_url)

        # Check if the rosters request was successful
        if rosters_response.status_code == 200:
            rosters = rosters_response.json()  # Parse the JSON response for rosters

            # Make the GET request for users to get display names and user info
            users_response = requests.get(users_url)

            # Check if the users request was successful
            if users_response.status_code == 200:
                users = users_response.json()  # Parse the JSON response for users

                # Create a mapping from user_id to user details (display_name, etc.)
                user_mapping = {user['user_id']: {
                    'display_name': user.get('display_name', 'Unknown'),
                    'username': user.get('username', 'Unknown')
                } for user in users}

                # Create a mapping from roster_id to owner_id (user_id)
                roster_mapping = {roster['roster_id']: roster['owner_id'] for roster in rosters}

                # Fetch the player information from the Sleeper API
                players_response = requests.get(players_url)

                # Check if the players request was successful
                if players_response.status_code == 200:
                    players = players_response.json()  # Parse the JSON response for players

                    # Create a mapping from player_id to player details (full name)
                    player_mapping = {player_id: {
                        'first_name': player.get('first_name', 'Unknown'),
                        'last_name': player.get('last_name', 'Unknown'),
                        'full_name': f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
                    } for player_id, player in players.items()}

                    # Variables to keep track of the highest scorer
                    highest_scorer = None
                    highest_points = float('-inf')  # Start with negative infinity so any player can beat it

                    # Process the matchups
                    for matchup in matchups:
                        roster_id = matchup['roster_id']
                        points = matchup['points']
                        starters = matchup['starters']
                        starters_points = matchup.get('starters_points', [])  # Ensure we handle cases where this field might be missing

                        # Get the user_id from the roster mapping
                        user_id = roster_mapping.get(roster_id, 'Unknown')

                        # Get the user details from the user mapping
                        user_details = user_mapping.get(user_id, {})
                        display_name = user_details.get('display_name', 'Unknown')
                        username = user_details.get('username', 'Unknown')

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

                    # Return the highest scorer if found, otherwise return None
                    return highest_scorer if highest_scorer else None
                else:
                    print(f"Failed to retrieve players for week {week}: {players_response.status_code}")
                    return None
            else:
                print(f"Failed to retrieve users for week {week}: {users_response.status_code}")
                return None
        else:
            print(f"Failed to retrieve rosters for week {week}: {rosters_response.status_code}")
            return None
    else:
        print(f"Failed to retrieve matchups for week {week}: {matchups_response.status_code}")
        return None

# Loop through weeks and process data
week = 1  # Start from week 1
while True:
    print(f"Processing Week {week}...")
    highest_scorer = process_week(week)
    
    # Check if the week has not started or no data found
    if highest_scorer is None:
        print(f"Week {week} has not started or has no data.")
        break
    
    # Stop the script if the highest score is 0
    if highest_scorer['points'] == 0:
        print(f"Week {week} has no points.")
        break

    # Print details for weeks where there is a high score
    if highest_scorer['points'] > 0:
        print(f"Week {week} - Highest Scorer:")
        print(f"Player Name: {highest_scorer['player_name']}")
        print(f"Points: {highest_scorer['points']}")
        print(f"User ID: {highest_scorer['user_id']}")
        print(f"Display Name: {highest_scorer['display_name']}")
        print()

    # Increment the week
    week += 1
