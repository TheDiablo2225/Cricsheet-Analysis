import os
import zipfile
import json
import uuid
import mysql.connector

# Connect to MySQL database
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mysql",
    database="CricketData"
)
cursor = db.cursor()

# Function to insert match data
def insert_match_info(data, match_id):
    match_info = data['info']
    outcome = match_info.get('outcome', {})
    
    query = """
    INSERT INTO MatchInfo (
        match_id, city, venue, match_type, season, team1, team2, toss_winner, toss_decision, 
        winner, win_by_runs, win_by_wickets
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        match_id,
        match_info.get('city'),
        match_info.get('venue'),
        match_info.get('match_type'),
        match_info.get('season'),
        match_info['teams'][0],
        match_info['teams'][1],
        match_info['toss']['winner'],
        match_info['toss']['decision'],
        outcome.get('winner'),
        outcome.get('by', {}).get('runs', None),
        outcome.get('by', {}).get('wickets', None)
    )
    cursor.execute(query, values)
    db.commit()

# Function to insert players
def insert_players(data, match_id):
    for team, players in data['info']['players'].items():
        for player in players:
            query = "INSERT INTO Players (match_id, team, player_name) VALUES (%s, %s, %s)"
            values = (match_id, team, player)
            cursor.execute(query, values)
    db.commit()

# Function to insert innings and deliveries
def insert_innings_and_deliveries(data, match_id):
    for inning_data in data['innings']:
        team = inning_data['team']
        
        # Insert innings data
        cursor.execute("INSERT INTO Innings (match_id, team) VALUES (%s, %s)", (match_id, team))
        inning_id = cursor.lastrowid
        
        # Insert deliveries data
        for over_data in inning_data['overs']:
            over_number = over_data['over']
            for ball_idx, delivery in enumerate(over_data['deliveries']):
                query = """
                INSERT INTO Deliveries (
                    inning_id, over_number, ball_number, batter, bowler, runs_batter, runs_extras, runs_total, 
                    extras_type, dismissal_kind, player_out
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    inning_id,
                    over_number,
                    ball_idx + 1,
                    delivery['batter'],
                    delivery['bowler'],
                    delivery['runs']['batter'],
                    delivery['runs'].get('extras', 0),
                    delivery['runs']['total'],
                    next(iter(delivery.get('extras', {}).keys()), None) if 'extras' in delivery else None,
                    delivery.get('wickets', [{}])[0].get('kind') if 'wickets' in delivery else None,
                    delivery.get('wickets', [{}])[0].get('player_out') if 'wickets' in delivery else None
                )
                cursor.execute(query, values)
        db.commit()

# Process all JSON files from the zip
zip_file_path = "tests_json.zip"  # Path to the zip file
extracted_folder = "extracted_files"

# Extract the zip file
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extracted_folder)

# Iterate over all JSON files
for filename in os.listdir(extracted_folder):
    if filename.endswith('.json'):
        filepath = os.path.join(extracted_folder, filename)
        with open(filepath, 'r') as file:
            try:
                match_data = json.load(file)
                unique_match_id = str(uuid.uuid4())  # Generate a unique match_id
                insert_match_info(match_data, unique_match_id)
                insert_players(match_data, unique_match_id)
                insert_innings_and_deliveries(match_data, unique_match_id)
                print(f"Processed {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

# Close database connection
cursor.close()
db.close()
