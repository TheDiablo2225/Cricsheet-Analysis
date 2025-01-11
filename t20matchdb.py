import os
import json
import mysql.connector
import uuid

# Connect to MySQL database
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mysql",
    database="T20CricketData"
)
cursor = db.cursor()

# Function to insert match information
def insert_match_info(data, match_id):
    info = data['info']
    outcome = info.get('outcome', {})
    
    query = """
    INSERT INTO MatchInfo (
        match_id, city, venue, match_type, season, event_name, event_group, match_number, 
        team1, team2, toss_winner, toss_decision, winner, win_by_runs, win_by_wickets, player_of_match
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        match_id,
        info.get('city'),
        info.get('venue'),
        info.get('match_type'),
        info.get('season'),
        info.get('event', {}).get('name'),
        info.get('event', {}).get('group'),
        info.get('event', {}).get('match_number'),
        info['teams'][0],
        info['teams'][1],
        info['toss']['winner'],
        info['toss']['decision'],
        outcome.get('winner'),
        outcome.get('by', {}).get('runs'),
        outcome.get('by', {}).get('wickets'),
        next(iter(info.get('player_of_match', [])), None)
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
                    inning_id, over_number, ball_number, batter, bowler, non_striker, 
                    runs_batter, runs_extras, runs_total, extras_type, dismissal_kind, player_out
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    inning_id,
                    over_number,
                    ball_idx + 1,
                    delivery['batter'],
                    delivery['bowler'],
                    delivery.get('non_striker'),
                    delivery['runs']['batter'],
                    delivery['runs'].get('extras', 0),
                    delivery['runs']['total'],
                    next(iter(delivery.get('extras', {}).keys()), None) if 'extras' in delivery else None,
                    delivery.get('wickets', [{}])[0].get('kind') if 'wickets' in delivery else None,
                    delivery.get('wickets', [{}])[0].get('player_out') if 'wickets' in delivery else None
                )
                cursor.execute(query, values)
        db.commit()

# Process JSON files
data_folder = "C:\\Users\\prasi\\Cricsheet\\t20s_json"

 # Replace with your folder path

for filename in os.listdir(data_folder):
    if filename.endswith('.json'):
        file_path = os.path.join(data_folder, filename)
        with open(file_path, 'r') as file:
            try:
                match_data = json.load(file)
                match_id = str(uuid.uuid4())  # Unique match ID
                insert_match_info(match_data, match_id)
                insert_players(match_data, match_id)
                insert_innings_and_deliveries(match_data, match_id)
                print(f"Processed {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

# Close database connection
cursor.close()
db.close()
