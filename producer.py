import csv
import json
from kafka import KafkaProducer

CSV_FILE = 'ipl1819.csv'

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open(CSV_FILE, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        try:
            delivery_msg = {
                'match_id': row['match_id'],
                'inning': int(row['inning']),
                'batting_team': row['batting_team'],
                'bowling_team': row['bowling_team'],
                'over': int(row['over']),
                'ball': int(row['ball']),
                'batter': row['batter'],
                'bowler': row['bowler'],
                'non_striker': row['non_striker'],
                'batsman_runs': int(row['batsman_runs']),
                'extra_runs': int(row['extra_runs']),
                'total_runs': int(row['total_runs']),
                'extras_type': row.get('extras_type', ''),
                'is_wicket': int(row.get('is_wicket', '0')),
                'player_dismissed': row.get('player_dismissed', ''),
                'dismissal_kind': row.get('dismissal_kind', ''),
                'fielder': row.get('fielder', '')
            }
            
            producer.send('deliveries_topic', value=delivery_msg)
            
            team_msg = {
                'match_id': row['match_id'],
                'inning': int(row['inning']),
                'batting_team': row['batting_team'],
                'bowling_team': row['bowling_team']
            }
            producer.send('teams_topic', value=team_msg)
            
            if row.get('is_wicket') == '1':
                wicket_msg = {
                    'match_id': row['match_id'],
                    'bowler': row['bowler'],
                    'player_dismissed': row['player_dismissed'],
                    'dismissal_kind': row['dismissal_kind'],
                    'fielder': row.get('fielder', '')
                }
                producer.send('wickets_topic', value=wicket_msg)
        except Exception as e:
            print(f"Error processing row: {row}")
            print(f"Error: {e}")

print("All rows sent to Kafka!")
producer.flush()
producer.close()