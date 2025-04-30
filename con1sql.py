from kafka import KafkaConsumer
import json
import pymysql
from pymysql import OperationalError as Error

mysql_config = {
    "host": "localhost",
    "user": "root",
    "password": "", # removed it
    "database": "ipl_data"
}

def setup_mysql():
    conn = None
    try:
        conn = pymysql.connect(
            host=mysql_config["host"],
            user=mysql_config["user"],
            password=mysql_config["password"]
        )
        cursor = conn.cursor()

        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_config['database']}")
        cursor.execute(f"USE {mysql_config['database']}")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS deliveries (
                id INT AUTO_INCREMENT PRIMARY KEY,
                match_id VARCHAR(50),
                inning INT,
                batting_team VARCHAR(100),
                bowling_team VARCHAR(100),
                over_num INT,
                ball INT,
                batter VARCHAR(100),
                bowler VARCHAR(100),
                non_striker VARCHAR(100),
                batsman_runs INT,
                extra_runs INT,
                total_runs INT,
                extras_type VARCHAR(50),
                is_wicket INT,
                player_dismissed VARCHAR(100),
                dismissal_kind VARCHAR(50),
                fielder VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        print("MySQL database and table setup completed successfully")
        return True

    except Error as e:
        print(f"Error setting up MySQL: {e}")
        return False

    finally:
        if conn and conn.open:
            cursor.close()
            conn.close()

def insert_to_mysql(records):
    if not records:
        print("No data to insert in this batch")
        return

    conn = None
    try:
        conn = pymysql.connect(**mysql_config)
        cursor = conn.cursor()

        values = []
        for record in records:
            values.append((
                record.get('match_id'),
                record.get('inning'),
                record.get('batting_team'),
                record.get('bowling_team'),
                record.get('over'),
                record.get('ball'),
                record.get('batter'),
                record.get('bowler'),
                record.get('non_striker'),
                record.get('batsman_runs'),
                record.get('extra_runs'),
                record.get('total_runs'),
                record.get('extras_type'),
                record.get('is_wicket'),
                record.get('player_dismissed'),
                record.get('dismissal_kind'),
                record.get('fielder')
            ))

        insert_query = """
            INSERT INTO deliveries 
            (match_id, inning, batting_team, bowling_team, over_num, ball, 
            batter, bowler, non_striker, batsman_runs, extra_runs, total_runs, 
            extras_type, is_wicket, player_dismissed, dismissal_kind, fielder)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_query, values)
        conn.commit()

        print(f"Batch: {len(values)} records inserted into MySQL")

    except Error as e:
        print(f"Error inserting into MySQL: {e}")

    finally:
        if conn and conn.open:
            cursor.close()
            conn.close()

def main():
    if not setup_mysql():
        print("Failed to set up MySQL. Please check your configuration.")
        exit(1)

    consumer = KafkaConsumer(
        'deliveries_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='mysql_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000  
    )

    print("Starting Kafka consumer...")

    batch_size = 1000
    batch = []

    try:
        for message in consumer:
            record = message.value
            batch.append(record)

            if len(batch) >= batch_size:
                insert_to_mysql(batch)
                batch = []

        if batch:
            insert_to_mysql(batch)

        print("All records consumed and inserted. Exiting.")

    except Exception as e:
        print(f"Error in consumer: {e}")
        if batch:
            insert_to_mysql(batch)

if __name__ == "__main__":
    main()
