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
            CREATE TABLE IF NOT EXISTS teams (
                id INT AUTO_INCREMENT PRIMARY KEY,
                match_id VARCHAR(50),
                inning INT,
                batting_team VARCHAR(100),
                bowling_team VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        print("MySQL database and teams table setup completed successfully")
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
                record.get('bowling_team')
            ))

        insert_query = """
            INSERT INTO teams 
            (match_id, inning, batting_team, bowling_team)
            VALUES (%s, %s, %s, %s)
        """

        cursor.executemany(insert_query, values)
        conn.commit()

        print(f"Batch: {len(values)} records inserted into MySQL (teams)")

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
        'teams_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='mysql_teams_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000  
    )

    print("Starting Kafka consumer for teams_topic...")

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

        print("All records consumed from teams_topic and inserted. Exiting.")

    except Exception as e:
        print(f"Error in consumer: {e}")
        if batch:
            insert_to_mysql(batch)

if __name__ == "__main__":
    main()
