# IPLStreamAnalysis

**Starting Zookeeper:**
```zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties```

**Starting Kafka Broker:**
```kafka-server-start.sh $KAFKA_HOME/config/server.properties```

**Topics used in the Project:**
```kafka-topics.sh --create --topic deliveries_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1```

```kafka-topics.sh --create --topic wickets_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1```

```kafka-topics.sh --create --topic teams_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1```

**Start consumer script:**
```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 SparkConsumer1.py```

**Producer and Pseudo-Consumers:**
These can be started like regular python scripts, provided that the Kafka dependencies are running
