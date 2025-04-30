from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import shutil
import time

start_time = time.time()

schema = StructType([
    StructField("match_id", StringType()),
    StructField("inning", IntegerType()),
    StructField("batting_team", StringType()),
    StructField("bowling_team", StringType()),
    StructField("over", IntegerType()),
    StructField("ball", IntegerType()),
    StructField("batter", StringType()),
    StructField("bowler", StringType()),
    StructField("non_striker", StringType()),
    StructField("batsman_runs", IntegerType()),
    StructField("extra_runs", IntegerType()),
    StructField("total_runs", IntegerType()),
    StructField("extras_type", StringType()),
    StructField("is_wicket", IntegerType()),
    StructField("player_dismissed", StringType()),
    StructField("dismissal_kind", StringType()),
    StructField("fielder", StringType())
])

output_path = "streaming_output"

if os.path.exists(output_path):
    shutil.rmtree(output_path)
os.makedirs(output_path)

spark = SparkSession.builder \
    .appName("IPL Deliveries Stream Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

last_batch_time = [time.time()]
processed_any_data = [False]
idle_threshold = 10  # seconds to wait 

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "deliveries_topic") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")
parsed_df.createOrReplaceTempView("deliveries")

result_df = spark.sql("""
    SELECT 
        batter, 
        SUM(CAST(total_runs AS INT)) as total_runs_scored
    FROM deliveries
    GROUP BY batter
    ORDER BY total_runs_scored DESC
""")

def process_batch(df, epoch_id):
    last_batch_time[0] = time.time()
    row_count = df.count()
    if row_count > 0:
        processed_any_data[0] = True
        
        output_file = f"{output_path}/batch_{epoch_id}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_file)
        
        print(f"Batch {epoch_id} processed and saved to {output_file}")
        df.show(truncate=False)
    else:
        print(f"Batch {epoch_id} had no data")

streaming_query = result_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .trigger(processingTime='2 seconds') \
    .start()

print("Streaming started. Monitoring for inactivity...")
try:
    while True:
        time.sleep(1)
        current_time = time.time()
        time_since_last_batch = current_time - last_batch_time[0]
        
        if processed_any_data[0] and time_since_last_batch > idle_threshold:
            print(f"No data received for {idle_threshold} seconds. Shutting down...")
            break
            
except KeyboardInterrupt:
    print("Interrupted by user")
finally:
    for query in spark.streams.active:
        query.stop()
    
    end_time = time.time()
    total_time = end_time - start_time - 10.0
    
    print(f"Processing completed!")
    print(f"Total execution time: {total_time:.2f} seconds")
    
    def merge_final_results():
        try:
            import glob
            batch_files = glob.glob(f"{output_path}/batch_*/part-*.csv")
            
            if not batch_files:
                print(f"No batch files found in {output_path}/batch_*. Cannot merge results.")
                return
                
            csv_schema = StructType([
                StructField("batter", StringType()),
                StructField("total_runs_scored", IntegerType())
            ])
            
            final_df = spark.read.option("header", "true").schema(csv_schema).csv(f"{output_path}/batch_*")
            final_output = f"{output_path}/final_batters_runs"
            final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(final_output)
            
            print(f"Final results saved to {final_output}")
            print("Top batters by runs scored:")
            final_df.show(10, truncate=False)
            
        except Exception as e:
            print(f"Error merging results: {str(e)}")
    
    merge_final_results()
    spark.stop()