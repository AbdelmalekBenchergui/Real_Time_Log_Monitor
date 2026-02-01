from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, window, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("LogsConsumer")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0"
        ])
    )
    .getOrCreate()
)


schema = StructType([
    StructField("id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("timestamp_human", StringType(), True),
    StructField("api", StringType(), True),
    StructField("method", StringType(), True),
    StructField("response_time", IntegerType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("log_level", StringType(), True),
    StructField("response_code", IntegerType(), True),
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs") \
    .option("startingOffsets", "latest") \
    .load()


df_json = df.selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), schema).alias("data")) \
            .select("data.*")


df_json = df_json.withColumn(
    "alert_level",
    when(col("status_code") >= 500, "server errors")        # server errors
    .when((col("status_code") >= 400) & (col("status_code") < 500), "client errors")  # client errors
    .otherwise("NORMAL")                            
)


es_options_all_logs = {
    "es.nodes": "localhost",
    "es.port": "9200",
    "es.resource": "all_logs",
    "es.mapping.id": "id"
}

def write_all_logs(df):
    df.write \
      .format("org.elasticsearch.spark.sql") \
      .options(**es_options_all_logs) \
      .mode("append") \
      .save()

query_all_logs = df_json.writeStream \
    .foreachBatch(lambda df, epochId: write_all_logs(df)) \
    .outputMode("append") \
    .start()


alerts = df_json.filter(col("alert_level") == "server errors")

es_options_alerts = {
    "es.nodes": "localhost",
    "es.port": "9200",
    "es.resource": "alerts",
    "es.mapping.id": "id"
}

def write_alerts(df):
    df.write \
      .format("org.elasticsearch.spark.sql") \
      .options(**es_options_alerts) \
      .mode("append") \
      .save()

query_alerts = alerts.writeStream \
    .foreachBatch(lambda df, epochId: write_alerts(df)) \
    .outputMode("append") \
    .start()



query_all_logs.awaitTermination()
query_alerts.awaitTermination()
