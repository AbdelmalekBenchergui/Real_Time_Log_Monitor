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

# ----------------------------
# 4️⃣ Convertir la valeur Kafka en JSON structuré
# ----------------------------
df_json = df.selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), schema).alias("data")) \
            .select("data.*")

# ----------------------------
# 5️⃣ Ajouter la colonne alert_level
# ----------------------------
df_json = df_json.withColumn(
    "alert_level",
    when(col("status_code") >= 500, "server errors")        # server errors
    .when((col("status_code") >= 400) & (col("status_code") < 500), "client errors")  # client errors
    .otherwise("NORMAL")                            # others
)

# ----------------------------
# 6️⃣ Écriture dans Elasticsearch - tous les logs
# ----------------------------
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

# ----------------------------
# 7️⃣ Filtrer uniquement les alertes (server errors)
# ----------------------------
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

# ----------------------------
# 8️⃣ Calculs / métriques supplémentaires (optionnel)
# ----------------------------
requests_per_api = df_json.groupBy("api").count()
status_dist = df_json.groupBy("status_code").count().orderBy(col("count").desc())
client_errors = df_json.filter((col("status_code") >= 400) & (col("status_code") < 500)).groupBy("api").count()
server_errors = df_json.filter(col("status_code") >= 500).groupBy("api").count()
avg_response = df_json.groupBy("api").agg(avg("response_time").alias("avg_response_time"))
top_ips = df_json.groupBy("ip_address").count().orderBy(col("count").desc()).limit(5)

# ----------------------------
# 9️⃣ SQL Database writing function (optional)
# ----------------------------
sql_url = "jdbc:sqlserver://<AZURE_SQL_SERVER>:1433;databaseName=<DB_NAME>"
sql_properties = {
    "user": "<USER>",
    "password": "<PASSWORD>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def write_to_sql(df, table_name):
    df.write \
      .format("jdbc") \
      .option("url", sql_url) \
      .option("dbtable", table_name) \
      .option("user", sql_properties["user"]) \
      .option("password", sql_properties["password"]) \
      .option("driver", sql_properties["driver"]) \
      .mode("append") \
      .save()

# ----------------------------
# 10️⃣ Await streaming termination
# ----------------------------
query_all_logs.awaitTermination()
query_alerts.awaitTermination()
