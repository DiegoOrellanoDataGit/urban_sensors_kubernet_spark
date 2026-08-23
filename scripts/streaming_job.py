from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, IntegerType, DoubleType, LongType

# Spark local con el paquete de Kafka
spark = SparkSession.builder \
    .appName("UrbanSensorsStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()


# Definición del esquema de los mensajes
schema = StructType() \
    .add("sensor_id", IntegerType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("air_quality_index", IntegerType()) \
    .add("timestamp", LongType())

# Lectura desde Kafka (localhost gracias al port-forward)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "urban_sensors") \
    .load()

# Parseo del JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Agregación por ventana de 1 minuto y sensor_id
agg_df = json_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp").cast("timestamp"), "1 minute"),
        col("sensor_id")
    ).agg(
        avg("temperature").alias("avg_temp"),
        avg("air_quality_index").alias("avg_aqi")
    )

# Salida en consola
query = agg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()