"""
spark_streaming_consumer.py
Consumidor con Spark Structured Streaming: lee las lecturas de sensores del topic
"sensor_data" de Kafka y calcula la temperatura y la humedad promedio por sensor
en ventanas de tiempo de 1 minuto.

Ejecución (el productor debe estar corriendo antes):
  spark-submit --driver-memory 512m \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
    spark_streaming_consumer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

# Sesión de Spark. Se usan 4 particiones en los shuffles: el valor por defecto (200)
# es excesivo para una máquina virtual pequeña.
spark = (
    SparkSession.builder
    .appName("KafkaSparkStreaming")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
# Nivel de log WARN para reducir los mensajes INFO
spark.sparkContext.setLogLevel("WARN")

# Esquema de los datos de entrada (el mismo JSON que envía el productor)
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType())
])

# Lector de streaming: se suscribe al topic de Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .load()
)

# El valor llega como bytes: se convierte a texto y se interpreta el JSON
parsed_df = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# Estadísticas por ventana de tiempo de 1 minuto y por sensor
windowed_stats = (
    parsed_df
    .groupBy(window(col("timestamp"), "1 minute"), "sensor_id")
    .agg({"temperature": "avg", "humidity": "avg"})
)

# Escribir los resultados en la consola cada 10 segundos
query = (
    windowed_stats.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
