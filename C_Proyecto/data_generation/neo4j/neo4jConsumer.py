from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType

# Definir esquema de los datos de menú y plato
schema_menu = StructType([
    StructField("id_menu", StringType(), True),
    StructField("precio", FloatType(), True),
    StructField("disponibilidad", BooleanType(), True),
    StructField("id_restaurante", StringType(), True)
])

schema_plato = StructType([
    StructField("platoID", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("ingredientes", StringType(), True),
    StructField("alergenos", StringType(), True)
])

schema = StructType([
    StructField("m", schema_menu, True),
    StructField("p", schema_plato, True)
])

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

# Leer datos desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "menus_stream") \
    .load()

# Convertir los datos de Kafka de JSON a DataFrame
menus_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Escribir los datos en el sistema de archivos
output_path = "/opt/spark-output/menus"

query = menus_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", "/opt/spark-output/checkpoints/menus") \
    .start()

query.awaitTermination()
