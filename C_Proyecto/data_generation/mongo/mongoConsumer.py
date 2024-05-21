from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Definir esquema de los datos de cliente
schema = StructType([
    StructField("id_cliente", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("direccion", StringType(), True),
    StructField("preferencias_alimenticias", StringType(), True)
])

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("mongoConsumer") \
    .getOrCreate()

# Leer datos desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clientes_stream") \
    .load()

# Convertir los datos de Kafka de JSON a DataFrame
clientes_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Escribir datos en el sistema de archivos
output_path = "/opt/spark-output/clientes"

query = clientes_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", "/opt/spark-output/checkpoints/clientes") \
    .start()

query.awaitTermination()
