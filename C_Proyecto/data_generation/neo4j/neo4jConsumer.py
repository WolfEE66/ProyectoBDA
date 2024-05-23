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


# Definir las credenciales de AWS
aws_access_key_id = 'test'
aws_secret_access_key = 'test'


# Crear sesión de Spark

spark = SparkSession.builder \
    .appName("neo4jConsumer") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://c_proyecto-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars", "/opt/spark-data_generation/postgresql-42.2.22.jar")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Leer datos desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "menus_stream") \
    .load()

# Convertir los datos de Kafka de JSON a DataFrame
menus_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Escribir los datos en el sistema de archivos
output_path = "/opt/spark-data_generation/neo4j/menus"

query = menus_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", "/opt/spark-data_generation/neo4j/menus") \
    .start()

query.awaitTermination()
