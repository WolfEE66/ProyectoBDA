from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType


# Definir las credenciales de AWS
aws_access_key_id = 'test'
aws_secret_access_key = 'test'


# Crear sesión de Spark

spark = SparkSession.builder \
    .appName("KafkaToFolder") \
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


# Definir el esquema para los datos de reserva
schema = StructType([
    StructField("ID_Cliente", StringType(), True),
    StructField("Fecha_Llegada", StringType(), True),
    StructField("Fecha_Salida", StringType(), True),
    StructField("Tipo_Habitacion", StringType(), True),
    StructField("Preferencias_Comida", StringType(), True),
    StructField("ID_Habitacion", StringType(), True),
    StructField("ID_Restaurante", StringType(), True)
])

# Rutas relativas
input_file_path = "/opt/spark-data_Prim_ord/text/reservas.txt"
output_folder = "/opt/spark-data_generation/txt/reservas_json"

# Leer el archivo de reservas
reservas_df = spark.read.text(input_file_path)

# Definir una expresión regular para extraer los campos de cada reserva
pattern = r"\*\*\* Reserva (\d+) \*\*\*\nID Cliente: (\d+)\nFecha Llegada: (\d{4}-\d{2}-\d{2})\nFecha Salida: (\d{4}-\d{2}-\d{2})\nTipo Habitacion: (\w+)\nPreferencias Comida: (\w+)\nId Habitacion: (\d+)\nID Restaurante: (\d+)"

# Extraer los campos utilizando la expresión regular y crear un DataFrame con el esquema definido
reservas_parsed_df = reservas_df.select(
    regexp_extract(col("value"), pattern, 1).alias("ID_Reserva"),
    regexp_extract(col("value"), pattern, 2).alias("ID_Cliente"),
    regexp_extract(col("value"), pattern, 3).alias("Fecha_Llegada"),
    regexp_extract(col("value"), pattern, 4).alias("Fecha_Salida"),
    regexp_extract(col("value"), pattern, 5).alias("Tipo_Habitacion"),
    regexp_extract(col("value"), pattern, 6).alias("Preferencias_Comida"),
    regexp_extract(col("value"), pattern, 7).alias("ID_Habitacion"),
    regexp_extract(col("value"), pattern, 8).alias("ID_Restaurante")
).filter("ID_Reserva != ''")

# Convertir los datos a formato JSON y guardarlos en una carpeta
query = reservas_parsed_df \
    .selectExpr("to_json(struct(*)) AS value") \
    .write \
    .mode("overwrite") \
    .json(output_folder)

# Detener la sesión de Spark
spark.stop()
