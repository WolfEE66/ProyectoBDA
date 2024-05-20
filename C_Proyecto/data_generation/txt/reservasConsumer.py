from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaToFolder") \
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

# Leer el archivo de reservas
reservas_df = spark.read.text("reservas.txt")

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
output_folder = "reservas_json"

query = reservas_parsed_df \
    .selectExpr("to_json(struct(*)) AS value") \
    .write \
    .mode("overwrite") \
    .json(output_folder)

query.awaitTermination()

# Detener la sesión de Spark
spark.stop()
