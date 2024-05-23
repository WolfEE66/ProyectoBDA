from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType
import boto3

# Definir las credenciales de AWS
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

# Nombre del bucket de S3
bucket_name = 'bucket'

# Configuración de conexión a Postgres
db_url = "jdbc:postgresql://localhost:5432/"
db_properties = {
    "user": "postgres",
    "password": "bda",
    "driver": "org.postgresql.Driver"
}

# Crear una sesión de Spark

spark = SparkSession.builder \
    .appName("HotelAnalysis") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://c_proyecto-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars", "/opt/spark-apps_Prim_ord/postgresql-42.2.22.jar")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Cargar los DataFrames desde S3
df_restaurantes = spark.read.json("s3a://bucket/output/restaurantes")
df_habitaciones = spark.read.csv("s3a://bucket/output/habitaciones", header=True, inferSchema=True)
df_reservas = spark.read.json("s3a://bucket/output/reservasConsumer")
df_empleados = spark.read.csv("s3a://bucket/output/postgresConsumer/empleados", header=True, inferSchema=True)
df_hoteles = spark.read.csv("s3a://bucket/output/postgresConsumer/hoteles", header=True, inferSchema=True)
df_menus = spark.read.json("s3a://bucket/output/neo4jConsumer/menus")
df_platos = spark.read.json("s3a://bucket/output/neo4jConsumer/platos")

# Preparar los DataFrames para las preguntas

# 1. Preferencias Alimenticias (Preferencias_Comida)
df_preferencias_comida = df_reservas.select("Preferencias_Comida")

# 2. Rendimiento del Restaurante (Rendimiento_Restaurante)
df_precio_medio_menu = df_menus.groupBy("id_restaurante").avg("precio").withColumnRenamed("avg(precio)", "precio_medio")
df_disponibilidad_platos = df_menus.groupBy("id_restaurante").count().withColumnRenamed("count", "platos_disponibles")
df_rendimiento_restaurante = df_precio_medio_menu.join(df_disponibilidad_platos, on="id_restaurante")

# 3. Patrones de Reserva (Patrones_Reserva)
df_duracion_estancia = df_reservas.withColumn("duracion_estancia", col("Fecha_Salida").cast("date").cast("long") - col("Fecha_Llegada").cast("date").cast("long"))
df_ocupacion_periodos = df_reservas.groupBy("Fecha_Llegada").count().withColumnRenamed("count", "reservas")
df_patrones_reserva = df_duracion_estancia.join(df_ocupacion_periodos, on="Fecha_Llegada")

# 4. Gestión de Empleados y Ocupación (Empleados_Ocupacion)
df_empleados_hotel = df_empleados.groupBy("ID_Hotel").count().withColumnRenamed("count", "numero_empleados")
df_ocupacion_hotel_categoria = df_reservas.groupBy("ID_Hotel", "Tipo_Habitacion").count().withColumnRenamed("count", "numero_reservas")
df_gestion_empleados_ocupacion = df_empleados_hotel.join(df_ocupacion_hotel_categoria, on="ID_Hotel")

# Guardar los DataFrames en Postgres

# 1. Preferencias Alimenticias
df_preferencias_comida.write.jdbc(url=db_url, table="preferencias_comida", mode="overwrite", properties=db_properties)

# 2. Rendimiento del Restaurante
df_rendimiento_restaurante.write.jdbc(url=db_url, table="rendimiento_restaurante", mode="overwrite", properties=db_properties)

# 3. Patrones de Reserva
df_patrones_reserva.write.jdbc(url=db_url, table="patrones_reserva", mode="overwrite", properties=db_properties)

# 4. Gestión de Empleados y Ocupación
df_gestion_empleados_ocupacion.write.jdbc(url=db_url, table="gestion_empleados_ocupacion", mode="overwrite", properties=db_properties)

# Detener la sesión de Spark
spark.stop()