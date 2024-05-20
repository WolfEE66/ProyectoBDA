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
db_url = "jdbc:postgresql://localhost:5432/       "
db_properties = {
    "user": "    ",
    "password": "     ",
    "driver": "org.postgresql.Driver"
}

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("HotelAnalysis") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4518") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.2.18") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar:/opt/spark/jars/postgresql-42.2.18.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar:/opt/spark/jars/postgresql-42.2.18.jar") \
    .master("local[*]") \
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

# 3. Patrones de Reserva (Patrones_Reserva)
df_duracion_estancia = df_reservas.withColumn("duracion_estancia", col("Fecha_Salida").cast("date").cast("long") - col("Fecha_Llegada").cast("date").cast("long"))
df_ocupacion_periodos = df_reservas.groupBy("Fecha_Llegada").count().withColumnRenamed("count", "reservas")

# 4. Gestión de Empleados y Ocupación (Empleados_Ocupacion)
df_empleados_hotel = df_empleados.groupBy("ID_Hotel").count().withColumnRenamed("count", "numero_empleados")
df_ocupacion_hotel_categoria = df_reservas.groupBy("ID_Hotel", "Tipo_Habitacion").count().withColumnRenamed("count", "numero_reservas")

# Guardar los DataFrames en Postgres

# 1. Preferencias Alimenticias
df_preferencias_comida.write.jdbc(url=db_url, table="preferencias_comida", mode="overwrite", properties=db_properties)

# 2. Rendimiento del Restaurante
df_precio_medio_menu.write.jdbc(url=db_url, table="rendimiento_restaurante_precio_medio", mode="overwrite", properties=db_properties)
df_disponibilidad_platos.write.jdbc(url=db_url, table="rendimiento_restaurante_disponibilidad_platos", mode="overwrite", properties=db_properties)

# 3. Patrones de Reserva
df_duracion_estancia.write.jdbc(url=db_url, table="patrones_reserva_duracion", mode="overwrite", properties=db_properties)
df_ocupacion_periodos.write.jdbc(url=db_url, table="patrones_reserva_ocupacion_periodos", mode="overwrite", properties=db_properties)

# 4. Gestión de Empleados y Ocupación
df_empleados_hotel.write.jdbc(url=db_url, table="empleados_hotel", mode="overwrite", properties=db_properties)
df_ocupacion_hotel_categoria.write.jdbc(url=db_url, table="ocupacion_hotel_categoria", mode="overwrite", properties=db_properties)

# Detener la sesión de Spark
spark.stop()
