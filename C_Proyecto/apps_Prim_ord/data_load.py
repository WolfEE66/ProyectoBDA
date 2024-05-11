from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("Carga de datos en PostgreSQL") \
    .getOrCreate()

# Leer datos transformados desde S3 a DataFrames de Spark
df_clientes = spark.read.json("s3a://mi-bucket/output_1/clientes.json", header=True)
df_reservas = spark.read.csv("s3a://mi-bucket/output_1/reservas.csv", header=True, inferSchema=True)
df_habitaciones = spark.read.csv("s3a://mi-bucket/output_1/habitaciones.csv", header=True, inferSchema=True)
df_menus = spark.read.csv("s3a://mi-bucket/output_1/menus.csv", header=True, inferSchema=True)

# Escribir DataFrames en tablas PostgreSQL
df_clientes.write.format("jdbc").options(
    url="jdbc:postgresql://tu_host:9999/PrimOrd",
    driver="org.postgresql.Driver",
    dbtable="clientes",
    user="primOrd",
    password="bdaPrimOrd"
).mode("overwrite").save()


df_reservas.write.format("jdbc").options(
    url="jdbc:postgresql://tu_host:9999/PrimOrd",
    driver="org.postgresql.Driver",
    dbtable="reservas",
    user="primOrd",
    password="bdaPrimOrd"
).mode("overwrite").save()

df_habitaciones.write.format("jdbc").options(
    url="jdbc:postgresql://tu_host:9999/PrimOrd",
    driver="org.postgresql.Driver",
    dbtable="habitaciones",
    user="primOrd",
    password="bdaPrimOrd"
).mode("overwrite").save()

df_menus.write.format("jdbc").options(
    url="jdbc:postgresql://tu_host:9999/PrimOrd",
    driver="org.postgresql.Driver",
    dbtable="menus",
    user="primOrd",
    password="bdaPrimOrd"
).mode("overwrite").save()

# Detener la sesión de Spark
spark.stop()
