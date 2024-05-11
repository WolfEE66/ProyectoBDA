from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("Análisis de datos con Spark y PostgreSQL") \
    .getOrCreate()

# Configurar conexión a PostgreSQL
jdbc_url = "jdbc:postgresql://tu_host:9999/PrimOrd"
connection_properties = {
    "user": "primOrd",
    "password": "bdaPrimOrd",
    "driver": "org.postgresql.Driver"
}

# Leer datos desde PostgreSQL
df_clientes = spark.read.jdbc(url=jdbc_url, table="clientes", properties=connection_properties)
df_reservas = spark.read.jdbc(url=jdbc_url, table="reservas", properties=connection_properties)
df_habitaciones = spark.read.jdbc(url=jdbc_url, table="habitaciones", properties=connection_properties)
df_menus = spark.read.jdbc(url=jdbc_url, table="menus", properties=connection_properties)




# Análisis de datos con Spark SQL

df_clientes.createOrReplaceTempView("clientes")
clientes_con_reseservas_y_sus_preferencias = spark.sql("SELECT id_cliente,preferencias_alimenticias FROM clientes")

clientes_con_reseservas_y_sus_preferencias.show()

df_reservas.createOrReplaceTempView("reservas")
habitaciones_reservadas = spark.sql("SELECT * FROM reservas W")

habitaciones_reservadas.show()

df_habitaciones.createOrReplaceTempView("habitaciones")
= spark.sql("SELECT ")

habitaciones_reservadas.show()

df_menus.createOrReplaceTempView("menus")
 = spark.sql("SELECT ")
 
habitaciones_reservadas.show()




# Detener la sesión de Spark
spark.stop()
