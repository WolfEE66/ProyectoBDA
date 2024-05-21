from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, explode, split, month, sum
from pyspark.sql.types import IntegerType

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
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
    .master("local[*]") \
    .getOrCreate()

# Cargar los DataFrames desde Postgres
df_preferencias_comida = spark.read.jdbc(url=db_url, table="preferencias_comida", properties=db_properties)
df_rendimiento_restaurante = spark.read.jdbc(url=db_url, table="rendimiento_restaurante", properties=db_properties)
df_patrones_reserva = spark.read.jdbc(url=db_url, table="patrones_reserva", properties=db_properties)
df_gestion_empleados_ocupacion = spark.read.jdbc(url=db_url, table="gestion_empleados_ocupacion", properties=db_properties)
df_reservas = spark.read.jdbc(url=db_url, table="reservas", properties=db_properties)
df_precios_habitaciones = spark.read.jdbc(url=db_url, table="precios_habitaciones", properties=db_properties)
df_menus = spark.read.jdbc(url=db_url, table="menus", properties=db_properties)
df_platos = spark.read.jdbc(url=db_url, table="platos", properties=db_properties)

# 5.2.1 Análisis de las preferencias de los clientes
# ¿Cuáles son las preferencias alimenticias más comunes entre los clientes?
preferencias_comunes = df_preferencias_comida.groupBy("Preferencias_Comida").count().orderBy("count", ascending=False)
preferencias_comunes.show()

# 5.2.2 Análisis del rendimiento del restaurante
# ¿Qué restaurante tiene el precio medio de menú más alto?
restaurante_precio_mas_alto = df_rendimiento_restaurante.select("id_restaurante", "precio_medio").orderBy("precio_medio", ascending=False).first()
print("Restaurante con el precio medio más alto: ", restaurante_precio_mas_alto)

# ¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?
tendencias_disponibilidad = df_rendimiento_restaurante.groupBy("id_restaurante").agg(avg("platos_disponibles")).orderBy("avg(platos_disponibles)", ascending=False)
tendencias_disponibilidad.show()

# 5.2.3 Patrones de reserva
# ¿Cuál es la duración media de la estancia de los clientes de un hotel?
duracion_media_estancia = df_patrones_reserva.agg(avg("duracion_estancia"))
duracion_media_estancia.show()

# ¿Existen periodos de máxima ocupación en función de las fechas de reserva?
periodos_maxima_ocupacion = df_patrones_reserva.groupBy("Fecha_Llegada").agg(count("reservas").alias("total_reservas")).orderBy("total_reservas", ascending=False)
periodos_maxima_ocupacion.show()

# 5.2.4 Gestión de empleados
# ¿Cuántos empleados tiene de media cada hotel?
media_empleados_hotel = df_gestion_empleados_ocupacion.groupBy("ID_Hotel").agg(avg("numero_empleados").alias("media_empleados"))
media_empleados_hotel.show()

# 5.2.5 Ocupación e ingresos del hotel
# ¿Cuál es el índice de ocupación de cada hotel y varía según la categoría de habitación?
df_ocupacion_hotel_categoria = df_gestion_empleados_ocupacion.withColumn("indice_ocupacion", (col("numero_reservas") / col("capacidad_habitacion")) * 100)
indice_ocupacion = df_ocupacion_hotel_categoria.groupBy("ID_Hotel", "Tipo_Habitacion").agg(avg("indice_ocupacion").alias("media_indice_ocupacion"))
indice_ocupacion.show()

# ¿Podemos estimar los ingresos generados por cada hotel basándonos en los precios de las habitaciones y los índices de ocupación?
df_ingresos = df_ocupacion_hotel_categoria.join(df_precios_habitaciones, ["ID_Hotel", "Tipo_Habitacion"])
df_ingresos = df_ingresos.withColumn("ingresos_estimados", col("media_indice_ocupacion") * col("precio_habitacion"))
ingresos_por_hotel = df_ingresos.groupBy("ID_Hotel").agg(sum("ingresos_estimados").alias("ingresos_totales"))
ingresos_por_hotel.show()

# 5.2.6 Análisis de menús
# ¿Qué platos son los más y los menos populares entre los restaurantes?
popularidad_platos = df_platos.groupBy("nombre").count().orderBy("count", ascending=False)
popularidad_platos.show()

# ¿Hay ingredientes o alérgenos comunes que aparezcan con frecuencia en los platos?
df_ingredientes = df_platos.select(explode(split(col("ingredientes"), ",")).alias("ingrediente"))
ingredientes_comunes = df_ingredientes.groupBy("ingrediente").count().orderBy("count", ascending=False)
ingredientes_comunes.show()

df_alergenos = df_platos.select(explode(split(col("alergenos"), ",")).alias("alergeno"))
alergenos_comunes = df_alergenos.groupBy("alergeno").count().orderBy("count", ascending=False)
alergenos_comunes.show()

# 5.2.7 Comportamiento de los clientes
# ¿Existen pautas en las preferencias de los clientes en función de la época del año?
df_reservas_with_month = df_reservas.withColumn("Mes_Llegada", month(col("Fecha_Llegada")))
pautas_preferencias = df_reservas_with_month.groupBy("Mes_Llegada", "Preferencias_Comida").count().orderBy("count", ascending=False)
pautas_preferencias.show()

# ¿Los clientes con preferencias dietéticas específicas tienden a reservar en restaurantes concretos?
preferencias_por_restaurante = df_reservas.groupBy("ID_Restaurante", "Preferencias_Comida").count().orderBy("count", ascending=False)
preferencias_por_restaurante.show()

# 5.2.8 Garantía de calidad
# ¿Existen discrepancias entre la disponibilidad de platos comunicada y las reservas reales realizadas?
# Aquí necesitaríamos los datos de reservas de platos y la disponibilidad comunicada para detectar discrepancias.

# 5.2.9 Análisis de mercado
# ¿Cómo se comparan los precios de las habitaciones de los distintos hoteles y existen valores atípicos?
precios_comparacion = df_precios_habitaciones.groupBy("ID_Hotel").agg(avg("precio_habitacion").alias("precio_medio"))
precios_comparacion.show()

# Detener la sesión de Spark
spark.stop()