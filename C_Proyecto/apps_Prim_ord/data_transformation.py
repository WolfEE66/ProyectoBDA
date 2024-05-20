from pyspark.sql import SparkSession
import boto3

# Definir las credenciales de AWS
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

# Nombre del bucket de S3
bucket_name = 'bucket'

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("HotelAnalysis") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4518") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.1.jar") \
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

# Pregunta 5.2.1: Análisis de las preferencias de los clientes
# Necesitamos solo la columna "Preferencias_Comida" del DataFrame de reservas
df_preferencias_comida = df_reservas.select("Preferencias_Comida")

# Pregunta 5.2.2: Análisis del rendimiento del restaurante
# Para el precio medio del menú, necesitamos solo "ID_Restaurante" y "precio"
df_precio_medio_menu = df_menus.select("id_restaurante", "precio")

# Para la disponibilidad de platos, necesitamos "id_restaurante" y "disponibilidad"
df_disponibilidad_platos = df_menus.select("id_restaurante", "disponibilidad")

# Pregunta 5.2.3: Patrones de reserva
# Para la duración media de la estancia, necesitamos "Fecha_Llegada" y "Fecha_Salida"
df_duracion_estancia = df_reservas.select("Fecha_Llegada", "Fecha_Salida")

# Para los periodos de máxima ocupación, necesitamos "Fecha_Llegada"
df_ocupacion_periodos = df_reservas.select("Fecha_Llegada")

# Pregunta 5.2.4: Gestión de empleados
# Para el número de empleados por hotel, necesitamos "ID_Hotel" del DataFrame de empleados
df_empleados_hotel = df_empleados.select("ID_Hotel")

# Pregunta 5.2.5: Ocupación e ingresos del hotel
# Para el índice de ocupación, necesitamos "ID_Hotel", "Tipo_Habitacion" del DataFrame de reservas
df_ocupacion_hotel_categoria = df_reservas.select("ID_Hotel", "Tipo_Habitacion")

# Para los ingresos estimados, necesitamos "ID_Hotel", "ID_Habitacion" del DataFrame de reservas y "Precio_Habitacion" del DataFrame de habitaciones
df_ingresos_hotel = df_reservas.select("ID_Hotel", "ID_Habitacion")
df_precios_habitaciones = df_habitaciones.select("ID_Habitacion", "Precio_Habitacion")

# Pregunta 5.2.6: Análisis de menús
# Para la popularidad de los platos, necesitamos "nombre"
df_popularidad_platos = df_platos.select("nombre")

# Para los ingredientes comunes, necesitamos "ingredientes" y "alergenos"
df_ingredientes_comunes = df_platos.select("ingredientes", "alergenos")

# Pregunta 5.2.7: Comportamiento de los clientes
# Para las preferencias por época del año, necesitamos "Fecha_Llegada" y "Preferencias_Comida"
df_preferencias_epoca = df_reservas.select("Fecha_Llegada", "Preferencias_Comida")

# Para las preferencias dietéticas específicas, necesitamos "ID_Restaurante" y "Preferencias_Comida"
df_preferencias_dietas_restaurantes = df_reservas.select("ID_Restaurante", "Preferencias_Comida")

# Pregunta 5.2.8: Garantía de calidad
# Para las discrepancias entre disponibilidad y reservas, necesitamos "ID_Restaurante", "nombre", "disponibilidad" del DataFrame de menus y "ID_Restaurante", "nombre" del DataFrame de reservas
df_discrepancias_platos_menus = df_menus.select("id_restaurante", "disponibilidad")
df_discrepancias_platos_reservas = df_reservas.select("ID_Restaurante", "Nombre_Plato")

# Pregunta 5.2.9: Análisis de mercado
# Para los precios de habitaciones, necesitamos "ID_Hotel" y "Precio_Habitacion"
df_precios_habitaciones_mercado = df_habitaciones.select("ID_Hotel", "Precio_Habitacion")

# Mostrar los DataFrames resultantes
print("Preferencias de Comida:")
df_preferencias_comida.show()

print("Precio Medio del Menú:")
df_precio_medio_menu.show()

print("Disponibilidad de Platos:")
df_disponibilidad_platos.show()

print("Duración de la Estancia:")
df_duracion_estancia.show()

print("Ocupación por Periodos:")
df_ocupacion_periodos.show()

print("Empleados por Hotel:")
df_empleados_hotel.show()

print("Ocupación por Categoría:")
df_ocupacion_hotel_categoria.show()

print("Ingresos Estimados por Hotel:")
df_ingresos_hotel.show()

print("Popularidad de Platos:")
df_popularidad_platos.show()

print("Ingredientes Comunes:")
df_ingredientes_comunes.show()

print("Preferencias por Época del Año:")
df_preferencias_epoca.show()

print("Preferencias Dietéticas Específicas:")
df_preferencias_dietas_restaurantes.show()

print("Discrepancias entre Disponibilidad y Reservas:")
df_discrepancias_platos_menus.show()
df_discrepancias_platos_reservas.show()

print("Precios de Habitaciones:")
df_precios_habitaciones_mercado.show()
