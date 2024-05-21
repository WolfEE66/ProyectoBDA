from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("PostgresReader") \
    .config("spark.jars", "/path/to/postgresql-42.2.19.jar") \
    .getOrCreate()

# Configuración de la base de datos
db_config = {
    "url": "jdbc:postgresql://localhost:9999/primord",
    "user": "postgres",
    "password": "bda",
    "driver": "org.postgresql.Driver"
}

# Leer datos desde Postgres
empleados_df = spark.read \
    .format("jdbc") \
    .option("url", db_config["url"]) \
    .option("dbtable", "empleados") \
    .option("user", db_config["user"]) \
    .option("password", db_config["password"]) \
    .option("driver", db_config["driver"]) \
    .load()

hoteles_df = spark.read \
    .format("jdbc") \
    .option("url", db_config["url"]) \
    .option("dbtable", "hoteles") \
    .option("user", db_config["user"]) \
    .option("password", db_config["password"]) \
    .option("driver", db_config["driver"]) \
    .load()

# Escribir los DataFrames en el sistema de archivos
output_path_empleados = "/opt/spark-output/empleados"
output_path_hoteles = "/opt/spark-output/hoteles"

empleados_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path_empleados)

hoteles_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path_hoteles)

spark.stop()
