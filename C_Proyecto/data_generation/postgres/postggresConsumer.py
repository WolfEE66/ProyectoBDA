from pyspark.sql import SparkSession

# Definir las credenciales de AWS
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("PostgresReader") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://c_proyecto-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars", "/opt/spark-data_generation/postgresql-42.2.22.jar") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .master("spark://spark-master:7077") \
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
output_path_empleados = "/opt/spark-data_generation/postgres/empleados"
output_path_hoteles = "/opt/spark-data_generation/postgres/hoteles"

empleados_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path_empleados)

hoteles_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path_hoteles)

# Detener la sesión de Spark
spark.stop()
