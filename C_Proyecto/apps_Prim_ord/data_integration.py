from pyspark.sql import SparkSession
import boto3
import os

# Definir las credenciales de AWS
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

# Nombre del bucket de S3
bucket_name = 'bucket'

# Crear un cliente de S3 para crear el bucket
s3_client = boto3.client('s3',
                         endpoint_url='http://c_proyecto-localstack-1:4566',
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)

try:
    # Crear el bucket en S3
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' creado exitosamente.")
except Exception as e:
    print(f"Error al crear el bucket '{bucket_name}': {e}")

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

# Función para subir archivos a S3
def upload_to_s3(local_path, s3_path):
    df = spark.read.json(local_path) if local_path.endswith('.json') else spark.read.csv(local_path, header=True)
    df.write \
        .option('fs.s3a.committer.name', 'partitioned') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
        .mode('overwrite') \
        .json(s3_path) if local_path.endswith('.json') else df.write.csv(s3_path, header=True)

try:
    # Subir archivos directamente a S3
    df1 = spark.read.json("restaurantes.json")
    df2 = spark.read.option("header", True).csv("habitaciones.csv")

    df1.write \
        .option('fs.s3a.committer.name', 'partitioned') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
        .mode('overwrite') \
        .json("s3a://bucket/output/restaurantes")

    df2.write \
        .option('fs.s3a.committer.name', 'partitioned') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
        .mode('overwrite') \
        .csv("s3a://bucket/output/habitaciones")

    # Subir archivos generados por los consumidores a S3
    consumers_output_paths = [
        ("output/mongoConsumer", "s3a://bucket/output/mongoConsumer"),
        ("output/neo4jConsumer", "s3a://bucket/output/neo4jConsumer"),
        ("output/postgresConsumer/empleados", "s3a://bucket/output/postgresConsumer/empleados"),
        ("output/postgresConsumer/hoteles", "s3a://bucket/output/postgresConsumer/hoteles"),
        ("reservas_json", "s3a://bucket/output/reservasConsumer")
    ]

    for local_path, s3_path in consumers_output_paths:
        if os.path.exists(local_path):
            upload_to_s3(local_path, s3_path)
        else:
            print(f"Path {local_path} does not exist")

except Exception as e:
    print("Error al subir archivos a S3")
    print(e)

# Detener la sesión de Spark
spark.stop()
