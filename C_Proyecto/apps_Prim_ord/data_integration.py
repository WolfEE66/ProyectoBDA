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
                         endpoint_url='http://localstack:4566',
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)

try:
    # Crear el bucket en S3 si no existe
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' creado exitosamente.")
except Exception as e:
    print(f"Error al crear el bucket '{bucket_name}': {e}")

# Subir los archivos de los consumidores a S3
files_to_upload = ['sparkConsumer.py', 'mongoProducer.py', 'neo4jProducer.py']

for file_name in files_to_upload:
    try:
        s3_client.upload_file(file_name, bucket_name, file_name)
        print(f"Archivo '{file_name}' subido a S3 exitosamente.")
    except Exception as e:
        print(f"Error al subir el archivo '{file_name}' a S3: {e}")

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("SPARK S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
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

try:
    # Leer datos desde S3 y procesarlos
    df1 = spark.read.option("delimiter", ",").option("header", True).json("s3a://bucket/restaurantes.json")
    df2 = spark.read.option("delimiter", ",").option("header", True).csv("s3a://bucket/habitaciones.csv")
    
    df1 \
    .write \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .json(path='s3a://bucket/output/restaurantes', sep=',')
    
    df2 \
    .write \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .csv(path='s3a://bucket/output/habitaciones', sep=',')
    
except Exception as e:
    print("Error al leer datos desde S3")
    print(e)

# Detener la sesión de Spark
spark.stop()
