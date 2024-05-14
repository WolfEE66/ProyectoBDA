from pyspark.sql import SparkSession
import boto3

# Definir las credenciales de AWS
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

# Nombre del bucket de S3
bucket_name = 'mi-bucket'

# Crear un cliente de S3 para crear el bucket
s3_client = boto3.client('s3',
                         endpoint_url='http://localstack:4566',
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
    .appName("SPARK S3") \
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

# Cargar el archivo restaurantes.json a un DataFrame de Spark
restaurantes_df = spark.read.json("s3a://mi-bucket/restaurantes.json")

# Cargar el archivo habitaciones.csv a un DataFrame de Spark
habitaciones_df = spark.read.csv("s3a://mi-bucket/habitaciones.csv", header=True, inferSchema=True)

# Escribir el DataFrame restaurantes_df en formato JSON en S3
restaurantes_df.write.mode('overwrite').json("s3a://mi-bucket/output/restaurantes")

# Escribir el DataFrame habitaciones_df en formato CSV en S3
habitaciones_df.write.mode('overwrite').csv("s3a://mi-bucket/output/habitaciones")

# Detener la sesión de Spark
spark.stop()
