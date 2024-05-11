from pyspark.sql import SparkSession
import boto3

# Definir las credenciales de AWS
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

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

# Crear un cliente de S3
s3_client = boto3.client('s3', endpoint_url='http://localstack:4566',
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)



# Nombre del bucket de S3
bucket_name = 'mi-bucket'

# Crear el bucket en S3
try:
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' creado exitosamente.")
except Exception as e:
    print(f"Error al crear el bucket '{bucket_name}': {e}")

# Subir los archivos CSV a S3

try:
    df1 = spark.read.option("delimiter", ",").option("header", True).json("/opt//opt/spark-data_Prim_ord/clientes.json")
    df2 = spark.read.option("delimiter", ",").option("header", True).csv("/opt//opt/spark-data_Prim_ord/reservas.csv")
    df3 = spark.read.option("delimiter", ",").option("header", True).csv("/opt//opt/spark-data_Prim_ord/habitaciones.csv")
    df4 = spark.read.option("delimiter", ",").option("header", True).csv("/opt//opt/spark-data_Prim_ord/menus.csv")
    
    df1 \
    .write \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .csv(path='s3a://mi-bucket/output_1/', sep=',')
    
    df2 \
    .write \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .csv(path='s3a://mi-bucket/output_1/', sep=',')
    
    df3 \
    .write \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .csv(path='s3a://mi-bucket/output_1/', sep=',')
    
    df4 \
    .write \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .csv(path='s3a://mi-bucket/output_1/', sep=',')
    
    df1 \
    .write \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .csv(path='s3a://mi-bucket/output_1/', sep=',')
    spark.stop()
    
except Exception as e:
    print("error reading TXT")
    print(e)

# Detener la sesión de Spark
spark.stop()
