from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from kafka import KafkaUtils
import json

# Configuración de Spark
spark = SparkSession.builder \
    .appName("SparkKafkaConsumer") \
    .getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sc, 5)  # Se actualiza cada 5 segundos

# Configuración de Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "menus_stream"

# Función para procesar los datos del stream
def process_stream_data(rdd):
    if not rdd.isEmpty():
        # Convertir el RDD a DataFrame
        df = spark.read.json(rdd)
        
        # Realizar cualquier procesamiento necesario
        df.show()

# Crear un receptor de Kafka
kafka_params = {"bootstrap.servers": kafka_bootstrap_servers}
kafka_stream = KafkaUtils.createDirectStream(ssc, [kafka_topic], kafka_params)

# Obtener los datos del stream y procesarlos
lines = kafka_stream.map(lambda x: json.loads(x[1]))
lines.foreachRDD(process_stream_data)

# Iniciar el flujo de Spark Streaming
ssc.start()
ssc.awaitTermination()
