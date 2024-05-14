from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from kafka import KafkaUtils
import json

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Consumer Spark Streaming") \
    .getOrCreate()

# Crear un contexto de streaming de Spark con un intervalo de procesamiento de 5 segundos
ssc = StreamingContext(spark.sparkContext, 5)

# Configurar los parámetros de Kafka
kafka_params = {
    "bootstrap.servers": "localhost:9092",
    "auto.offset.reset": "largest",  # Comenzar desde el último offset
    "group.id": "spark-streaming-consumer"  # Identificador de grupo para el consumidor
}

# Crear un DStream que representa el flujo de mensajes de Kafka
kafka_stream = KafkaUtils.createDirectStream(ssc, ['clientes_stream'], kafka_params)

# Procesar los mensajes del stream
def process_message(message):
    # Convertir el mensaje JSON a diccionario
    data = json.loads(message[1])
    
    # Procesar los datos
    print("Nuevo mensaje recibido:", data)

# Aplicar la función de procesamiento a cada mensaje del stream
kafka_stream.foreachRDD(lambda rdd: rdd.foreach(process_message))

# Iniciar el procesamiento en streaming
ssc.start()

# Esperar hasta que el proceso de streaming sea detenido manualmente
ssc.awaitTermination()
