from kafka import KafkaProducer
import json

# Configuración de Kafka
bootstrap_servers = 'localhost:9092'
topic = 'neo4j_to_kafka_topic'

# Conexión a Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Función para enviar los datos a Kafka
def send_data_to_kafka(data):
    try:
        # Enviar los datos a Kafka
        for record in data:
            producer.send(topic, json.dumps(record).encode('utf-8'))

        print("Datos enviados a Kafka desde Neo4j.")

    except Exception as e:
        print("Error al enviar datos a Kafka:", e)

# Obtener datos de Neo4j y enviarlos a Kafka
data_from_neo4j = []  # Agrega aquí la lógica para obtener los datos de Neo4j
send_data_to_kafka(data_from_neo4j)

# Cerrar el productor de Kafka
producer.close()
