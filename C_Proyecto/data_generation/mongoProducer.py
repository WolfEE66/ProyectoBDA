from kafka import KafkaProducer
import pymongo
import json

# Conexión a MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["proyectobd"]  
collection = db["proyectocoleccion"] 

# Conexión al servidor de Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Enviar datos desde MongoDB a Kafka
for document in collection.find():
    # Convertir el documento de MongoDB a formato JSON
    message = json.dumps(document).encode('utf-8')
    
    # Enviar el mensaje a Kafka en el topic 'clientes_stream'
    producer.send('clientes_stream', value=message)

# Esperar a que todos los mensajes sean enviados antes de cerrar el productor
producer.flush()

# Cerrar la conexión al servidor de Kafka
producer.close()

print("Datos enviados a Kafka desde MongoDB.")
