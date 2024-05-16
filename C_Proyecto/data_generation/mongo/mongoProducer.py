from kafka import KafkaProducer
import pymongo
import json

# Configuración de Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Conexión a MongoDB
client = pymongo.MongoClient("mongodb://mongo:27017/")
db = client["hotel_management"]
collection = db["clientes"]

# Leer datos desde MongoDB
clientes = list(collection.find())

# Enviar datos a Kafka
for cliente in clientes:
    producer.send('clientes_stream', cliente)
    print(f"Enviado: {cliente}")

producer.flush()
print("Todos los datos han sido enviados a Kafka.")
