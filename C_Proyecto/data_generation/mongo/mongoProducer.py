from kafka import KafkaProducer
import pymongo
import json
from bson import ObjectId

# Configuración de Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# Conexión a MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["hotel_management"]
collection = db["clientes"]

# Leer datos desde MongoDB
clientes = list(collection.find())

# Enviar datos a Kafka
for cliente in clientes:
    # Convertir ObjectId a string
    cliente['_id'] = str(cliente['_id'])
    producer.send('clientes_stream', cliente)
    print(f"Enviado: {cliente}")

producer.flush()
print("Todos los datos han sido enviados a Kafka.")
