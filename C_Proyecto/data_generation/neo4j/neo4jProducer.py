from kafka import KafkaProducer
from neo4j import GraphDatabase
import json

# Configuración de Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Conexión a Neo4j
uri = "bolt://neo4j:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neo4j_password"))

def get_data(tx, query):
    result = tx.run(query)
    return [record.data() for record in result]

with driver.session() as session:
    # Obtener datos de los menús y sus relaciones
    query = """
    MATCH (m:Menu)-[:CONTIENE]->(p:Plato)
    RETURN m, p
    """
    data = session.read_transaction(get_data, query)

# Enviar datos a Kafka
for record in data:
    producer.send('menus_stream', record)
    print(f"Enviado: {record}")

producer.flush()
print("Todos los datos han sido enviados a Kafka.")
driver.close()
