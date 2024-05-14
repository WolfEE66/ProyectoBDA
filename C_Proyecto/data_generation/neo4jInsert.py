from neo4j import GraphDatabase
import csv
import json

# Conexión a Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"
driver = GraphDatabase.driver(uri, auth=(username, password))

# Función para insertar datos de los archivos CSV
def insert_data_from_csv(tx):
    # Leer y procesar menu.csv
    with open('menu.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            tx.run("MERGE (m:Menu {id: $id}) "
                   "SET m.precio = $precio, m.disponibilidad = $disponibilidad",
                   id=row['id_menu'], precio=row['precio'], disponibilidad=row['disponibilidad'])

    # Leer y procesar platos.csv
    with open('platos.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            tx.run("MERGE (p:Plato {id: $id}) "
                   "SET p.nombre = $nombre, p.ingredientes = $ingredientes, p.alergenos = $alergenos",
                   id=row['platoID'], nombre=row['nombre'], ingredientes=row['ingredientes'], alergenos=row['alergenos'])

# Función para insertar relaciones desde relaciones.json
def insert_relations(tx):
    with open('relaciones.json') as jsonfile:
        data = json.load(jsonfile)
        for rel in data:
            tx.run("MATCH (r:Restaurante {id: $id_restaurante}) "
                   "MATCH (h:Hotel {id: $id_hotel}) "
                   "MERGE (r)-[:PERTENECE_A]->(h)",
                   id_restaurante=rel['id_restaurante'], id_hotel=rel['id_hotel'])

# Ejecutar las transacciones
with driver.session() as session:
    session.write_transaction(insert_data_from_csv)
    session.write_transaction(insert_relations)

print("Datos insertados en Neo4j.")
