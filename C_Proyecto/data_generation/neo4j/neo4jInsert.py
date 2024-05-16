from neo4j import GraphDatabase
import csv
import json

# Conexión a Neo4j
uri = "bolt://neo4j:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neo4j_password"))

def insert_data(tx, query, parameters=None):
    tx.run(query, parameters)

# Leer y cargar datos de menu.csv
with open('/opt/spark-data_Prim_ord/menu.csv', mode='r') as file:
    reader = csv.DictReader(file)
    menu_data = [row for row in reader]

# Leer y cargar datos de platos.csv
with open('/opt/spark-data_Prim_ord/platos.csv', mode='r') as file:
    reader = csv.DictReader(file)
    platos_data = [row for row in reader]

# Leer y cargar datos de relaciones.json
with open('/opt/spark-data_Prim_ord/relaciones.json') as file:
    relaciones_data = json.load(file)

with driver.session() as session:
    # Insertar menús
    for menu in menu_data:
        query = """
        CREATE (m:Menu {id_menu: $id_menu, precio: $precio, disponibilidad: $disponibilidad, id_restaurante: $id_restaurante})
        """
        session.write_transaction(insert_data, query, menu)
    
    # Insertar platos
    for plato in platos_data:
        query = """
        CREATE (p:Plato {platoID: $platoID, nombre: $nombre, ingredientes: $ingredientes, alergenos: $alergenos})
        """
        session.write_transaction(insert_data, query, plato)
    
    # Insertar relaciones
    for relacion in relaciones_data:
        query = """
        MATCH (m:Menu {id_menu: $id_menu}), (p:Plato {platoID: $id_plato})
        CREATE (m)-[:CONTIENE]->(p)
        """
        session.write_transaction(insert_data, query, relacion)

print("Datos insertados en Neo4j correctamente.")
driver.close()
