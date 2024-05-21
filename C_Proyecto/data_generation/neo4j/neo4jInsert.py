import csv
import json
from neo4j import GraphDatabase

# Definir la función para insertar datos
def insert_data(tx, query, data):
    tx.run(query, **data)

# Definir la conexión con Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "123123123"
driver = GraphDatabase.driver(uri, auth=(username, password))

try:
    # Leer y cargar datos de platos.csv
    with open('C:/Users/A.A.R.O/Desktop/Proyecto/ProyectoBDA/C_Proyecto/data_Prim_ord/csv/platos.csv', mode='r') as file:
        reader = csv.DictReader(file)
        platos_data = [row for row in reader]

    # Leer y cargar datos de menus.csv
    with open('C:/Users/A.A.R.O/Desktop/Proyecto/ProyectoBDA/C_Proyecto/data_Prim_ord/csv/menu.csv', mode='r') as file:
        reader = csv.DictReader(file)
        menus_data = [row for row in reader]

    # Leer y cargar datos de relaciones.json 
    with open('C:/Users/A.A.R.O/Desktop/Proyecto/ProyectoBDA/C_Proyecto/data_Prim_ord/json/relaciones.json') as file:
        relaciones_data = json.load(file)

    with driver.session() as session:
        # Insertar platos
        for plato in platos_data:
            query = """
            CREATE (p:Plato {platoID: $platoID, nombre: $nombre, ingredientes: $ingredientes, alergenos: $alergenos})
            """
            session.write_transaction(insert_data, query, plato)
        
        # Insertar menus
        for menu in menus_data:
            query = """
            CREATE (m:Menu {id_menu: $id_menu, precio: $precio, disponibilidad: $disponibilidad, id_restaurante: $id_restaurante})
            """
            session.write_transaction(insert_data, query, menu)
        
        # Insertar relaciones
        for relacion in relaciones_data:
            query = """
            MATCH (m:Menu {id_menu: $id_menu}), (p:Plato {platoID: $id_plato})
            CREATE (m)-[:CONTIENE]->(p)
            """
            session.write_transaction(insert_data, query, relacion)

    print("Datos insertados en Neo4j correctamente.")

finally:
    if 'driver' in locals():
        driver.close()
