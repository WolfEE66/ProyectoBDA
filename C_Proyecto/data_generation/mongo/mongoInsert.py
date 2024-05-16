import pymongo
import json

# Conexión a MongoDB
client = pymongo.MongoClient("mongodb://mongo:27017/")
db = client["hotel_management"]
collection = db["clientes"]

# Leer datos desde clientes.json
with open('/opt/spark-data_Prim_ord/clientes.json') as f:
    clientes = json.load(f)

# Insertar datos en la colección de MongoDB
collection.insert_many(clientes)

print("Datos insertados en MongoDB correctamente.")
