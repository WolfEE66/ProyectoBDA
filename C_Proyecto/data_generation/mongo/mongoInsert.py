import pymongo
import json

# Conexión a MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["hotel_management"]
collection = db["clientes"]

# Leer datos desde clientes.json
with open('C:/Users/A.A.R.O/Desktop/Proyecto/ProyectoBDA/C_Proyecto/data_Prim_ord/json/clientes.json') as f:
    clientes = json.load(f)

# Insertar datos en la colección de MongoDB
collection.insert_many(clientes)

print("Datos insertados en MongoDB correctamente.")


