import pymongo
import json

# Conexión a la base de datos MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["proyectobd"]  
collection = db["proyectocoleccion"]  

# Cargar datos desde el archivo clientes.json
with open('C:/Users/Desktop/Proyecto/ProyectoBDA/C_Proyecto/data_Prim_ord/json/clientes.json') as file:
    data = json.load(file)

# Insertar los datos en la colección
collection.insert_many(data)

print("Datos insertados exitosamente en MongoDB.")
