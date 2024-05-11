import psycopg2
import csv

# Conectar a PostgreSQL 
conexion = psycopg2.connect(
    host="localhost",
    database="PrimOrd",
    user="primOrd",
    password="bdaPrimOrd"
)

# Crear un cursor
cursor = conexion.cursor()

# Crear tabla si no existe

create_table_empleados = '''CREATE TABLE IF NOT EXISTS empleados (
    id_empleado SERIAL PRIMARY KEY,
    nombre VARCHAR(255),
    fecha_contratacion DATE,
    posicion TEXT
)'''


create_table_hoteles = '''CREATE TABLE IF NOT EXISTS hoteles (
    id_hotel SERIAL PRIMARY KEY,
    nombre_hotel VARCHAR(255),
    direccion_hotel VARCHAR(255)
)'''


def read_csv_file(filename):
    data =[]
    with open(filename, 'r') as file:
        reader= csv.reader(file)
        for element in reader:
            data.append(element)        
    return data

class Database:
    def __init__(self, host, database, user, password):
        self.conexion = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        self.cursor = self.connection.cursor()

    def create_table(self,stringCreate):
        self.cursor.execute(stringCreate)
        self.connection.commit()

    def insert_data(self, query,params):
        self.cursor.execute(query, params)
        self.connection.commit()
        
readerEmpleados= read_csv_file("C:/Users/Vespertino/Desktop/m3/data_Prim_ord/empleados.csv")
readerHoteles=read_csv_file("C:/Users/Vespertino/Desktop/m3/data_Prim_ord/hoteles.csv")

DB_HOST = "localhost"
DB_DATABASE = "PrimOrd"
DB_USER = "primOrd"
DB_PASSWORD: "bdaPrimOrd" 

db = Database(DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE)
db.create_table(create_table_empleados)
db.create_table(create_table_hoteles)


for element in readerEmpleados[1:]:
    insert_query = "INSERT INTO Empleados (id_empleado,nombre,posicion,fecha_contratacion) VALUES (%s, %s, %s, %s)"
    data= (element[0],element[1],element[2],element[3])
    db.insert_data(insert_query,data)
    
for element in readerHoteles[1:]:
    insert_query = "INSERT INTO Rentals (id_hotel,nombre_hotel,direccion_hotel) VALUES (%s, %s, %s, %s)"
    data= (element[0],element[1],element[2],element[3])
    db.insert_data(insert_query,data)

# Confirmar los cambios
conexion.commit()

# Cerrar conexión
cursor.close()
conexion.close()

print("Datos insertados correctamente en la base de datos PostgreSQL.")
