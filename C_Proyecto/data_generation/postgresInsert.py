import psycopg2
import csv
from pyspark.sql import SparkSession

# Configuración de la conexión a la base de datos PostgreSQL
db_host = "localhost"
db_port = "9999"
db_name = "PrimOrd"
db_user = "primOrd"
db_password = "bdaPrimOrd"

# Conexión a la base de datos
conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_password)
cursor = conn.cursor()

# Archivos CSV
empleados_csv = "empleados.csv"
hoteles_csv = "hoteles.csv"

# Función para insertar datos de empleados desde CSV a PostgreSQL
def insert_empleados():
    with open(empleados_csv, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Saltar la fila de encabezado
        for row in reader:
            cursor.execute("INSERT INTO empleados (id_empleado, nombre, puesto, fecha_contratacion) VALUES (%s, %s, %s, %s)", row)
    conn.commit()

# Función para insertar datos de hoteles desde CSV a PostgreSQL
def insert_hoteles():
    with open(hoteles_csv, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Saltar la fila de encabezado
        for row in reader:
            cursor.execute("INSERT INTO hoteles (id_hotel, nombre_hotel, direccion, id_empleados) VALUES (%s, %s, %s, %s)", row)
    conn.commit()

# Llamar a las funciones para insertar datos
insert_empleados()
insert_hoteles()

# Cerrar la conexión a la base de datos
cursor.close()
conn.close()

print("Datos insertados en PostgreSQL exitosamente.")
