import psycopg2
import csv

# Configuración de la base de datos
db_config = {
    "dbname": "PrimOrd",
    "user": "primOrd",
    "password": "bdaPrimOrd",
    "host": "localhost",
    "port": "9999"
}

# Conectar a la base de datos
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Crear tablas
cursor.execute("""
    CREATE TABLE IF NOT EXISTS empleados (
        id_empleado SERIAL PRIMARY KEY,
        nombre VARCHAR(100),
        posicion VARCHAR(100),
        fecha_contratacion DATE
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS hoteles (
        id_hotel SERIAL PRIMARY KEY,
        nombre_hotel VARCHAR(100),
        direccion_hotel TEXT,
        empleados TEXT
    )
""")
conn.commit()

# Insertar datos en empleados
with open('/opt/spark-data_Prim_ord/empleados.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        cursor.execute("""
            INSERT INTO empleados (id_empleado, nombre, posicion, fecha_contratacion)
            VALUES (%s, %s, %s, %s)
        """, (row['id_empleado'], row['nombre'], row['posicion'], row['fecha_contratacion']))
conn.commit()

# Insertar datos en hoteles
with open('/opt/spark-data_Prim_ord/hoteles.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        cursor.execute("""
            INSERT INTO hoteles (id_hotel, nombre_hotel, direccion_hotel, empleados)
            VALUES (%s, %s, %s, %s)
        """, (row['id_hotel'], row['nombre_hotel'], row['direccion_hotel'], row['empleados']))
conn.commit()

print("Datos insertados en Postgres correctamente.")
cursor.close()
conn.close()
