import psycopg2
import csv
import json

# Conexión a la base de datos
conn = psycopg2.connect(
    dbname="",
    user="",
    password="",
    host="localhost",
    port="9999"
)
cur = conn.cursor()

# Crear las tablas en la base de datos
cur.execute("""
    CREATE TABLE IF NOT EXISTS empleados (
        id_empleado INT PRIMARY KEY,
        nombre VARCHAR(100),
        posicion VARCHAR(100),
        fecha_contratacion DATE
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS habitaciones (
        numero_habitacion INT PRIMARY KEY,
        categoria VARCHAR(100),
        tarifa_por_noche NUMERIC
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS hoteles (
        id_hotel INT PRIMARY KEY,
        nombre_hotel VARCHAR(100),
        direccion_hotel TEXT,
        empleados INT[]
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS menu (
        id_menu INT PRIMARY KEY,
        precio NUMERIC,
        disponibilidad BOOLEAN,
        id_restaurante INT
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS platos (
        platoID INT PRIMARY KEY,
        nombre VARCHAR(100),
        ingredientes TEXT,
        alergenos TEXT
    )
""")

# Leer y cargar datos desde archivos CSV
with open('empleados.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Saltar la fila de encabezados
    for row in reader:
        cur.execute(
            "INSERT INTO empleados (id_empleado, nombre, posicion, fecha_contratacion) VALUES (%s, %s, %s, %s)",
            row
        )

with open('habitaciones.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Saltar la fila de encabezados
    for row in reader:
        cur.execute(
            "INSERT INTO habitaciones (numero_habitacion, categoria, tarifa_por_noche) VALUES (%s, %s, %s)",
            row
        )

with open('hoteles.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Saltar la fila de encabezados
    for row in reader:
        cur.execute(
            "INSERT INTO hoteles (id_hotel, nombre_hotel, direccion_hotel, empleados) VALUES (%s, %s, %s, %s)",
            row
        )

with open('menu.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Saltar la fila de encabezados
    for row in reader:
        cur.execute(
            "INSERT INTO menu (id_menu, precio, disponibilidad, id_restaurante) VALUES (%s, %s, %s, %s)",
            row
        )

with open('platos.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Saltar la fila de encabezados
    for row in reader:
        cur.execute(
            "INSERT INTO platos (platoID, nombre, ingredientes, alergenos) VALUES (%s, %s, %s, %s)",
            row
        )

# Leer y cargar datos desde archivos JSON
with open('restaurantes.json', 'r') as f:
    data = json.load(f)
    for item in data:
        cur.execute(
            "INSERT INTO restaurantes (id_restaurante, nombre, id_hotel) VALUES (%s, %s, %s)",
            (item['id_restaurante'], item['nombre'], item['id_hotel'])
        )

with open('clientes.json', 'r') as f:
    data = json.load(f)
    for item in data:
        cur.execute(
            "INSERT INTO clientes (id_cliente, nombre, direccion, preferencias_alimenticias) VALUES (%s, %s, %s, %s)",
            (item['id_cliente'], item['nombre'], item['direccion'], item['preferencias_alimenticias'])
        )

# Leer y cargar datos desde el archivo de texto
with open('reservas.txt', 'r') as f:
    lines = f.readlines()
    id_reserva = 1
    for line in lines:
        if "*** Reserva" in line:
            parts = line.split()
            id_cliente = parts[2]
            fecha_llegada = parts[5]
            fecha_salida = parts[8]
            tipo_habitacion = parts[11]
            preferencias_comida = parts[14]
            id_habitacion = parts[16]
            id_restaurante = parts[18]
            cur.execute(
                "INSERT INTO reservas (id_reserva, id_cliente, fecha_llegada, fecha_salida, tipo_habitacion, preferencias_comida, id_habitacion, id_rest
