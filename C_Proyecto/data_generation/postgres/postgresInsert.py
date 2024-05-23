import psycopg2
import csv



try:
    # Conectar a la base de datos
    conn = psycopg2.connect(
            host="localhost",
            database="primord",
            user="postgres",
            password="bda",
            port=9999
        )
    cursor = conn.cursor()

    # Crear tablas (si no existen)
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

    # Insertar datos en la tabla empleados
    with open('C:/Users/A.A.R.O/Desktop/Proyecto/ProyectoBDA/C_Proyecto/data_Prim_ord/csv/empleados.csv', 'r') as f:  
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute("""
                INSERT INTO empleados (nombre, posicion, fecha_contratacion)
                VALUES (%s, %s, %s)
            """, (row['nombre'], row['posicion'], row['fecha_contratacion']))
    conn.commit()

    # Insertar datos en la tabla hoteles
    with open('C:/Users/A.A.R.O/Desktop/Proyecto/ProyectoBDA/C_Proyecto/data_Prim_ord/csv/hoteles.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute("""
                INSERT INTO hoteles (nombre_hotel, direccion_hotel, empleados)
                VALUES (%s, %s, %s)
            """, (row['nombre_hotel'], row['direccion_hotel'], row['empleados']))
    conn.commit()

    print("Datos insertados en Postgres correctamente.")

except psycopg2.Error as e:
    print("Error al conectar a la base de datos PostgreSQL:", e)

finally:
    # Cerrar el cursor y la conexión
    if 'cursor' in locals() and cursor is not None:
        cursor.close()
    if 'conn' in locals() and conn is not None:
        conn.close()
