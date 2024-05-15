from kafka import KafkaProducer
import json

# Configuración de Kafka
bootstrap_servers = 'localhost:9092'
topic_name = 'reservas_topic'

# Crear un productor de Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Leer el archivo de reservas
with open('reservas.txt', 'r') as file:
    reserva_id = None
    reserva_data = {}
    for line in file:
        # Buscar el inicio de una nueva reserva
        if line.startswith('*** Reserva'):
            # Si hay datos de reserva previa, enviarlos a Kafka
            if reserva_id is not None:
                producer.send(topic_name, value=reserva_data)
                print(f"Reserva {reserva_id} enviada a Kafka.")
            
            # Obtener el ID de la nueva reserva
            reserva_id = int(line.split()[2])
            reserva_data = {}
        else:
            # Procesar los datos de la reserva
            key, value = line.strip().split(': ')
            reserva_data[key] = value
    
    # Enviar la última reserva a Kafka
    if reserva_id is not None:
        producer.send(topic_name, value=reserva_data)
        print(f"Reserva {reserva_id} enviada a Kafka.")

# Cerrar el productor de Kafka
producer.close()
