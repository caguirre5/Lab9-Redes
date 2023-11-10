from confluent_kafka import Producer
import random
import json
import time

# Configura el servidor y el topic de Kafka
bootstrap_servers = 'lab9.alumchat.xyz:9092'
topic = '20231'

# Configura el productor Kafka
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'python-producer'
}

producer = Producer(producer_config)

# Función para generar datos aleatorios con distribución normal


def generar_data():
    temperatura = round(random.gauss(50, 10), 1)  # Media 50, Varianza 10
    humedad = round(random.gauss(50, 10), 1)  # Media 50, Varianza 10
    direccion = random.choice(['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE'])
    datos = {'temperatura': temperatura,
             'humedad': humedad, 'direccion_viento': direccion}
    return datos


try:
    while True:
        # Genera datos aleatorios
        data = generar_data()

        # Envia los datos al servidor Kafka
        producer.produce(topic, key='sensor20231', value=json.dumps(data))
        producer.flush()

        print(f"Datos enviados: {data}")
        time.sleep(3)
except KeyboardInterrupt:
    print("Interrupción del usuario. Cerrando el productor Kafka.")
