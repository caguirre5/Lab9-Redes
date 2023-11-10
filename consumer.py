import threading
from confluent_kafka import Consumer, KafkaError
import tkinter as tk
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import json

# Configura el servidor y el topic de Kafka
bootstrap_servers = 'lab9.alumchat.xyz:9092'
topic = '20231'

# Configura el consumidor Kafka
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'  # Lee desde el inicio del topic
}

consumer = Consumer(consumer_config)
consumer.subscribe([topic])

# Listas para almacenar los datos
all_temp = []
all_hume = []
all_wind = []


# ... (configuración de Kafka y listas de datos)

# Función para procesar y graficar datos

def plot_all_data():
    figure.clear()
    ax = figure.add_subplot(111)
    ax.plot(all_temp, label='Temperatura')
    ax.plot(all_hume, label='Humedad')
    ax.legend()
    ax.set_xlabel('Muestras')
    ax.set_ylabel('Valores')
    ax.set_title('Telemetría en Vivo')
    canvas.draw()


# Crear una ventana de Tkinter
root = tk.Tk()
root.title('Telemetría en Vivo')

# Crear una figura de Matplotlib
figure = Figure(figsize=(5, 4), dpi=100)

# Crear un lienzo para la figura
canvas = FigureCanvasTkAgg(figure, master=root)
canvas.get_tk_widget().pack()

# Función para iniciar el consumo de Kafka en un hilo


def start_kafka_consumer():
    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    print("No más mensajes en la partición.")
                else:
                    print(f"Error en el mensaje: {message.error()}")
            else:
                payload = json.loads(message.value())
                all_temp.append(payload['temperatura'])
                all_hume.append(payload['humedad'])
                all_wind.append(payload['direccion_viento'])

                print(f"Datos recibidos: {payload}")

                # Grafica los datos en tiempo real
                plot_all_data()
    except KeyboardInterrupt:
        print("Interrupción del usuario. Cerrando el consumidor Kafka.")
    finally:
        consumer.close()


# Iniciar el consumidor de Kafka en un hilo
kafka_thread = threading.Thread(target=start_kafka_consumer)
kafka_thread.daemon = True
kafka_thread.start()

# Iniciar la aplicación de Tkinter
root.mainloop()
