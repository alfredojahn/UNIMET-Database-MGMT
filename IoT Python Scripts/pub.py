# Clases
from person import Person
# Math
import numpy as np
# Date & Time
from dateutil import rrule
from datetime import datetime, timedelta
import time
# Utilidad
import json
import sys
# MQTT
import paho.mqtt.client as mqtt

# ---------  Variables de la simulacion --------------------------------------
# 1 - indices de items en las respectivas tiendas
productos = dict()
productos[1] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 32]  # indices items castellana
productos[2] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]  # indices items hatillo
# 2 - metodos de pago
methods = ["Debito", "Credito"]
banks = ["Bancamiga", "Banesco", "Mercantil Panama"]
# ----------------------------------------------------------------------------


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connection to broker failed")
    else:
        print("Connected with result code "+str(rc))


def main():
    client = mqtt.Client("publicador_iot")
    client.connect("localhost", 1883, 60)
    client.qos = 2

    # num de dias a simular empezando desde hoy
    now = datetime(2021, 7, 15, 14, 00)
    until_date = now + timedelta(days=30)
    entradas_registradas = 0
    # --------------------------- Loop para generar clientes
    for dt in rrule.rrule(rrule.MINUTELY, dtstart=now, until=until_date):

        hour = int(dt.strftime("%H"))  # para el horario

        # Check entre 8am y 9pm
        if hour > 8 and hour < 21:

            # Tienda 1 ----------------------------------------------------------------------------------
            mask = np.random.poisson(10/60)  # num de clientes con mascara/min
            # num de clientes sin mascara/min
            no_mask = np.random.poisson(15/60)

            # Generar n cantidad de clientes con mascara
            for _ in range(mask):
                persona = json.dumps(
                    Person(store_id=1, has_mask=True, arrival_time=dt, methods=methods, banks=banks, items=productos).__dict__)
                client.publish("iot_wallmart", payload=persona)
                entradas_registradas += 1

            # Generar n cantidad de clientes sin mascara
            for _ in range(no_mask):
                persona = json.dumps(
                    Person(store_id=1, has_mask=False, arrival_time=dt, methods=methods, banks=banks, items=productos).__dict__)
                client.publish("iot_wallmart", payload=persona)
                entradas_registradas += 1

            # Tienda 2 ----------------------------------------------------------------------------------
            mask = np.random.poisson(10/60)
            no_mask = np.random.poisson(15/60)

            for _ in range(mask):
                persona = json.dumps(
                    Person(store_id=2, has_mask=True, arrival_time=dt, methods=methods, banks=banks, items=productos).__dict__)
                client.publish("iot_wallmart", payload=persona)
                entradas_registradas += 1

            for _ in range(no_mask):
                persona = json.dumps(
                    Person(store_id=2, has_mask=False, arrival_time=dt, methods=methods, banks=banks, items=productos).__dict__)
                client.publish("iot_wallmart", payload=persona)
                entradas_registradas += 1

            time.sleep(0.1)

    print("Se registraron {} entradas al finalizar la simulacion".format(
        entradas_registradas))

    client.disconnect()


if __name__ == '__main__':
    main()
    sys.exit(0)
