import json
import paho.mqtt.client as mqtt
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv

# -----------------------Load DB credentials --------------------------------
load_dotenv()
HOST = "batyr.db.elephantsql.com"
USERNAME = "wbefhcue"
PASSWORD = "IR4BuSQ9QdBj5_5QbCYLcl3I5zlyC31T"
# --------------------------------------------------------


def register_entry(customer, conn):
    try:
        cur = conn.cursor()
        # Busca un customer_id que ya exista o crea un nuevo usuario
        if customer['is_new']:
            query = """insert into customer(affiliate,fidelity_lvl) values('false','0')"""
            cur.execute(query)
            conn.commit()
            query = """select max(customer_id) from customer"""
            cur.execute(query)
            id = cur.fetchall()[0][0]
        else:
            query = """select customer_id from customer order by random() limit 1"""
            cur.execute(query)
            id = cur.fetchall()[0][0]

        values = (customer['has_mask'], customer['body_temp'],
                  customer['arrival_time'], id, customer['store_id'])

        query = """insert into entry_registry(has_mask,body_temp,arrival_time,customer_id,store_id) values('{}','{}','{}','{}','{}')""".format(
            *values)

        cur.execute(query)
        conn.commit()
        cur.close()
        return id
    except (Exception, Error) as error:
        print("No query for you! Error: ", error)
        return


def shop(customer, conn):

    items = list()
    quants = list()
    # crea un par de listas con los items a comprar
    for key in customer['shopping_cart'].keys():
        items.append(int(key))
    for value in customer['shopping_cart'].values():
        quants.append(int(value))

    # crear carrito
    cur = conn.cursor()
    query = """insert into active_cart(store_id,customer_id) values('{}','{}')""".format(
            customer['store_id'], customer['id'])
    cur.execute(query)
    conn.commit()
    # buscar id del carrito
    query = """select cart_id from active_cart where customer_id = '{}'""".format(
            customer['id'])
    cur.execute(query)
    cart_id = cur.fetchall()[0][0]
    # ingresar items al carrito
    for i, q in zip(items, quants):
        query = """insert into cart_content(cart_id,product_id,quantity) values('{}','{}','{}')""".format(
            cart_id, i, q)
        cur.execute(query)
        conn.commit()

    # get total a pagar
    query = """SELECT SUM(product_price.price*cart_content.quantity)
	            FROM cart_content
	            INNER JOIN product_price
	            ON cart_content.product_id = product_price.product_id
	            WHERE cart_content.cart_id = {}""".format(cart_id)
    cur.execute(query)
    total = cur.fetchall()[0][0]
    # registrar compra
    values = (customer['store_id'], customer['id'], customer['card'],
              customer['bank'], total, customer['exit_time'])
    try:
        query = """insert into sale(store_id,customer_id,pay_method,bank,amount,date) values('{}','{}','{}','{}',{},'{}')""".format(
            *values)
        cur.execute(query)
        conn.commit()
        # get id de compra
        query = """select sale_id from sale where customer_id = '{}' order by date desc limit 1""".format(
            customer['id'])
        cur.execute(query)
        sale_id = cur.fetchall()[0][0]
        # insertar sale_details
        for i, q in zip(items, quants):
            query = """insert into sale_details(sale_id,product_id,quantity) values ('{}','{}','{}')""".format(
                sale_id, i, q)
            cur.execute(query)
            conn.commit()
    except (Exception, Error) as error:
        print("error en compra: ", error)

    # vaciar carrito
    query = """delete from active_cart where cart_id = '{}'""".format(cart_id)
    cur.execute(query)
    conn.commit()
    try:
        # get registry id
        query = """select entry_id from entry_registry where customer_id = '{}' order by exit_time desc limit 1""".format(
            customer['id'])
        cur.execute(query)
        entry_id = cur.fetchall()[0][0]
        # ponerle hora de salida al comprador y chao pescao
        query = """update entry_registry set exit_time = '{}' where entry_id = '{}'""".format(
            customer['exit_time'], entry_id)
        cur.execute(query)
        conn.commit()
        cur.close()
        print('compra exitosa!')
    except (Exception, Error) as error:
        print("Error en la recta final: ", error)


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("iot_wallmart", qos=2)


def on_message(client, userdata, msg):
    try:
        conn = psycopg2.connect(dbname=USERNAME, user=USERNAME,
                                host=HOST, password=PASSWORD)
    except:
        print("I am unable to connect to the database")

    x = json.loads(str(msg.payload.decode("utf-8")))
    x['id'] = register_entry(x, conn)
    # rebotado de la tienda, no sigue con los queries
    if x['body_temp'] >= 38 or x['has_mask'] is False:
        return
    shop(x, conn)
    conn.close()


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("localhost", 1883, 60)
client.loop_forever()
