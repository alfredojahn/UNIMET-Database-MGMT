import numpy as np
from datetime import timedelta
import random


class Person():
    def __init__(self, store_id, has_mask, arrival_time, methods, banks, items):
        self.store_id = store_id
        self.is_new = random.choice([True, False])
        self.has_mask = has_mask
        self.shopping_cart = self.fill_cart(items)
        self.card = random.choice(methods)
        self.bank = random.choice(banks)
        # Genera la temperatura con la que llega la persona
        self.body_temp = np.round(np.random.normal(37, 0.9), decimals=2)
        self.arrival_time = arrival_time.strftime("%Y-%m-%d %H:%M:%S")
        self.exit_time = (arrival_time + timedelta(minutes=int(np.random.normal(50, 15)))).strftime(
            "%Y-%m-%d %H:%M:%S")   # Genera la hora/fecha en la que el cliente se va a ir de la tienda

        #'2011-05-16 15:36:38'

    # Genera un carrito random
    def fill_cart(self, items):
        while True:
            item_num = np.random.poisson(3)  # Numero de productor diferentes
            if item_num > 0:
                break
        cart = dict()
        for _ in range(item_num):
            item = random.choice(items[self.store_id])
            if item not in cart.keys():
                cart[item] = random.randint(1, 4)  # Cantidad de cada producto
        return cart
