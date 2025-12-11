import pika
import json
import time

RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'pedidos_pendentes'

def callback_estoque