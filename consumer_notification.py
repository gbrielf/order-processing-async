import pika
from pika import PlainCredentials
import json
import time

RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'pedidos_pendentes'
RABBITMQ_USER = 'user'
RABBITMQ_PASS = 'password'

def callback_notification(ch, method, properties, body):
    # Callback chamado quando uma mensagem é recebida na fila

    order = json.loads(body)
    order_id = order.get('order_id', 'N/A')

    print(f"[Notificação] Recebida pelo ID: {order_id} - Preparando e-mail...")

    #simulação de Processamento Rápido
    time.sleep(1)

    # Lógica de negócio (envio de e-mail)
    print(f"[Notificação] >>> E-mail de confirmação enviado para o Pedido ID {order_id}.")

    # ACK (Acknowledgment) - Confirmação ao MOM
    ch.basick_ack(delivery_tag = method.delivery_tag)
    print(f"[Notificação] ACK enviado para Pedido ID: {order_id}")

def start_consumer_notification():
    print('[Notificação] Tentando conectar ao RabbitMQ...')
    # Definindo as credenciais
    credentials = PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    )
    channel = connection.channel()

    # Garantir que a fila exista
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # Configurar para receber apenas 1 mensagem por vez (QoS)
    channel.basic_qos(prefetch_count=1)

    # Assinar a fila
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback_notification
    )

    print('[Notificação] [*] Aguardando pedidos. Pressione CNTRL+C para sair.')
    channel.start_consuming()

if __name__ == '__main__':
    start_consumer_notification()