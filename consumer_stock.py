import pika
from pika import PlainCredentials
import json
import time

RABBITMQ_USER = 'user'
RABBITMQ_PASS = 'password'
RABBITMQ_HOST = 'host.docker.internal'
QUEUE_NAME = 'pedidos_pendentes'

def callback_stock(ch, method, properties, body):
    # Callback chamado para uma mensagem é recebida da fila

    # Desserialização do corpo da mensagem
    order = json.loads(body)
    order_id = order.get('order_id', 'N/A')

    print(f"[Estoque] Recebido ID: {order_id} - Iniciando baixa de estoque...")

    time.sleep(3)

    # Lógica de negócio
    print(f"[Estoque] *** SUCESSO ***: Estoque atualizado para o Pedido ID {order_id}.")

    # ACK(Acknowledgment) - Confirmação ao MOM
    # CRITÉRIO CUMPRIDO: Sinaliza que a mensagem foi processada e pode ser removida da fila
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"[Estoque] ACK enviado para Pedido ID: {order_id}")

def start_consumer_stock():
    print('[Estoque] Tentando conectar ao RabbitMQ...')
    # Definindo as credenciais
    credentials = PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

    # Conexão (com retires simples, se necessário)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    )
    channel = connection.channel()

    # Garantir que a fila exista (o mesmo nome usado pelo Produtor)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # Configurar para receber apenas 1 mensagem por vez (QoS)
    channel.basic_qos(prefetch_count=1)

    # Assinar a fial
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback_stock
    )

    print(' [Estoque] [*] Aguardando pedidos. Pressione CNTRL+C para sair.')
    # Iniciar o consumo
    channel.start_consuming()

if __name__ == '__main__':
    start_consumer_stock()