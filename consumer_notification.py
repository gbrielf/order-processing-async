import pika
from pika import PlainCredentials, exceptions
import json
import time

RABBITMQ_HOST = 'rabbitmq'
QUEUE_NAME = 'pedidos_pendentes'
RABBITMQ_USER = 'user'
RABBITMQ_PASS = 'password'

def callback_notification(ch, method, properties, body):
    # Callback chamado quando uma mensagem é recebida na fila

    order = json.loads(body)
    order_id = order.get('order_id', 'N/A')

    print(f"[Notificação] Recebida pelo ID: {order_id} - Preparando e-mail...")

    # Simulação de Processamento Rápido
    time.sleep(1)

    # Lógica de negócio (envio de e-mail)
    print(f"[Notificação] >>> E-mail de confirmação enviado para o Pedido ID {order_id}.")

    # ACK (Acknowledgment) - Confirmação ao MOM
    # CORREÇÃO 1: Nome correto do método é basic_ack
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"[Notificação] ACK enviado para Pedido ID: {order_id}")

def start_consumer_notification():
    # CORREÇÃO 2: Log correto para este serviço
    print(' [Notificação] Tentando conectar ao RabbitMQ...')
    
    # Configuração de Retry
    max_retries = 10
    retry_delay = 5 # Espera de 5 segundos entre as tentativas
    
    for attempt in range(max_retries):
        try:
            credentials = PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            
            # TENTA CONECTAR:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
            
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_qos(prefetch_count=1) 
            
            # CORREÇÃO 3: Chamar a função de callback correta para Notificação
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback_notification)
            
            print(' [Notificação] [*] Conexão estabelecida! Aguardando pedidos.')
            
            channel.start_consuming() 
            return 
            
        except exceptions.AMQPConnectionError as e:
            if attempt + 1 < max_retries:
                # CORREÇÃO 2 (continuação): Log correto
                print(f" [Notificação] Falha na conexão (Tentativa {attempt+1}/{max_retries}). Retentando em {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                # CORREÇÃO 2 (continuação): Log correto
                print(" [Notificação] Erro fatal: Máximo de retries atingido. Encerrando.")
                raise e
if __name__ == '__main__':
    start_consumer_notification()