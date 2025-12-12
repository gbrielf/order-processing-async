import pika
from pika import PlainCredentials
import json
import time
from pika import exceptions # <<< CORREÇÃO 1: Adicionar a importação de exceções

RABBITMQ_USER = 'user'
RABBITMQ_PASS = 'password'
RABBITMQ_HOST = 'rabbitmq'
QUEUE_NAME = 'pedidos_pendentes'

def callback_stock(ch, method, properties, body): # Note: função chamada 'callback_stock'
    # Callback chamado para uma mensagem é recebida da fila

    # Desserialização do corpo da mensagem
    order = json.loads(body)
    order_id = order.get('order_id', 'N/A')

    print(f"[Estoque] Recebido ID: {order_id} - Iniciando baixa de estoque...")

    # Simula o processamento lento (Baixa de Estoque)
    time.sleep(3)

    # Lógica de negócio
    print(f"[Estoque] *** SUCESSO ***: Estoque atualizado para o Pedido ID {order_id}.")

    # ACK(Acknowledgment) - Confirmação ao MOM
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"[Estoque] ACK enviado para Pedido ID: {order_id}")

def start_consumer_stock():
    print(' [Estoque] Tentando conectar ao RabbitMQ...')
    
    # Configuração de Retry
    max_retries = 10
    retry_delay = 5 # Espera de 5 segundos entre as tentativas
    
    for attempt in range(max_retries):
        try:
            credentials = PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            
            # TENTA CONECTAR:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
            
            # Se a conexão for bem-sucedida, o código continua aqui
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_qos(prefetch_count=1) 
            
            # CORREÇÃO 2: A função de callback deve ser 'callback_stock'
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback_stock)
            
            print(' [Estoque] [*] Conexão estabelecida! Aguardando pedidos.')
            
            # Inicia o consumo (processo principal)
            channel.start_consuming() 
            return 
            
        except exceptions.AMQPConnectionError as e:
            if attempt + 1 < max_retries:
                print(f" [Estoque] Falha na conexão (Tentativa {attempt+1}/{max_retries}). Retentando em {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(" [Estoque] Erro fatal: Máximo de retries atingido. Encerrando.")
                raise e

if __name__ == '__main__':
    start_consumer_stock()