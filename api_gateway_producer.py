# recebe a requisição e publica a mensagem
from flask import Flask, request, jsonify
import pika
from pika import PlainCredentials
import json

app = Flask(__name__)

# Configuração do RabbitMQ
RABBITMQ_HOST = 'host.docker.internal'
QUEUE_NAME = 'pedidos_pendentes'
RABBITMQ_USER = 'user'
RABBITMQ_PASS = 'password'

# Função para publicar mensagem
def publish_order(order_data):
    connection = None
    try:
        # Definindo as credenciais
        credentials = PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        # primeiro: conexão com o Broker (MOM)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
        )
        channel = connection.channel()

        # segundo: garantir que a fila exista
        # 'durable = True' garante que a fila sobreviva a reinicializações do Rabbit
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        # terceiro: publicar a mensagem(transformada em JSON string)
        channel.basic_publish(
            exchange='',  # usa um exchange padrão(default)
            routing_key=QUEUE_NAME,
            body=json.dumps(order_data),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent  # Mensagem persistente
            )
        )
        print(f" [x] Pedido Publicado: {order_data.get('order_id')}", flush=True)
        return True
    except pika.exceptions.AMQPConnectionError as e:
        print(f"[!] Erro na conexão com o RabbitMQ: {e}", flush=True)
        return False
    finally:
        # IMPORTANTE: sempre fechar a conexão para evitar vazamento de recursos
        if connection and not connection.is_closed:
            connection.close()
    
#  Endpoint da API Gateway
@app.route('/api/order', methods=['POST'])
def process_order():
    if not request.is_json:
        return jsonify({"message": "Missing JSON in request"}), 400
    
    order_data = request.get_json()

    # Adicionando um timestamp para facilitar a demonstração
    import datetime
    order_data['timestamp'] = datetime.datetime.now().isoformat()
    
    print(f" [→] Recebido pedido: {order_data.get('order_id')}", flush=True)

    if publish_order(order_data):
        # A requisição foi aceita no processamento, mas ainda não foi concluída
        return jsonify ({
            "status": "Pedido aceito e em processamento assíncrono",
            "order_id": order_data.get('order_id')
        }), 202
    else:
        return jsonify({"status": "Falha ao enviar o pedido para a fila", "error": "MOM indisponível"}), 503

if __name__ == '__main__':
    # Você pode rodar com 'flask run' ou diretamente:
    app.run(host='0.0.0.0', port=5000)