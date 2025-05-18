import pika
from encryption import decrypt_message
from db_postgres import insert_normalized

def receive_via_rabbitmq(config):
    rabbit_conf = config['rabbitmq-import']
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_conf['host']))
    channel = connection.channel()
    channel.queue_declare(queue=rabbit_conf['queue'])

    def callback(ch, method, properties, body):
        row = decrypt_message(config, body)
        insert_normalized(config, row)

    channel.basic_consume(queue=rabbit_conf['queue'], on_message_callback=callback, auto_ack=True)
    print("Waiting for messages...")
    channel.start_consuming()