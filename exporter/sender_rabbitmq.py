import pika
from encryption import encrypt_message

def send_via_rabbitmq(config, data_iter):
    rabbit_conf = config['rabbitmq-export']
    credentials = pika.PlainCredentials(rabbit_conf["user"], rabbit_conf["password"])
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_conf['host'], credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=rabbit_conf['queue'])
    for data in data_iter:
        msg = encrypt_message(config, data)
        channel.basic_publish(exchange='', routing_key=rabbit_conf['queue'], body=msg)
    connection.close()