from configparser import ConfigParser
import pika
import json

config = ConfigParser()
config.read('config.ini')

# RabbitMQ credentials
RABBITMQ_URL = config['RABBITMQ']['URL'] 
RABBITMQ_PORT = config['RABBITMQ']['PORT'] 
RABBITMQ_USERNAME = config['RABBITMQ']['USERNAME'] 
RABBITMQ_APIKEY = config['RABBITMQ']['APIKEY']
RABBITMQ_VHOST = config['RABBITMQ']['VHOST'] 
EXCHANGE_NAME = config['RABBITMQ']['EXCHANGE_NAME']
ROUTING_KEY = config['RABBITMQ']['ROUTING_KEY'] 

# Load demographic data from JSON file
data_file = "../demo/data/income_data.json"
with open(data_file, "r") as file:
    data_list = json.load(file)

def publish_message(data):
    """Connects to RabbitMQ and publishes a message."""
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_APIKEY)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_URL,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    for record in data:
        message_body = json.dumps(record)
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            body=message_body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2  # Persistent messages
            )
        )
        print(f" [x] Sent message: {message_body}")
    
    # Close connection
    connection.close()

# Send the demographic data to RabbitMQ
publish_message(data_list)
