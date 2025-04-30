from configparser import ConfigParser
import pika
import json
import psycopg2

config = ConfigParser()
config.read('config.ini')

# RabbitMQ credentials
RABBITMQ_URL = config['RABBITMQ']['URL'] 
RABBITMQ_PORT = int(config['RABBITMQ']['PORT'])
RABBITMQ_USERNAME = config['RABBITMQ']['USERNAME'] 
RABBITMQ_APIKEY = config['RABBITMQ']['APIKEY']
RABBITMQ_VHOST = config['RABBITMQ']['VHOST'] 
EXCHANGE_NAME = config['RABBITMQ']['EXCHANGE_NAME']
ROUTING_KEY = config['RABBITMQ']['ROUTING_KEY'] 

# PostgreSQL credentials
PG_HOST = config['POSTGRES']['HOST']
PG_PORT = config['POSTGRES']['PORT']
PG_DB = config['POSTGRES']['DB']
PG_USER = config['POSTGRES']['USER']
PG_PASSWORD = config['POSTGRES']['PASSWORD']

def fetch_data_from_postgres():
    """Fetch data from PostgreSQL res_partner table."""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()

    query = """
        <sql-query>
    """
    cur.execute(query)

    columns = [desc[0] for desc in cur.description]
    data = [dict(zip(columns, row)) for row in cur.fetchall()]

    cur.close()
    conn.close()
    return data

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
        message = {
            "id": "<Resource-ID>",
            "beneficiaryID": record["beneficiaryID"],
            "nationalID": record["nationalID"],
            "programName": record["programName"],
            "status": record["status"],
            "enrollmentDate": record["enrollmentDate"],
        }

        message_body = json.dumps(message, default=str)
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
    
    connection.close()

# Main execution
if __name__ == "__main__":
    data_list = fetch_data_from_postgres()
    publish_message(data_list)
