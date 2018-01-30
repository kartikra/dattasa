import pika
import threading
import os


class Producer(threading.Thread):
    def __init__(self, config_file, db_credentials, db_host):

        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

        self.config_file = config_file
        self.db_credentials = db_credentials
        self.db_host = db_host
        return

    def stop(self):
        self.stop_event.set()
        return

    def send(self, queue_name, message, auto_delete=False):

        db_credentials = self.db_credentials
        rabbitmq_host = self.db_host
        exception = ""

        connect_string = 'amqp://' + db_credentials[rabbitmq_host]['user'] + ':' + \
                         db_credentials[rabbitmq_host]['password'] + '@' + \
                         db_credentials[rabbitmq_host]['host'] + ':' + \
                         str(db_credentials[rabbitmq_host]['port']) + '/' + \
                         db_credentials[rabbitmq_host]['vhost'] + '/%2f'

        url = os.environ.get('CLOUDAMQP_URL', connect_string)
        try:
            params = pika.URLParameters(url)
            params.socket_timeout = 5
            connection = pika.BlockingConnection(params)  # Connect to RabbitMQ Host
        except Exception as e:
            exception = "Unable to connect to rabbitmq. error message: {0}".format(e)

        if exception == "":
            try:
                channel = connection.channel()  # start a channel
                channel.queue_declare(queue=queue_name , durable=True ,
                                      auto_delete=auto_delete)  # Declare a queue - durable=True
                channel.basic_publish(exchange='' ,
                                      routing_key=queue_name,
                                      body=message ,
                                      properties=pika.BasicProperties(
                                          delivery_mode=2,  # make message persistent
                                        )
                                      )
                connection.close()
            except Exception as e:
                connection.close()
                exception = "Error while transfer meesage. error message: {0}".format(e)

        return exception
