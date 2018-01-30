import threading, time
import multiprocessing
import socket
import json
import os
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.common import LeaderNotAvailableError
from kafka.errors import KafkaError


'''
                KAFKA BASICS
describe a topic
/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic test


creating a topic
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic test
For replication factor to be > 1, there has to be more than 1 kafka broker

deleting a topic
/usr/local/kafka/config
delete.topic.enable=true 

/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test


                python kafka package DOCUMENTATION
http://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
http://kafka-python.readthedocs.io/en/master/apidoc/kafka.producer.html

                REFERENCES
http://www.giantflyingsaucer.com/blog/?p=5541
https://pythonexample.com/code/python-kafka-consumer-record/
https://community.hortonworks.com/articles/74077/kafka-producer-sample-code-in-scala-and-python.html
https://www.confluent.io/blog/introduction-to-apache-kafka-for-python-programmers/
http://www.giantflyingsaucer.com/blog/?p=5541
https://github.com/dpkp/kafka-python/blob/master/example.py


                KAFKA Installation
http://davidssysadminnotes.blogspot.in/2016/01/installing-apache-kafka-and-zookeeper.html


                KAFKA GUI Tool
https://github.com/HomeAdvisor/Kafdrop
cd Kafdrop
mvn clean package
java -jar <path to folder>/Kafdrop/target/kafdrop-2.0.0.jar --zookeeper.connect=<zookeeper-host>:2181 --server.port=9090

goto http://localhost:9090/ to see the UI

vi /etc/systemd/system/kafdrop.service

[Unit]
Description=kafkadrop UI for Apache Kakfa server (broker)
Documentation=https://github.com/HomeAdvisor/Kafdrop
Requires=network.target remote-fs.target
After=network.target remote-fs.target zookeeper.service kafka.service

[Service]
Type=simple
User=my_user
Group=my_group
Environment=JAVA_HOME=/opt/jdk1.8.0_121/
ExecStart=/usr/bin/env "${JAVA_HOME}/bin/java" -jar /opt/Kafdrop/target/kafdrop-2.0.0.jar --zookeeper.connect=kakfa-server:2181 --server.port=9090

[Install]
WantedBy=multi-user.target

:wq

systemctl daemon-reload
service kafdrop start

'''


class BatchProducer(threading.Thread):
    def __init__(self, config_file, db_credentials, db_host):
        threading.Thread.__init__(self)
        self.config_file = config_file
        self.stop_event = threading.Event()
        self.db_credentials = db_credentials
        self.db_host = db_host
        self.bootstrap_servers = self.db_credentials[self.db_host]['bootstrap_servers']
        return

    def stop(self):
        self.stop_event.set()
        return

    def send(self, topic_name, message, topic_partition=-1):

        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        try:
            exception = ""
            if topic_partition == -1:
                producer.send(topic_name, message)
            else:
                producer.send(topic_name, message, partition=topic_partition)

        except LeaderNotAvailableError:
            # https://github.com/mumrah/kafka-python/issues/249
            time.sleep(1)
            if topic_partition == -1:
                producer.send(topic_name, message)
            else:
                producer.send(topic_name, message, partition=topic_partition)

        except KafkaError as ex:
            print(ex)
            exception = str(ex)

        finally:
            producer.close()

        return exception


class BatchConsumer(multiprocessing.Process):

    def __init__(self, config_file, db_credentials, db_host):

        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        self.config_file = config_file
        self.db_credentials = db_credentials
        self.db_host = db_host
        self.bootstrap_servers = self.db_credentials[self.db_host]['bootstrap_servers']

        return

    def stop(self):
        self.stop_event.set()
        return

    def connect(self, group_id="", client_id="",
                auto_offset_reset='earliest', consumer_timeout_ms=1000):

        client_id = socket.gethostname() if client_id == "" else client_id
        group_id = os.getlogin() if group_id == "" else group_id

        exception = ""
        try:
            consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers,
                                     auto_offset_reset=auto_offset_reset,
                                     consumer_timeout_ms=consumer_timeout_ms,
                                     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                     group_id=group_id,
                                     client_id=client_id,
                                     enable_auto_commit=False
                                     )
            self.consumer = consumer

        except KafkaError as ex:
            print(ex)
            exception = str(ex)
            self.consumer = None

        return exception

    def receive(self, topic_name, topic_partition=None, message_offset=None,
                out_dir='', out_file_name=''):

        exception = ""
        consumer = self.consumer

        try:
            if out_dir == '':
                out_dir = os.environ['HOME']

            if topic_partition is None:

                ''' Get all messages from all partitions '''
                consumer.subscribe(topic_name)

                if out_file_name == '':
                    out_file_name = str(self.db_host) + "_" + str(topic_name)
                out_file = open(out_dir + "/" + out_file_name + "_all.txt", 'w')

                for message in consumer:
                    # This will wait and print messages as they become available
                    kafka_message = dict()
                    kafka_message["topic"] = message.topic
                    kafka_message["partition"] = message.partition
                    kafka_message["offset"] = message.offset
                    kafka_message["key"] = message.key
                    kafka_message["value"] = message.value
                    kafka_message["timestamp"] = message.timestamp
                    kafka_message["timestamp_type"] = message.timestamp_type
                    out_file.write(json.dumps(kafka_message) + "\n")
                out_file.close()

            else:
                partition_list = [TopicPartition(topic_name, topic_partition)]
                consumer.assign(partition_list)

                ''' Start fetching data from partition '''
                for topic_partition in consumer.assignment():
                    print("fetching results for {0} ...".format(topic_partition))

                    if message_offset is not None:
                        print("setting offset to {0}".format(message_offset))
                        consumer.seek(partition=topic_partition, offset=message_offset)
                    else:
                        print("last committed offset {0}".format(consumer.end_offsets([topic_partition])[topic_partition]))
                        consumer.poll()

                    if out_file_name == '':
                        out_file_name = str(self.db_host) + "_" + str(topic_name)

                    if out_dir == '':
                        out_dir = os.environ['HOME']
                    out_file = open(out_dir + "/" + out_file_name + "_" + str(topic_partition.partition) + ".txt", 'w')
                    for message in consumer:
                        # This will wait and print messages as they become available
                        kafka_message = dict()
                        kafka_message["topic"] = message.topic
                        kafka_message["partition"] = message.partition
                        kafka_message["offset"] = message.offset
                        kafka_message["key"] = message.key
                        kafka_message["value"] = message.value
                        kafka_message["timestamp"] = message.timestamp
                        kafka_message["timestamp_type"] = message.timestamp_type
                        out_file.write(json.dumps(kafka_message) + "\n")
                    out_file.close()

        except KafkaError as ex:
            print(ex)
            exception = str(ex)
            consumer.close()
        except KeyboardInterrupt:
            pass

        return exception

    def commit(self):
        self.consumer.commit()
        return

    def close(self):
        self.consumer.close()
        return

