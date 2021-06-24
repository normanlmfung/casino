''' There're two python packages: kafka-python (more mature), confluent-kafka (faster). 
    "confluent-kafka" didnt work, I ran into import problem "cannot import name 'KafkaProducer' from 'kafka'": https://stackoverflow.com/questions/62010589/importerror-cannot-import-name-kafkaproducer
    I ended up uninstall both, and reinstalled "kafka-python" before things working again.
        pip uninstall kafka-python
        pip uninstall confluent-kafka

    To run Kafka locally,
        STEP 1. Download Kafka from https://kafka.apache.org/downloads

        STEP 2. Unzip to C:\kafka

        STEP 3. Start zookeeper and brokers
        cd C:\kafka\kafka_2.13-2.7.0
        Start zookeeper: bin\windows\zookeeper-server-start.bat config\zookeeper.properties
        Start brokers (Need have separate broker.id, listeners, log.dirs). "listeners" in this format: listeners=PLAINTEXT://:9092 (no need actually specify hostname)
            bin\windows\kafka-server-start.bat config\server0.properties
            bin\windows\kafka-server-start.bat config\server1.properties
            bin\windows\kafka-server-start.bat config\server2.properties
    
    To check consumer groups:
        bin\windows\kafka-consumer-groups.bat  --list --bootstrap-server localhost:9092
        bin\windows\kafka-consumer-groups.bat  --bootstrap-server localhost:9092 --describe --group xxx
        bin\windows\kafka-consumer-groups.bat  --bootstrap-server localhost:9092 --describe --group xxxxxxxxxxxxx
        bin\windows\kafka-consumer-groups.bat  --bootstrap-server localhost:9092 --describe --group names-consumer-group

    config.dev.json referencing to local kafka - AWS MSK not accessible from Laptop, or outside AWS.
    
    References:
    a. kafka-python:
            https://kafka-python.readthedocs.io/en/master/usage.html  
            https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1
    b. confluent-kafka: https://www.confluent.io/blog/introduction-to-apache-kafka-for-python-programmers/?utm_medium=sem&utm_source=google&utm_campaign=ch.sem_br.nonbrand_tp.prs_tgt.kafka_mt.mbm_rgn.apac_lng.eng_dv.all_con.kafka-python&utm_term=%2Bkafka%20%2Bpython&creative=&device=c&placement=&gclid=CjwKCAjwwqaGBhBKEiwAMk-FtBWlGAJ0GXO9pMk13iuWpe50-tzLMZeDGgnKKob98umrbaELYARRjxoCGjwQAvD_BwE
    c. AWS MSK 
        Plaintext broker port 9092, TLS 9094
        https://docs.aws.amazon.com/msk/latest/developerguide/client-access.html

        AWS MSK not accessible from outside AWS
            https://aws.amazon.com/msk/faqs/
            https://repetitive.it/aws-msk-how-to-expose-the-cluster-on-the-public-network/?lang=en

'''

from datetime import datetime

''' 
So I changed ...
    FROM: from kafka import KafkaProducer 
    TO: from kafka.producer import KafkaProducer 
Reasons is: SyntaxError: invalid syntax (Strangely only when run from docker)
    https://stackoverflow.com/questions/63773535/got-syntaxerror-around-self-async-while-import-kafkaproducer-on-python-3-8-5-on
    https://github.com/dpkp/kafka-python/issues/1906 
'''
from kafka.producer import KafkaProducer 
from kafka.consumer import KafkaConsumer

from gizmo import logging_gizmo

JOB_NAME = "kafka_gizmo"
logFilename=f"./shared/log/{JOB_NAME}_{datetime.now().strftime('%Y%m%d')}.log"

def publish(bootstrap_servers, topic, messages, encoding='utf-8'):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    for message in messages:
        ''' Exception has occurred: AssertionError exception: no description
        '''
        producer.send(topic, message.encode(encoding))
        logging_gizmo.log_info(logFilename,f"Published message: {message}")

def consume(bootstrap_servers, topic, handlers = None, consumer_group_id = None):
    def _default_handler(message):
        logging_gizmo.log_info(logFilename,f"Consumed message: {message}")

    if not handlers or not isinstance(handlers, list):
        handlers = []
    handlers.append(_default_handler)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group_id
    )
    
    for message in consumer:
        for handler in handlers:
            handler(message)

if __name__ == '__main__':
    from faker import Faker
    fake = Faker()
    
    kafka_brokers = [ "localhost:9092", "localhost:9093","localhost:9094"]

    messages = []
    import pandas as pd
    import numpy as np
    df = pd.DataFrame(np.random.randint(0,100,size=(100, 10)), columns=list('ABCDEFGHIJ'))
    df_json = df.to_json()
    for _ in range(10000):
        messages.append(df_json)
    TOPIC = "random_data"
    publish(bootstrap_servers = kafka_brokers, topic = TOPIC, messages = messages)

    messages.clear()
    for _ in range(1000000):
        name = fake.name()
        messages.append(name)
    
    TOPIC = "random_names"
    publish(bootstrap_servers = kafka_brokers, topic = TOPIC, messages = messages)

    def _process_message(message):
        logging_gizmo.log_info(logFilename,f"_process_message: {message}")

    consume(bootstrap_servers = kafka_brokers, topic = TOPIC, handlers=[_process_message], consumer_group_id = 'names-consumer-group')
    
    logging_gizmo.log_info(logFilename,f"All done!")


