from confluent_kafka import Consumer, KafkaException

def consume_data(topic_name, bootstrap_servers):
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([topic_name])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    topic_name = 'my_topic'
    bootstrap_servers = 'localhost:9092'
    consume_data(topic_name, bootstrap_servers)