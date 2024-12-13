from kafka.admin import KafkaAdminClient, NewTopic
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_kafka_topic(bootstrap_servers: str, client_id: str, topic_name: str, num_partitions: int,
                       replication_factor: int) -> None:
    """
    Создает новый топик в Kafka.

    :param bootstrap_servers: Список серверов Kafka для подключения.
    :param client_id: Идентификатор клиента.
    :param topic_name: Имя топика.
    :param num_partitions: Количество партиций для топика.
    :param replication_factor: Фактор репликации для топика.
    :return: None
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id
        )
        topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logging.info(f"Топик '{topic_name}' успешно создан.")
    except Exception as e:
        logging.error(f"Ошибка при создании топика: {e}")


if __name__ == "__main__":
    bootstrap_servers = "localhost:9092"
    client_id = 'test'
    topic_name = "my_topic_4"
    num_partitions = 1
    replication_factor = 1

    create_kafka_topic(bootstrap_servers, client_id, topic_name, num_partitions, replication_factor)
