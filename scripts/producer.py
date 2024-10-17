from websocket_server import WebsocketServer
import logging
from typing import Dict, Any
from confluent_kafka import Producer
import json
from collections import deque
import ast

kafka_producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})
kpi_up_queue = deque(maxlen=10)

def produce(client: Dict[str, Any], server: WebsocketServer, message: str) -> None:
    """
    Обработчик полученного сообщения от клиента.

    :param client: Информация о клиенте.
    :param server: Экземпляр WebSocket сервера.
    :param message: Полученное сообщение.
    :return: None
    """
    try:

        data = ast.literal_eval(message)
        kpi_up = float(data['kpi_up'])
        kpi_down = float(data['kpi_down'])
        result = kpi_up / kpi_down

        kpi_up_queue.append(kpi_up)

        moving_average = sum(kpi_up_queue) / len(kpi_up_queue)

        threshold_exceeded = kpi_up > 5

        kafka_message = {
            'kpi_up': kpi_up,
            'kpi_down': kpi_down,
            'result': result,
            'moving_average': moving_average,
            'threshold_exceeded': threshold_exceeded,
            'timestamp': data['timestamp']
        }

        kafka_producer.produce('my_topic', key=str(data['id']), value=json.dumps(kafka_message))
        kafka_producer.flush()

        print(f"Сообщение от клиента {client['id']}: {message}")
        print(f"Результат деления kpi_up на kpi_down: {result}")
        print(f"Скользящее среднее kpi: {moving_average}")
        print(f"Порог kpi_up превышен: {threshold_exceeded}")
        print('Сообщение отправленно', '\n')
    except Exception as e:
        print(f"Ошибка при обработке сообщения: {e}")