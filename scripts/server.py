from websocket_server import WebsocketServer
import logging
from typing import Dict, Any
from confluent_kafka import Producer
from collections import deque
from producer import produce

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

kafka_producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

kpi_up_queue = deque(maxlen=10)

def new_client(client: Dict[str, Any], server: WebsocketServer) -> None:
    """
    Обработчик нового подключения клиента.

    :param client: Информация о клиенте.
    :param server: Экземпляр WebSocket сервера.
    :return: None
    """
    logging.info(f"Новое подключение клиента: {client['id']}")


def run_server() -> None:
    """
    Запускает WebSocket сервер.

    :return: None
    """
    server = WebsocketServer(host='0.0.0.0', port=8765)
    server.set_fn_new_client(new_client)
    server.set_fn_message_received(produce)
    print("WebSocket сервер запущен.")
    server.run_forever()

if __name__ == "__main__":
    run_server()
