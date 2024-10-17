import csv
import time
import logging
from websocket import create_connection
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_data() -> None:
    """
    Отправляет данные из CSV файла через WebSocket соединение.

    :return: None
    """
    uri = "ws://localhost:8765"
    ws = create_connection(uri)

    try:
        with open('../data_chunck.csv', newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                timestamp = datetime.now().isoformat()
                row['timestamp'] = timestamp
                ws.send(str(row))
                logging.info(f"Sent row: {row}")
                time.sleep(5)
    except Exception as e:
        logging.error(f"Error sending data: {e}")
    finally:
        ws.close()
        logging.info("WebSocket connection closed")

if __name__ == "__main__":
    send_data()
