from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor, ListState
from pyflink.datastream.time_characteristic import TimeCharacteristic
from datetime import datetime
from pyflink.common import Types
import json

env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

env.add_jars(
    "file:///Users/ruslan/education/sensor_processing/new_venv/lib/python3.9/site-packages/pyflink/lib/flink-connector-kafka-3.1.0-1.18.jar",
    "file:///Users/ruslan/education/sensor_processing/new_venv/lib/python3.9/site-packages/pyflink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar",
    "file:///Users/ruslan/education/sensor_processing/new_venv/lib/python3.9/site-packages/pyflink/lib/kafka-clients-3.7.0.jar"
)

kafka_consumer = FlinkKafkaConsumer(
    topics='my_topic_4',
    deserialization_schema=SimpleStringSchema(),
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my_group'}
)

kafka_consumer.set_start_from_earliest()
stream = env.add_source(kafka_consumer)

def parse_json(json_str):
    data = json.loads(json_str)
    data['timestamp'] = datetime.strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%S.%f').isoformat()
    return data

stream = stream.map(lambda x: parse_json(x))

class MovingAverageFunction(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.window_elements = runtime_context.get_list_state(ListStateDescriptor("window-elements", Types.PICKLED_BYTE_ARRAY()))

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        self.window_elements.add(json.dumps(value).encode('utf-8'))
        ctx.timer_service().register_processing_time_timer(ctx.timer_service().current_processing_time() + 5000)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        elements = [json.loads(element.decode('utf-8')) for element in self.window_elements.get()]
        if len(elements) >= 5:
            kpi_up_values = [element['kpi_up'] for element in elements[-5:]]
            moving_avg_up = sum(kpi_up_values) / 5
            original_messages = [element for element in elements[-5:]]
            yield (moving_avg_up, original_messages)

stream = stream.key_by(lambda x: x['id'])
stream = stream.process(MovingAverageFunction())

kafka_producer = FlinkKafkaProducer(
    topic='my_topic_flink_test',
    serialization_schema=SimpleStringSchema(),
    producer_config={'bootstrap.servers': 'localhost:9092'}
)

def format_result(result):
    moving_avg_up, original_messages = result
    return json.dumps({'moving_avg_up': moving_avg_up, 'original_messages': original_messages})

stream = stream.map(lambda x: format_result(x), output_type=Types.STRING())
stream.add_sink(kafka_producer)
stream.print()

env.execute("Kafka to Flink Job with Moving Average")
