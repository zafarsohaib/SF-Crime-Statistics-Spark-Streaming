from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):
    """
    Kafka Producer Server Class
    """
    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        """
        Reading data from input file and sending out messages to Kafka topic
        """
        with open(self.input_file) as f:
            data = json.load(f)
            for line in data:
                message = self.dict_to_binary(line)
                self.send(self.topic, message)
                time.sleep(0.2)

    def dict_to_binary(self, json_dict):
        """
        convert json dictionary to binary format
        """
        return json.dumps(json_dict).encode("utf-8")
        