import producer_server


def run_kafka_server():
    """
    Runs Kafka server
    """
    input_file = "/home/workspace/police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="department.police.service.call",
        bootstrap_servers="localhost:9092",
        client_id="police_call"
    )

    return producer


def feed():
    """
    Runs Kafka server and generate data to Kafka topic
    """
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
