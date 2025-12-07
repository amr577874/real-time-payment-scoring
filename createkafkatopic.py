#Create Kafka Topic
from kafka.admin import NewTopic
from kafka.admin import KafkaAdminClient

topic_name = "payments"
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='admin'
)
topic_list = [NewTopic(name=topic_name, num_partitions=3, replication_factor=1)]
admin_client.create_topics(new_topics=topic_list, validate_only=False)

print(f"Topic '{topic_name}' created")