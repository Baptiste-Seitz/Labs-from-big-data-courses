from confluent_kafka.admin import AdminClient, NewTopic

config = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(config)

new_topic = 'book-topic'

# Création du topic avec 1 partition
admin_client.create_topics([
    NewTopic(new_topic, num_partitions=1, replication_factor=1)
])

# Affichage des topics existants
metadata = admin_client.list_topics(timeout=5)
print("Topics sur le cluster :", list(metadata.topics.keys()))