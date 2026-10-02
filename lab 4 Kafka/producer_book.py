import time
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

topic = 'book-topic'
file_path = 'book.txt'

print("Début de l'envoi du livre vers Kafka...")

with open(file_path, 'r', encoding='utf-8') as f:
    for line_number, line in enumerate(f, start=1):
        clean_line = line.strip()
        
        # Ignorer les lignes vides
        if not clean_line:
            continue
            
        # Envoi de la ligne au topic
        producer.produce(topic=topic, value=clean_line.encode('utf-8'))
        
        # Toutes les 100 lignes, traiter les événements internes et marquer une micro-pause
        if line_number % 100 == 0:
            producer.poll(0)
            print(f"Lignes envoyées : {line_number}")
            time.sleep(0.02)

producer.flush()
print("Livre envoyé en totalité !")