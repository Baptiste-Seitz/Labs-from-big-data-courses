import string
import time
from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    # Un groupe unique garantit une lecture propre depuis le début
    'group.id': f'book-cleaner-{int(time.time())}',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['book-topic'])

output_file_path = 'cleaned_book.txt'
MAX_EMPTY_POLLS = 5
empty_polls = 0
received_count = 0

def clean_text(text: str) -> str:
    # 1. Minuscules
    text = text.lower()
    # 2. Suppression de la ponctuation
    text = text.translate(str.maketrans('', '', string.punctuation))
    # 3. Espaces nettoyés
    return " ".join(text.split())

print("Consommateur connecté, synchronisation et lecture des données...")

with open(output_file_path, 'w', encoding='utf-8') as out_f:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            # On ne décompte le silence que si on a déjà commencé à dépiler des messages
            if received_count > 0:
                empty_polls += 1
                if empty_polls >= MAX_EMPTY_POLLS:
                    print("Fin du flux : tous les messages ont été consommés.")
                    break
            else:
                print("En attente de l'assignation de la partition par le broker...")
            continue

        if msg.error():
            print(f"Erreur : {msg.error()}")
            break

        empty_polls = 0
        received_count += 1
        raw_line = msg.value().decode('utf-8')
        cleaned_line = clean_text(raw_line)

        if cleaned_line:
            out_f.write(cleaned_line + '\n')

        if received_count % 500 == 0:
            print(f"Lignes traitées et nettoyées : {received_count}")

consumer.close()
print(f"Terminé ! Total : {received_count} lignes écrites dans '{output_file_path}'.")