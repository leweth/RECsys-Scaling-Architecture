from time import sleep
import csv 
from kafka import KafkaProducer
import json

# Créer un producteur Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],  # Adresses des brokers Kafka
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))  # Sérialiser les valeurs des messages en JSON et les encoder en UTF-8

# Ouvrir le fichier CSV
with open('Kafka_testing_dataset/dataset.csv') as file_obj:
    reader_obj = csv.reader(file_obj)  # Lire le fichier CSV
    for data in reader_obj: 
        if data[0] == 'rating':  # Vérifier si la ligne est l'en-tête
            print("Skipping header row ...")  # Afficher un message et passer à la ligne suivante
            continue
        print(data)  # Afficher les données de la ligne
        producer.send('numtest', value=data)  # Envoyer les données au sujet 'numtest' de Kafka
        sleep(3)  # Attendre 3 secondes avant de lire la ligne suivante
