# kafka_pipeline.py

from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
import requests
import json
from datetime import datetime

# Función para obtener datos del clima
def fetch_weather(api_key, city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(url)
    data = response.json()
    
    # Convertir las temperaturas a grados Celsius
    data['main']['temp_min'] = data['main']['temp_min'] - 273.15
    data['main']['temp_max'] = data['main']['temp_max'] - 273.15
    
    return {
        "city": data['name'],
        "weather_description": data['weather'][0]['description'],
        "temp_min": data['main']['temp_min'],
        "temp_max": data['main']['temp_max'],
        "date": datetime.now().isoformat()
    }

# Función para obtener datos de calidad del aire
def fetch_aqicn(api_key, city):
    url = f"https://api.waqi.info/feed/{city}/?token={api_key}"
    response = requests.get(url)
    data = response.json()
    
    return {
        "city": data['data']['city']['name'],
        "aqi": data['data']['aqi'],
        "date": datetime.now().isoformat()
    }

def kafka_producer(api_key_weather, api_key_aqicn, cities):
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    for city in cities:
        # Obtener datos del clima
        weather_data = fetch_weather(api_key_weather, city)
        print(f"Weather data for {city} fetched:", weather_data)
        
        # Obtener datos de calidad del aire
        aqicn_data = fetch_aqicn(api_key_aqicn, city)
        print(f"AQICN data for {city} fetched:", aqicn_data)
        
        # Enviar datos a los tópicos respectivos
        producer.send('weather_topic', weather_data)
        print(f"Weather data for {city} sent to Kafka")
        producer.send('aqicn_topic', aqicn_data)
        print(f"AQICN data for {city} sent to Kafka")
    
    producer.flush()
    producer.close()

def kafka_consumer(mongo_uri):
    client = MongoClient(mongo_uri)
    db = client['data_pipeline']
    weather_collection = db['weather']
    aqicn_collection = db['aqicn']
    
    consumer = KafkaConsumer(
        'weather_topic', 'aqicn_topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='my-group',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    for message in consumer:
        print(f"Received message from {message.topic}")
        if message.topic == 'weather_topic':
            weather_collection.insert_one(message.value)
            print("Weather data inserted into MongoDB")
        elif message.topic == 'aqicn_topic':
            aqicn_collection.insert_one(message.value)
            print("AQICN data inserted into MongoDB")

if __name__ == "__main__":
    # Claves de API
    api_key_weather = '7b1bef8a229cd1ad47d78aab3371a853'
    api_key_aqicn = '813f73a1b1495f82cc3dda8b3b9bed190217212c'
    
    # URI de MongoDB Atlas
    mongo_uri = "mongodb+srv://a345635:123@pipeline-info.tdukogc.mongodb.net/"
    
    # Lista de ciudades para analizar
    cities = ["Chihuahua", "Mexico City", "Guadalajara", "Monterrey", "Beijing"]
    
    # Ejecutar productor
    print("Ejecutando productor...")
    kafka_producer(api_key_weather, api_key_aqicn, cities)
    
    # Ejecutar consumidor
    print("Consumiendo datos de Kafka y almacenándolos en MongoDB...")
    kafka_consumer(mongo_uri)
    
