from kafka import KafkaProducer, KafkaConsumer;
import requests, json;

topic1 = 'input_weatherData1'
topic2 = 'output_weatherData2'

consumer = KafkaConsumer(topic1, bootstrap_servers='localhost:9092', value_deserializer=lambda x: json.loads(x.decode('utf-8')));
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda x: json.dumps(x).encode('utf-8'));

for message in consumer:
    
    jsonData = message.value
    
    temp = float(jsonData['main']['temp'])
    print(temp)

    obradjeniPodaci = {
        'city': jsonData.get('name','Uknown City'),
        'temperatura': round(temp,2)
    }

    producer.send(topic2, value=obradjeniPodaci)
    producer.flush()

producer.close()
consumer.close()
    

