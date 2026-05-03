from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json
import time
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers = 'broker:9092',
    value_deserializer = lambda x: json.loads(x.decode('utf-8'))
)

customer_alert = defaultdict(list)

for m in consumer:
    event = m.value
    customer = event['user_id']
    time = datetime.fromisoformat(event['timestamp'])

    if len(customer_alert[customer]) < 4:
        customer_alert[customer].append(time)
        if len(customer_alert[customer]) == 4:
            if customer_alert[customer][0] > datetime.now() - timedelta(seconds=60):
                print(f'ALERT: Klient {customer} wykonał więcej niż 3 transakcje w ciągu 60 sekund.')
    else:
        customer_alert[customer].pop(0)
        customer_alert[customer].append(time)
        if customer_alert[customer][0] > datetime.now() - timedelta(seconds=60):
                print(f'ALERT: Klient {customer} wykonał więcej niż 3 transakcje w ciągu 60 sekund.')
    