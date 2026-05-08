from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='enrich-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def get_risk_level(amount):
    if amount > 3000:
        return "HIGH"
    elif amount > 1000:
        return "MEDIUM"
    else:
        return "LOW"

for message in consumer:
    tx = message.value
    tx['risk_level']= get_risk_level(tx['amount'])
    print(f"[{tx['risk_level']:6s}] {tx['tx_id']} | {tx['amount']:.2f} PLN")
