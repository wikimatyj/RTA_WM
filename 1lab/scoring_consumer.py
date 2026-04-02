from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def score_transaction(tx):
    score = 0
    rules = []
    
    amount = tx.get('amount', 0)
    category = tx.get('category', '')
    hour = int(tx.get('timestamp', '1970-01-01T00:00:00')[11:13])
    
    if amount > 3000:
        score += 3
        rules.append('R1')
        
    if category == 'elektronika' and amount > 1500:
        score += 2
        rules.append('R2')
        
    if hour < 6:
        score += 2
        rules.append('R3')
    
    return score, rules


for message in consumer:
    tx = message.value
    
    score, rules = score_transaction(tx)
    
    if score >= 3:
        
        tx['score'] = score
        tx['rules_triggered'] = rules
        tx['alert'] = True
        
        
        alert_producer.send('alerts', value=tx)
        
        print(
            f" ALERT | {tx['tx_id']} | "
            f"{tx['amount']:.2f} PLN | "
            f"score: {score} | rules: {rules}"
        )

alert_producer.flush()
alert_producer.close()
