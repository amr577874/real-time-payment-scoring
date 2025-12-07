#Produce data to kafka topic
#auto generated
import uuid
import random
import time
import json
from kafka import KafkaProducer

# -----------------------
# Kafka configuration
# -----------------------
BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC = 'payments'

# -----------------------
# Possible values
# -----------------------
employment_statuses = ["employed", "self-employed", "unemployed", "retired"]
transaction_types = ["purchase", "refund", "withdrawal", "deposit"]
currencies = ["USD", "EUR", "SAR", "GBP"]
payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer"]

# -----------------------
# Create Kafka producer
# -----------------------
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=50,        # batch messages for 50ms
    batch_size=16384,    # 16 KB batch
    acks=1               # wait for leader ack only (faster)
)

print(f"Producing transaction events to topic: {TOPIC}")

# -----------------------
# Function to generate a transaction event
# -----------------------
def generate_payment_event():
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "age": random.randint(18, 75),
        "income": round(random.uniform(2000, 20000), 2),
        "employment_status": random.choice(employment_statuses),
        "credit_score": random.randint(300, 850),
        "avg_monthly_spend": round(random.uniform(100, 5000), 2),
        "active_loans": random.randint(0, 5),
        "payment_delay_days": random.randint(0, 60),
        "last_payment_amount": round(random.uniform(50, 5000), 2),
        "transaction_amount": round(random.uniform(10, 10000), 2),
        "transaction_type": random.choice(transaction_types),
        "account_balance": round(random.uniform(0, 50000), 2),
        "fraud_flag": random.choice([0,1]),
        "currency": random.choice(currencies),
        "payment_method": random.choice(payment_methods),
        "timestamp": int(time.time() * 1000)
    }

# -----------------------
# Produce messages
# -----------------------
count = 0
while True:
    event = generate_payment_event()
    producer.send(TOPIC, event)
    count += 1

    # Flush every 100 messages for efficiency
    if count % 100 == 0:
        producer.flush()
        print(f"Flushed {count} messages... latest transaction_id: {event['transaction_id']}")
