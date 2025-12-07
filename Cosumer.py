from kafka import KafkaConsumer
import mysql.connector
import json
import joblib
import pandas as pd

# -------------------------------
# Load pre-trained model
# -------------------------------
model = joblib.load("fraudModel")

# -------------------------------
# Kafka consumer
# -------------------------------
topic="payments"
consumer = KafkaConsumer(
    topic,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# -------------------------------
# MySQL connection
# -------------------------------
db = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='payments_db'
)
cursor = db.cursor()

print("Consuming transactions and scoring...")

for msg in consumer:
    txn = msg.value
    print(txn)
    
    # -------------------------------
    # Prepare features for ML model
    # -------------------------------
    features = pd.DataFrame([{
        'age': txn['age'],
        'income': txn['income'],
        'credit_score': txn['credit_score'],
        'avg_monthly_spend': txn['avg_monthly_spend'],
        'active_loans': txn['active_loans'],
        'payment_delay_days': txn['payment_delay_days'],
        'transaction_amount': txn['transaction_amount'],
        'account_balance': txn['account_balance']
    }])
    
    # -------------------------------
    # Generate fraud probability score
    # -------------------------------
    fraud_prob = model.predict_proba(features)[:,1][0]  # probability of fraud
    txn['fraud_score'] = round(fraud_prob, 4)
    
    # -------------------------------
    # Determine action (example)
    # -------------------------------
    if txn['fraud_score'] > 0.8:
        txn['action'] = 'DECLINE'
    elif txn['fraud_score'] > 0.5:
        txn['action'] = 'REVIEW'
    else:
        txn['action'] = 'APPROVE'
    
    # -------------------------------
    # Insert scored transaction into MySQL
    # -------------------------------
    query = """
    INSERT INTO payment_transactions (
        transaction_id, customer_id, age, income, employment_status, credit_score,
        avg_monthly_spend, active_loans, payment_delay_days, last_payment_amount,
        transaction_amount, transaction_type, account_balance, fraud_flag,
        currency, payment_method, timestamp, fraud_score, action
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        fraud_score=VALUES(fraud_score),
        action=VALUES(action)
    """
    
    values = (
        txn.get("transaction_id"),
        txn.get("customer_id"),
        txn.get("age"),
        txn.get("income"),
        txn.get("employment_status"),
        txn.get("credit_score"),
        txn.get("avg_monthly_spend"),
        txn.get("active_loans"),
        txn.get("payment_delay_days"),
        txn.get("last_payment_amount"),
        txn.get("transaction_amount"),
        txn.get("transaction_type"),
        txn.get("account_balance"),
        txn.get("fraud_flag"),
        txn.get("currency"),
        txn.get("payment_method"),
        txn.get("timestamp"),
        txn.get("fraud_score"),
        txn.get("action")
    )
    
    cursor.execute(query, values)
    db.commit()
    
    print(f"Transaction {txn['transaction_id']} scored: {txn['fraud_score']} → {txn['action']}")
