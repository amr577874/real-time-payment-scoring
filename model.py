# train model
from sklearn.ensemble import RandomForestClassifier
import joblib
import pandas as pd

# Example training data
df = pd.read_csv("fraud_status.csv")
X = df[['age','income','credit_score','avg_monthly_spend',
                'active_loans','payment_delay_days','transaction_amount','account_balance']]
y = df['fraud_flag']

model = RandomForestClassifier(n_estimators=100)
model.fit(X, y)

joblib.dump(model, "fraudModel")