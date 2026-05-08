from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import numpy as np

app= FastAPI(title="Fraud Detection API")

# Model ładowany przy starcie serwera (raz)
model= pickle.load(open('fraud_model.pkl', 'rb'))

class Transaction(BaseModel):
    amount: float
    is_electronics: int
    tx_per_minute: int
    
@app.post("/score")
def score(tx: Transaction):
    # Cechy w tej samej kolejności co podczas treningu:
    # ['amount', 'is_electronics', 'tx_per_minute']
    X= np.array([[tx.amount, tx.is_electronics, tx.tx_per_minute]])
    proba= model.predict_proba(X)[0, 1] # prawdopodobieństwo klasy 1 (fraud)
    return {
        "is_fraud": bool(proba >= 0.5),
        "fraud_probability": round(float(proba), 4)
    }
@app.get("/health")
def health():
    return {"status": "ok"}
