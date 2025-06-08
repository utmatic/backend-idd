from flask import Flask
import os, json
import firebase_admin
from firebase_admin import credentials, firestore

app = Flask(__name__)

@app.route("/")
def test_firestore():
    try:
        cred_dict = json.loads(os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON"))
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        docs = list(db.collection('inddJobs').limit(1).stream())
        return f"Success! Documents found: {len(docs)}"
    except Exception as e:
        return f"Firestore error: {e}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
