from fastapi import FastAPI
import os, json
import firebase_admin
from firebase_admin import credentials, firestore
import traceback

app = FastAPI()

@app.get("/")
def test_firestore():
    """Test endpoint: checks for Google credentials, Firestore connectivity (read/write), and returns clear diagnostics."""
    diagnostics = {}
    # 1. Check environment variable
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not creds_json:
        diagnostics["error"] = "GOOGLE_APPLICATION_CREDENTIALS_JSON env variable is missing."
        return diagnostics

    # 2. Try to init Firebase app only once
    try:
        if not firebase_admin._apps:
            cred_dict = json.loads(creds_json)
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
        diagnostics["firebase_admin"] = "Initialized"
    except Exception as e:
        diagnostics["firebase_admin_error"] = str(e)
        diagnostics["traceback"] = traceback.format_exc()
        return diagnostics

    # 3. Try to access Firestore (read)
    try:
        db = firestore.client()
        docs = list(db.collection("inddJobs").limit(1).stream())
        diagnostics["firestore_read"] = f"Success. inddJobs has {len(docs)} document(s)."
    except Exception as e:
        diagnostics["firestore_read_error"] = str(e)
        diagnostics["traceback"] = traceback.format_exc()
        return diagnostics

    # 4. Try to write a test doc
    try:
        test_ref = db.collection("testConnectivity").document("render_test")
        test_ref.set({"alive": True})
        diagnostics["firestore_write"] = "Success. Wrote to testConnectivity/render_test."
    except Exception as e:
        diagnostics["firestore_write_error"] = str(e)
        diagnostics["traceback"] = traceback.format_exc()
        return diagnostics

    diagnostics["result"] = "All Firestore connectivity checks passed! 🎉"
    return diagnostics
