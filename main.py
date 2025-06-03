import os
import json
import shutil
import re
from fastapi import FastAPI, UploadFile, Form, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# === FIREBASE ADMIN INIT (USE SINGLE ENV VARIABLE) ===
import firebase_admin
from firebase_admin import credentials, firestore, auth as firebase_auth

def get_firebase_cred_from_json_env():
    json_str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not json_str:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON env variable not set.")
    cred_dict = json.loads(json_str)
    return credentials.Certificate(cred_dict)

cred = get_firebase_cred_from_json_env()
firebase_admin.initialize_app(cred)
db = firestore.client()

# === S3 CONFIG ===
S3_BUCKET = "idd-processor-bucket"
S3_REGION = "us-east-2"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=S3_REGION
)

app = FastAPI()
UPLOAD_DIR = "uploads"
JOB_DIR = "jobs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(JOB_DIR, exist_ok=True)

# === CORS FIX: allow only your frontend origin, not "*" ===
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://app.utmatic.com"],  # Only allow your frontend
    allow_methods=["*"],
    allow_headers=["*"],
)

def format_to_regex(format_string):
    special_chars = r"()[]{}.+*?^$|\\"
    escaped = ""
    for char in format_string:
        if char in special_chars:
            escaped += "\\" + char
        else:
            escaped += char
    regex = escaped.replace("N", r"\d").replace("L", r"[A-Za-z]")
    return f"^{regex}$"

def s3_key_exists(key):
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e

def generate_presigned_url(key, expiration=3600):
    try:
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": S3_BUCKET, "Key": key},
            ExpiresIn=expiration,
        )
        return url
    except Exception as e:
        print(f"Error generating presigned URL for {key}: {e}")
        return ""

def get_unique_s3_key(directory, filename):
    """
    If filename exists in S3 under directory, append _1, _2, etc. before the extension.
    """
    name, ext = os.path.splitext(filename)
    candidate = filename
    n = 1
    while s3_key_exists(f"{directory}/{candidate}"):
        candidate = f"{name}_{n}{ext}"
        n += 1
    return candidate

def strip_extension(filename):
    base, ext = os.path.splitext(filename)
    # Remove trailing dot if present (e.g., "test." becomes "test")
    if base.endswith('.'):
        base = base[:-1]
    return base

def get_current_user(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing auth token")
    id_token = auth_header.split("Bearer ")[1]
    try:
        decoded = firebase_auth.verify_id_token(id_token)
        return decoded["uid"]
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

def save_job_to_firestore(job_data):
    job_id = job_data.get('jobId') or job_data.get('file_name') or job_data.get('input_file') or "unknown"
    doc_id = job_id
    db.collection('inddJobs').document(str(doc_id)).set({
        **job_data,
        'completedAt': firestore.SERVER_TIMESTAMP
    })

@app.post("/upload/")
async def upload_file(
    request: Request,
    file: UploadFile,
    job_type: str = Form(...),
    target_formats: str = Form(""),
    base_url: str = Form(""),
    utm_source: str = Form(""),
    utm_medium: str = Form(""),
    utm_campaign: str = Form("")
):
    user_id = get_current_user(request)

    # Get a unique filename for S3 (avoid collisions)
    unique_filename = get_unique_s3_key("uploads", file.filename)
    upload_path = os.path.join(UPLOAD_DIR, unique_filename)

    # Save file locally
    with open(upload_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # --- Parse target_formats only if needed
    format_list, regex_patterns = [], []
    if job_type in ["add_links_only", "add_links_with_utm"]:
        format_list = [fmt.strip() for fmt in re.split(r'[,\\n]+', target_formats) if fmt.strip()]
        regex_patterns = [format_to_regex(fmt) for fmt in format_list]

    # Always strip the extension for output and report filenames
    base_filename = strip_extension(unique_filename)

    # Prepare job data
    job_data = {
        "input_file": f"uploads/{unique_filename}",
        "output_file": f"processed/{base_filename}_processed.indd",
        "report_file": f"reports/{base_filename}_report.txt",
        "job_type": job_type,
        "regexPatterns": regex_patterns,
        "baseURL": base_url,
        "utmParams": {
            "utm_source": utm_source,
            "utm_medium": utm_medium,
            "utm_campaign": utm_campaign
        },
        "file_name": base_filename,
        "jobId": base_filename,  # use base_filename as jobId for Firestore
        "userId": user_id        # store user ID
    }

    job_file = os.path.join(JOB_DIR, f"{unique_filename}.json")
    with open(job_file, "w") as jf:
        json.dump(job_data, jf, indent=2)

    # Upload to S3
    try:
        s3_client.upload_file(upload_path, S3_BUCKET, job_data["input_file"])
        s3_client.upload_file(job_file, S3_BUCKET, f"jobs/{unique_filename}.json")
    except NoCredentialsError:
        return JSONResponse({"error": "AWS credentials not configured properly."}, status_code=500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

    # --- Generate download links and save job info to Firestore ---
    processed_url = generate_presigned_url(f"processed/{base_filename}_processed.indd")
    report_url = generate_presigned_url(f"reports/{base_filename}_report.txt")
    job_data["processed_url"] = processed_url
    job_data["report_url"] = report_url

    try:
        save_job_to_firestore(job_data)
    except Exception as e:
        print(f"Error saving job to Firestore: {e}")

    return JSONResponse({
        "message": "File received and uploaded to S3. Processing will start shortly.",
        "file_name": base_filename
    })

@app.get("/job_status/{file_name}")
def job_status(file_name: str):
    processed_key = f"processed/{file_name}_processed.indd"
    report_key = f"reports/{file_name}_report.txt"
    status = {
        "file_name": file_name,
        "processed_ready": False,
        "report_ready": False,
        "processed_url": "",
        "report_url": ""
    }

    if s3_key_exists(processed_key):
        status["processed_ready"] = True
        status["processed_url"] = generate_presigned_url(processed_key)
    if s3_key_exists(report_key):
        status["report_ready"] = True
        status["report_url"] = generate_presigned_url(report_key)

    return JSONResponse(status)

@app.get("/download/{file_name}/{filetype}")
def download_file(file_name: str, filetype: str):
    base_name = strip_extension(file_name)
    if filetype == "processed":
        key = f"processed/{base_name}_processed.indd"
        filename = f"{base_name}_processed.indd"
        media_type = "application/octet-stream"
    elif filetype == "report":
        key = f"reports/{base_name}_report.txt"
        filename = f"{base_name}_report.txt"
        media_type = "text/plain"
    else:
        raise HTTPException(status_code=404, detail="Invalid file type")

    try:
        s3_response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        return StreamingResponse(
            s3_response["Body"],
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            raise HTTPException(status_code=404, detail="File not found.")
        else:
            raise HTTPException(status_code=500, detail="S3 error.")

# === Endpoint to get recent jobs for dashboard/history (per user, requires auth) ===
from fastapi.encoders import jsonable_encoder
from datetime import datetime

@app.get("/jobs")
def list_jobs(request: Request, user_id: str = Depends(get_current_user)):
    jobs_ref = db.collection('inddJobs').where("userId", "==", user_id).order_by("completedAt", direction=firestore.Query.DESCENDING).limit(50)
    docs = jobs_ref.stream()
    jobs = []
    for doc in docs:
        data = doc.to_dict()
        completed_at = data.get("completedAt")
        if completed_at:
            try:
                date_str = completed_at.isoformat()
            except Exception:
                date_str = str(completed_at)
        else:
            date_str = ""
        jobs.append({
            "date": date_str,
            "filetype": "INDD",
            "document": data.get("file_name", ""),
            "jobtype": data.get("job_type", ""),
            "processedUrl": data.get("processed_url", ""),
            "changelogUrl": data.get("report_url", ""),
        })
    return JSONResponse(content=jsonable_encoder(jobs))
