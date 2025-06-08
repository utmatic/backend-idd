import os
import json
import shutil
import re
from fastapi import FastAPI, UploadFile, Form, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import firebase_admin
from firebase_admin import credentials, firestore, auth as firebase_auth
from datetime import datetime, timezone, timedelta

# === FIREBASE ADMIN INIT (USE SINGLE ENV VARIABLE) ===
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
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://app.utmatic.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
JOB_DIR = "jobs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(JOB_DIR, exist_ok=True)

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

def s3_object_url(key):
    return f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{key}"

def get_unique_s3_key(directory, filename):
    name, ext = os.path.splitext(filename)
    candidate = filename
    n = 1
    while s3_key_exists(f"{directory}/{candidate}"):
        candidate = f"{name}_{n}{ext}"
        n += 1
    return candidate

def strip_extension(filename):
    base, ext = os.path.splitext(filename)
    if base.endswith('.'):
        base = base[:-1]
    return base

def get_client_ip(request: Request):
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host

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
    if "userId" not in job_data or not job_data["userId"]:
        job_data["userId"] = job_data.get("user_id") or "unknown"
    db.collection('inddJobs').document(str(doc_id)).set({
        **job_data,
        'completedAt': firestore.SERVER_TIMESTAMP
    })

def extract_link_count_from_report(report_file_path, job_type):
    try:
        with open(report_file_path, "r", encoding="utf-8") as f:
            for line in f:
                if job_type in ("add_links_only", "add_links_with_utm", "links_only", "links_and_utm"):
                    match = re.search(r"Total hyperlinks created:\s*(\d+)", line)
                    if match:
                        return int(match.group(1))
                elif job_type in ("add_utm", "utm_only"):
                    match = re.search(r"Hyperlinks updated with UTM parameters:\s*(\d+)", line)
                    if match:
                        return int(match.group(1))
        return 0
    except Exception as e:
        print(f"Error extracting link count from report: {e}")
        return 0

def get_user_session_ref(user_id):
    return db.collection("activeSessions").document(user_id)

def get_active_job_ref(user_id):
    return db.collection("activeJobs").document(user_id)

def enforce_single_ip(request: Request, user_id: str):
    client_ip = get_client_ip(request)
    session_ref = get_user_session_ref(user_id)
    session_doc = session_ref.get()
    if session_doc.exists:
        data = session_doc.to_dict()
        stored_ip = data.get("ip")
        if stored_ip and stored_ip != client_ip:
            raise HTTPException(status_code=403, detail="User already logged in from another IP.")
    session_ref.set({"ip": client_ip, "lastActive": firestore.SERVER_TIMESTAMP})

def enforce_single_active_job(user_id: str):
    job_ref = get_active_job_ref(user_id)
    transaction = db.transaction()

    @firestore.transactional
    def transaction_func(transaction):
        job_doc = job_ref.get(transaction=transaction)
        if job_doc.exists and job_doc.to_dict().get("active"):
            started_at = job_doc.to_dict().get("startedAt")
            if started_at:
                if hasattr(started_at, "timestamp"):
                    started_at_dt = datetime.fromtimestamp(started_at.timestamp(), tz=timezone.utc)
                elif isinstance(started_at, datetime):
                    started_at_dt = started_at
                else:
                    started_at_dt = datetime.fromisoformat(str(started_at))
                now = datetime.now(timezone.utc)
                if now - started_at_dt < timedelta(minutes=10):
                    raise HTTPException(
                        status_code=429,
                        detail="You already have an active job processing. Please wait until it completes (or 10 minutes timeout)."
                    )
        transaction.set(job_ref, {"active": True, "startedAt": firestore.SERVER_TIMESTAMP})

    transaction_func(transaction)

def clear_active_job(user_id: str):
    job_ref = get_active_job_ref(user_id)
    job_ref.delete()

def strip_firestore_specials(d):
    # Remove or convert non-serializable Firestore values from dict
    out = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = strip_firestore_specials(v)
        elif "google.cloud.firestore" in str(type(v)):
            continue
        else:
            out[k] = v
    return out

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
    try:
        user_id = get_current_user(request)
        enforce_single_ip(request, user_id)
        enforce_single_active_job(user_id)

        unique_filename = get_unique_s3_key("uploads", file.filename)
        upload_path = os.path.join(UPLOAD_DIR, unique_filename)

        with open(upload_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        format_list, regex_patterns = [], []
        if job_type in ["add_links_only", "add_links_with_utm"]:
            format_list = [fmt.strip() for fmt in re.split(r'[,\\n]+', target_formats) if fmt.strip()]
            regex_patterns = [format_to_regex(fmt) for fmt in format_list]

        base_filename = strip_extension(unique_filename)
        submitted_at = firestore.SERVER_TIMESTAMP

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
            "jobId": base_filename,
            "userId": user_id,
            "status": "queued",
            "submittedAt": submitted_at
        }

        # --- PATCH: Remove Firestore special values for JSON file ---
        job_data_for_file = strip_firestore_specials(job_data)
        job_file = os.path.join(JOB_DIR, f"{unique_filename}.json")
        with open(job_file, "w") as jf:
            json.dump(job_data_for_file, jf, indent=2)

        db.collection('inddJobs').document(str(base_filename)).set(job_data)

        try:
            s3_client.upload_file(upload_path, S3_BUCKET, job_data["input_file"])
            s3_client.upload_file(job_file, S3_BUCKET, f"jobs/{unique_filename}.json")
        except NoCredentialsError:
            return JSONResponse({"error": "AWS credentials not configured properly."}, status_code=500)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

        local_report_file_path = os.path.join(os.path.dirname(upload_path), f"{base_filename}_report.txt")
        link_count = 0
        if os.path.exists(local_report_file_path):
            link_count = extract_link_count_from_report(local_report_file_path, job_type)
            job_data["link_count"] = link_count
            try:
                s3_client.upload_file(local_report_file_path, S3_BUCKET, job_data["report_file"])
            except Exception as e:
                print(f"Error uploading report file to S3: {e}")
        else:
            job_data["link_count"] = 0

        processed_url = s3_object_url(job_data["output_file"])
        report_url = s3_object_url(job_data["report_file"])
        job_data["processed_url"] = processed_url
        job_data["report_url"] = report_url

        try:
            save_job_to_firestore(job_data)
        except Exception as e:
            print(f"Error saving job to Firestore: {e}")

        return JSONResponse({
            "message": "File received and uploaded to S3. Processing will start shortly.",
            "file_name": base_filename,
            "link_count": link_count,
            "job_id": base_filename
        })
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"Error in /upload/: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/job_status/{job_id}")
def job_status(job_id: str):
    job_ref = db.collection('inddJobs').document(job_id)
    job_doc = job_ref.get()
    if not job_doc.exists:
        raise HTTPException(status_code=404, detail="Job not found")
    job = job_doc.to_dict()
    user_id = job.get("userId")
    status = job.get("status", "queued")
    submitted_at = job.get("submittedAt")

    processed_key = f"processed/{job_id}_processed.indd"
    report_key = f"reports/{job_id}_report.txt"

    processed_ready = s3_key_exists(processed_key)
    report_ready = s3_key_exists(report_key)
    processed_url = s3_object_url(processed_key) if processed_ready else ""
    report_url = s3_object_url(report_key) if report_ready else ""

    queue_position = None
    if status in ("queued", "processing"):
        jobs_ref = db.collection('inddJobs').where("status", "in", ["queued", "processing"])
        jobs_query = jobs_ref.stream()
        jobs_list = []
        for doc in jobs_query:
            d = doc.to_dict()
            ts = d.get("submittedAt")
            if ts is None:
                dt = datetime(2000,1,1)
            elif hasattr(ts, "timestamp"):
                dt = datetime.fromtimestamp(ts.timestamp())
            elif isinstance(ts, datetime):
                dt = ts
            else:
                try:
                    dt = datetime.fromisoformat(str(ts))
                except Exception:
                    dt = datetime(2000,1,1)
            jobs_list.append((doc.id, dt))
        jobs_list.sort(key=lambda tup: tup[1])
        job_ids_in_queue = [jid for jid, _ in jobs_list]
        if job_id in job_ids_in_queue:
            queue_position = job_ids_in_queue.index(job_id) + 1

    return JSONResponse({
        "file_name": job_id,
        "status": status,
        "queue_position": queue_position,
        "processed_ready": processed_ready,
        "report_ready": report_ready,
        "processed_url": processed_url,
        "report_url": report_url
    })

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

from fastapi.encoders import jsonable_encoder

@app.get("/jobs")
def list_jobs(request: Request, user_id: str = Depends(get_current_user)):
    indd_jobs_ref = db.collection('inddJobs').where("userId", "==", user_id).order_by("completedAt", direction=firestore.Query.DESCENDING).limit(50)
    indd_docs = indd_jobs_ref.stream()
    indd_jobs = []
    for doc in indd_docs:
        data = doc.to_dict()
        completed_at = data.get("completedAt")
        if completed_at:
            try:
                date_str = completed_at.isoformat()
            except Exception:
                date_str = str(completed_at)
        else:
            date_str = ""
        indd_jobs.append({
            "date": date_str,
            "filetype": "INDD",
            "document": data.get("file_name", ""),
            "jobtype": data.get("job_type", ""),
            "processedUrl": data.get("processed_url", ""),
            "changelogUrl": data.get("report_url", ""),
            "linkCount": data.get("link_count", 0),
        })
    pdf_jobs_ref = db.collection('pdfJobs').where("user_uid", "==", user_id)
    pdf_docs = pdf_jobs_ref.stream()
    pdf_jobs = []
    for doc in pdf_docs:
        data = doc.to_dict()
        completed_at = data.get("completedAt")
        if completed_at:
            try:
                date_str = completed_at.isoformat()
            except Exception:
                date_str = str(completed_at)
        else:
            date_str = ""
        pdf_jobs.append({
            "date": date_str,
            "filetype": "PDF",
            "document": data.get("file_name", ""),
            "jobtype": data.get("job_type", ""),
            "processedUrl": data.get("processed_url", ""),
            "changelogUrl": data.get("report_url", ""),
            "linkCount": data.get("link_count", 0),
        })
    all_jobs = indd_jobs + pdf_jobs
    all_jobs_sorted = sorted(all_jobs, key=lambda x: x["date"] or "", reverse=True)
    return JSONResponse(content=jsonable_encoder(all_jobs_sorted))

@app.post("/patch_legacy_jobs")
def patch_legacy_jobs(request: Request, user_id: str = Depends(get_current_user)):
    updated = 0
    jobs_ref = db.collection('inddJobs')
    docs = jobs_ref.stream()
    for doc in docs:
        data = doc.to_dict()
        if "userId" not in data or not data["userId"]:
            doc.reference.update({"userId": user_id})
            updated += 1
    return JSONResponse({"patched_docs": updated})

@app.delete("/jobs/{jobtype}/{jobid}")
def delete_job(jobtype: str, jobid: str, request: Request, user_id: str = Depends(get_current_user)):
    jobtype = jobtype.lower()
    if jobtype == "indd":
        collection = db.collection('inddJobs')
        user_field = "userId"
    elif jobtype == "pdf":
        collection = db.collection('pdfJobs')
        user_field = "user_uid"
    else:
        raise HTTPException(status_code=400, detail="Invalid job type")

    doc_ref = collection.document(jobid)
    doc = doc_ref.get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Job not found")
    data = doc.to_dict()
    if data.get(user_field) != user_id:
        raise HTTPException(status_code=403, detail="Not authorized to delete this job")
    doc_ref.delete()
    return JSONResponse({"message": "Job deleted successfully"})
