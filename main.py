import os
import uuid
import json
import shutil
from fastapi import FastAPI, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# === S3 CONFIG ===
S3_BUCKET = "idd-processor-bucket"
S3_REGION = "us-east-1"
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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

def get_presigned_url(key, expires=3600):
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET, 'Key': key},
            ExpiresIn=expires
        )
        return url
    except Exception as e:
        return ""

@app.post("/upload/")
async def upload_file(
    file: UploadFile,
    target_formats: str = Form(...),
    base_url: str = Form(...),
    utm_source: str = Form(...),
    utm_medium: str = Form(...),
    utm_campaign: str = Form(...)
):
    job_id = str(uuid.uuid4())
    filename = f"{job_id}_{file.filename}"
    upload_path = os.path.join(UPLOAD_DIR, filename)

    # Save file locally
    with open(upload_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Convert formats to regex
    format_list = [line.strip() for line in target_formats.strip().splitlines() if line.strip()]
    regex_patterns = [format_to_regex(fmt) for fmt in format_list]

    # Prepare job data
    job_data = {
        "job_id": job_id,
        "input_file": f"uploads/{filename}",
        "output_file": f"processed/{job_id}_processed.indd",
        "report_file": f"reports/{job_id}_hyperlink_report.txt",
        "regexPatterns": regex_patterns,
        "baseURL": base_url,
        "utmParams": {
            "utm_source": utm_source,
            "utm_medium": utm_medium,
            "utm_campaign": utm_campaign
        }
    }

    job_file = os.path.join(JOB_DIR, f"{job_id}.json")
    with open(job_file, "w") as jf:
        json.dump(job_data, jf, indent=2)

    # Upload to S3
    try:
        s3_client.upload_file(upload_path, S3_BUCKET, job_data["input_file"])
        s3_client.upload_file(job_file, S3_BUCKET, f"jobs/{job_id}.json")
    except NoCredentialsError:
        return JSONResponse({"error": "AWS credentials not configured properly."}, status_code=500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

    # Return job ID so client can poll for results
    return JSONResponse({
        "message": "File received and uploaded to S3. Processing will start shortly.",
        "job_id": job_id
    })

@app.get("/job_status/{job_id}")
def job_status(job_id: str):
    processed_key = f"processed/{job_id}_processed.indd"
    report_key = f"reports/{job_id}_hyperlink_report.txt"
    status = {
        "job_id": job_id,
        "processed_ready": False,
        "report_ready": False,
        "processed_url": "",
        "report_url": ""
    }

    if s3_key_exists(processed_key):
        status["processed_ready"] = True
        status["processed_url"] = get_presigned_url(processed_key)
    if s3_key_exists(report_key):
        status["report_ready"] = True
        status["report_url"] = get_presigned_url(report_key)

    # Optionally, add job parameters for frontend display (not strictly necessary)
    return JSONResponse(status)
