from fastapi import FastAPI, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import shutil, os, uuid, json
import boto3
from botocore.exceptions import NoCredentialsError

# === S3 CONFIG ===
S3_BUCKET = "idd-processor-bucket"  # <-- Replace with your bucket name
S3_REGION = "us-east-1"             # <-- Replace with your region
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=S3_REGION)

app = FastAPI()
UPLOAD_DIR = "uploads"
JOB_DIR = "jobs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(JOB_DIR, exist_ok=True)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def format_to_regex(format_string):
    special_chars = r"()[]{}.+*?^$|\\"  # Escape regex chars
    escaped = ""
    for char in format_string:
        if char in special_chars:
            escaped += "\\" + char
        else:
            escaped += char
    regex = escaped.replace("N", r"\d").replace("L", r"[A-Za-z]")
    return f"^{regex}$"

@app.post("/upload/")
async def upload_file(file: UploadFile, target_formats: str = Form(...), base_url: str = Form(...),
                      utm_source: str = Form(...), utm_medium: str = Form(...), utm_campaign: str = Form(...)):

    # Save file locally (optional, mainly for debugging)
    file_id = str(uuid.uuid4())
    upload_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
    with open(upload_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Convert formats to regex
    format_list = [line.strip() for line in target_formats.strip().splitlines() if line.strip()]
    regex_patterns = [format_to_regex(f) for f in format_list]

    # Prepare JSON job data
    job_data = {
        "input_file": f"uploads/{file_id}_{file.filename}",
        "output_file": f"uploads/{file_id}_processed.indd",
        "regexPatterns": regex_patterns,
        "baseURL": base_url,
        "utmParams": {
            "utm_source": utm_source,
            "utm_medium": utm_medium,
            "utm_campaign": utm_campaign
        }
    }

    job_file = os.path.join(JOB_DIR, f"{file_id}.json")
    with open(job_file, "w") as f:
        json.dump(job_data, f, indent=2)

    # Upload to S3
    try:
        s3_client.upload_file(upload_path, S3_BUCKET, f"uploads/{file_id}_{file.filename}")
        s3_client.upload_file(job_file, S3_BUCKET, f"jobs/{file_id}.json")
    except NoCredentialsError:
        return JSONResponse({"error": "AWS credentials not configured properly."}, status_code=500)

    return JSONResponse({"message": "File received and uploaded to S3. Processing will start shortly."})
