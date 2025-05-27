from fastapi import FastAPI, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import shutil, os, uuid, json
import boto3

# === S3 CONFIG ===
S3_BUCKET = "idd-processor-bucket"
S3_REGION = "us-east-1"

AWS_ACCESS_KEY = "AKIAVGQLEZSFBO2DR3IM"
AWS_SECRET_KEY = "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"

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
async def upload_file(
    file: UploadFile,
    target_formats: str = Form(...),
    base_url: str = Form(...),
    utm_source: str = Form(...),
    utm_medium: str = Form(...),
    utm_campaign: str = Form(...)
):
    # Save file
    file_id = str(uuid.uuid4())
    upload_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
    with open(upload_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Upload to S3 uploads/
    s3_client.upload_file(upload_path, S3_BUCKET, f"uploads/{file_id}_{file.filename}")

    # Convert formats to regex
    format_list = [line.strip() for line in target_formats.strip().splitlines() if line.strip()]
    regex_patterns = [format_to_regex(f) for f in format_list]

    # Prepare JSON job data
    job_data = {
        "input_file": upload_path,
        "output_file": os.path.join(UPLOAD_DIR, f"{file_id}_processed.indd"),
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

    # Upload job JSON to S3
    s3_client.upload_file(job_file, S3_BUCKET, f"jobs/{file_id}.json")

    return JSONResponse({"message": "File received and uploaded to S3. Processing will start shortly.", "job_id": file_id})

@app.get("/download/{job_id}")
async def get_download_links(job_id: str):
    try:
        indd_key = f"outputs/{job_id}_processed.indd"
        txt_key = f"outputs/{job_id}_hyperlink_report.txt"

        indd_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET, 'Key': indd_key},
            ExpiresIn=3600
        )
        txt_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET, 'Key': txt_key},
            ExpiresIn=3600
        )

        return {"indd_url": indd_url, "report_url": txt_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/")
def root():
    return {"status": "Backend up and running!", "usage": "POST your PDF and required fields to /upload"}
