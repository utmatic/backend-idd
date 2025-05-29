import os
import uuid
import json
import shutil
import re
from fastapi import FastAPI, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# === S3 CONFIG ===
S3_BUCKET = "idd-processor-bucket"
S3_REGION = "us-east-2"  # MATCH YOUR ACTUAL BUCKET REGION
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
        print(f"[DEBUG] S3 key exists: {key}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"[DEBUG] S3 key does NOT exist: {key}")
            return False
        else:
            print(f"[DEBUG] S3 key check error for {key}: {e}")
            raise e

def generate_presigned_url(key, expiration=3600):
    try:
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": S3_BUCKET, "Key": key},
            ExpiresIn=expiration,
        )
        print(f"[DEBUG] Generated presigned URL for {key}")
        return url
    except Exception as e:
        print(f"[DEBUG] Error generating presigned URL for {key}: {e}")
        return ""

@app.post("/upload/")
async def upload_file(
    file: UploadFile,
    job_type: str = Form(...),
    target_formats: str = Form(""),
    base_url: str = Form(""),
    utm_source: str = Form(""),
    utm_medium: str = Form(""),
    utm_campaign: str = Form(""),
    document_name: str = Form("")
):
    """
    Accepts the new job_type (add_utm, add_links_only, add_links_with_utm).
    For add_utm: doesn't require target_formats or base_url.
    For add_links_only/add_links_with_utm: uses formats and base_url as before.
    Accepts document_name for custom output file name.
    """
    job_id = str(uuid.uuid4())
    filename = f"{job_id}_{file.filename}"
    upload_path = os.path.join(UPLOAD_DIR, filename)

    # Save file locally
    with open(upload_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # --- Parse target_formats only if needed
    format_list, regex_patterns = [], []
    if job_type in ["add_links_only", "add_links_with_utm"]:
        # Accept input like "PDF, IDML, TXT" or "PDF\nIDML\nTXT" or any mix
        format_list = [fmt.strip() for fmt in re.split(r'[,\\n]+', target_formats) if fmt.strip()]
        regex_patterns = [format_to_regex(fmt) for fmt in format_list]

    # Use or sanitize the custom document name, fallback to original (without extension) if empty
    orig_ext = os.path.splitext(file.filename)[1]
    if document_name:
        # Only allow safe characters for filenames
        safe_document_name = re.sub(r'[^A-Za-z0-9_\-\. ]', '_', document_name).strip()
        if not safe_document_name.lower().endswith(orig_ext.lower()):
            safe_document_name += orig_ext
    else:
        safe_document_name = f"processed_{file.filename}"

    # Prepare job data
    job_data = {
        "job_id": job_id,
        "input_file": f"uploads/{filename}",
        "output_file": f"processed/{job_id}_processed.indd",
        "report_file": f"reports/{job_id}_hyperlink_report.txt",
        "job_type": job_type,
        "regexPatterns": regex_patterns,
        "baseURL": base_url,
        "utmParams": {
            "utm_source": utm_source,
            "utm_medium": utm_medium,
            "utm_campaign": utm_campaign
        },
        "original_filename": file.filename,
        "document_name": safe_document_name
    }

    job_file = os.path.join(JOB_DIR, f"{job_id}.json")
    with open(job_file, "w") as jf:
        json.dump(job_data, jf, indent=2)

    # DEBUG PRINT: Show the job JSON that will be uploaded
    print("[DEBUG] Job JSON to be uploaded:")
    print(json.dumps(job_data, indent=2))

    # Upload to S3
    try:
        s3_client.upload_file(upload_path, S3_BUCKET, job_data["input_file"])
        print(f"[DEBUG] Uploaded input file to S3: {job_data['input_file']}")
        s3_client.upload_file(job_file, S3_BUCKET, f"jobs/{job_id}.json")
        print(f"[DEBUG] Uploaded job JSON to S3: jobs/{job_id}.json")
    except NoCredentialsError:
        print("[DEBUG] AWS credentials not configured properly.")
        return JSONResponse({"error": "AWS credentials not configured properly."}, status_code=500)
    except Exception as e:
        print(f"[DEBUG] Error uploading to S3: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

    # Return job ID so client can poll for results
    return JSONResponse({
        "message": "File received and uploaded to S3. Processing will start shortly.",
        "job_id": job_id,
        "job_json": job_data  # For extra debug info
    })

@app.get("/job_status/{job_id}")
def job_status(job_id: str):
    # Fetch job metadata from local
    job_path = os.path.join(JOB_DIR, f"{job_id}.json")
    if not os.path.exists(job_path):
        # fallback: try from S3
        try:
            job_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=f"jobs/{job_id}.json")
            job_data = json.loads(job_obj['Body'].read())
            print("[DEBUG] Loaded job JSON from S3 for job_status:")
            print(json.dumps(job_data, indent=2))
        except Exception as e:
            print(f"[DEBUG] Could not load job JSON for job_status: {e}")
            return JSONResponse({"error": "Job not found"}, status_code=404)
    else:
        with open(job_path, "r") as jf:
            job_data = json.load(jf)
            print("[DEBUG] Loaded job JSON from local disk for job_status:")
            print(json.dumps(job_data, indent=2))

    document_name = job_data.get("document_name", f"{job_id}_processed.indd")
    processed_key = f"processed/{document_name}"
    report_key = job_data.get("report_file", f"reports/{job_id}_hyperlink_report.txt")

    status = {
        "job_id": job_id,
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

    print(f"[DEBUG] job_status response: {json.dumps(status, indent=2)}")
    return JSONResponse(status)

@app.get("/download/{job_id}/{filetype}")
def download_file(job_id: str, filetype: str):
    # Fetch the job file to get the chosen document name
    try:
        job_key = f"jobs/{job_id}.json"
        job_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=job_key)
        job_data = json.loads(job_obj['Body'].read())
        print("[DEBUG] Loaded job JSON from S3 for download:")
        print(json.dumps(job_data, indent=2))
    except Exception as e:
        print(f"[DEBUG] Could not load job JSON for download: {e}")
        raise HTTPException(status_code=404, detail="Job metadata not found.")

    if filetype == "processed":
        filename = job_data.get("document_name", f"{job_id}_processed.indd")
        key = f"processed/{filename}"
        media_type = "application/octet-stream"
    elif filetype == "report":
        key = job_data.get("report_file", f"reports/{job_id}_hyperlink_report.txt")
        filename = os.path.basename(key)
        media_type = "text/plain"
    else:
        print(f"[DEBUG] Invalid filetype requested: {filetype}")
        raise HTTPException(status_code=404, detail="Invalid file type")

    print(f"[DEBUG] Attempting to download from S3: {key} (filename: {filename})")
    try:
        s3_response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        print(f"[DEBUG] S3 file stream acquired for {key}")
        return StreamingResponse(
            s3_response["Body"],
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            print(f"[DEBUG] S3 Key not found: {key}")
            raise HTTPException(status_code=404, detail="File not found.")
        else:
            print(f"[DEBUG] Unknown S3 error: {e}")
            raise HTTPException(status_code=500, detail="S3 error.")
