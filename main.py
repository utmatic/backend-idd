from fastapi import FastAPI, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import shutil, os, uuid, json

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

    # Save file
    file_id = str(uuid.uuid4())
    upload_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
    with open(upload_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

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

    return JSONResponse({"message": "File received. Processing will start shortly."})
