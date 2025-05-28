import os
import uuid
import json
import shutil
import fitz  # PyMuPDF
import io
import re
import urllib.parse
import unicodedata
from typing import Optional, List
import logging

from fastapi import FastAPI, File, UploadFile, Form, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from botocore.exceptions import NoCredentialsError, ClientError
import boto3

# === Logging setup ===
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("utmaticlogic")

# === S3 CONFIG ===
S3_BUCKET = "idd-processor-bucket"
S3_REGION = "us-east-2"  # CHANGE IF YOUR BUCKET IS DIFFERENT
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=S3_REGION
)

# === Local dirs for IDD job handoff ===
UPLOAD_DIR = "uploads"
JOB_DIR = "jobs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(JOB_DIR, exist_ok=True)

app = FastAPI()

# CORS for your frontend (adjust if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://app.utmatic.com",
        "https://frontend-ten-alpha-65.vercel.app",
        "https://frontend-baipfc7fn-john-snivelys-projects.vercel.app",
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Link-Count"]
)

# === PDF logic ===
def extract_last_path_segment(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.strip("/")
    return path.split("/")[-1] if path else "link"

def int_to_rgb(color_int):
    r = ((color_int >> 16) & 255) / 255
    g = ((color_int >> 8) & 255) / 255
    b = (color_int & 255) / 255
    return (r, g, b)

def normalize(s):
    return unicodedata.normalize("NFKD", s).replace("–", "-").replace("—", "-").replace("\u00A0", " ").replace("\u2011", "-").strip()

def format_pattern_to_regex(fmt_pattern: str):
    result = ""
    i = 0
    while i < len(fmt_pattern):
        c = fmt_pattern[i]
        if c in "NnLl":
            run_char = c
            run_len = 1
            while i + run_len < len(fmt_pattern) and fmt_pattern[i + run_len].lower() == run_char.lower():
                run_len += 1
            if i + run_len < len(fmt_pattern) and fmt_pattern[i + run_len] == "+":
                if run_char in "Nn":
                    result += rf"\d{{{run_len},}}"
                else:
                    result += rf"[A-Za-z]{{{run_len},}}"
                i += run_len + 1
            else:
                if run_char in "Nn":
                    result += r"\d" * run_len
                else:
                    result += r"[A-Za-z]" * run_len
                i += run_len
        else:
            result += re.escape(c)
            i += 1
    return result

def process_pdf_logic(contents, source, medium, campaign, job_type, format_map=None, underline=False, diagnostics=False):
    final_pdf = fitz.open(stream=contents, filetype="pdf")
    preview_pdf = fitz.open(stream=contents, filetype="pdf")
    red_rects = []
    link_count = 0

    utm_base = f"utm_source={urllib.parse.quote(source)}&utm_medium={urllib.parse.quote(medium)}&utm_campaign={urllib.parse.quote(campaign)}"

    for page_num in range(len(final_pdf)):
        final_page = final_pdf[page_num]
        preview_page = preview_pdf[page_num]

        if diagnostics:
            dict_page = final_page.get_text("dict")
            logger.info(f"\n--- PAGE {page_num} ---")
            for block in dict_page["blocks"]:
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        raw_text = span.get("text", "")
                        logger.info(f"[Span] Page {page_num} | '{raw_text}' | BBox: {span['bbox']}")
            words = final_page.get_text("words")
            for word in words:
                logger.info(f"[Word] Page {page_num} | '{word[4]}' | BBox: {word[:4]}")

        if job_type == "utm_only":
            links = final_page.get_links()
            for link in links:
                if "uri" in link:
                    original = link["uri"]
                    rect = fitz.Rect(link["from"])
                    visible_text = final_page.get_textbox(rect).strip()
                    if " " in visible_text:
                        utm_val = extract_last_path_segment(original)
                    else:
                        utm_val = visible_text
                    utm_content = f"&utm_content={urllib.parse.quote(utm_val)}"
                    updated_url = original + ("&" if "?" in original else "?") + utm_base + utm_content

                    final_page.delete_link(link)
                    final_page.insert_link({
                        "from": rect,
                        "uri": updated_url,
                        "kind": 2,
                        "page": final_page.number
                    })
                    preview_page.insert_link({
                        "from": rect,
                        "uri": updated_url,
                        "kind": 2,
                        "page": preview_page.number
                    })

                    red_rects.append((page_num, rect))
                    link_count += 1

        elif job_type == "links_and_utm":
            if not format_map:
                raise ValueError("Format map is required.")

            try:
                format_dict = json.loads(format_map)
            except json.JSONDecodeError:
                raise ValueError("Format map must be a valid JSON object.")

            patterns = []
            for fmt_string, url in format_dict.items():
                formats = [f.strip() for f in fmt_string.split(",")]
                for fmt in formats:
                    regex = format_pattern_to_regex(fmt)
                    patterns.append((re.compile(rf"^{regex}$", re.IGNORECASE), url))

            dict_page = final_page.get_text("dict")
            words_mode = final_page.get_text("words")

            already_linked = set()

            for word in words_mode:
                x0, y0, x1, y1, text = word[:5]
                clean_text = normalize(text)
                for pattern, matched_url in patterns:
                    if pattern.fullmatch(clean_text):
                        rect = fitz.Rect(x0, y0, x1, y1)
                        base_plus_text = f"{matched_url}{clean_text}"
                        delimiter = "&" if "?" in base_plus_text else "?"
                        full_link = f"{base_plus_text}{delimiter}utm_source={urllib.parse.quote(source)}&utm_medium={urllib.parse.quote(medium)}&utm_campaign={urllib.parse.quote(campaign)}&utm_content={urllib.parse.quote(clean_text)}"
                        if (page_num, rect) in already_linked:
                            continue
                        final_page.insert_link({
                            "from": rect,
                            "uri": full_link,
                            "kind": 2,
                            "page": final_page.number
                        })
                        preview_page.insert_link({
                            "from": rect,
                            "uri": full_link,
                            "kind": 2,
                            "page": preview_page.number
                        })

                        red_rects.append((page_num, rect))
                        already_linked.add((page_num, rect))
                        link_count += 1
                        break

            for block in dict_page["blocks"]:
                for line in block.get("lines", []):
                    span_data = [(normalize(span.get("text", "")), span["bbox"]) for span in line.get("spans", [])]
                    line_text = " ".join([sd[0] for sd in span_data])
                    for pattern, matched_url in patterns:
                        match = pattern.search(line_text)
                        if match:
                            match_str = match.group()
                            found = False
                            for span_text, bbox in span_data:
                                if match_str == span_text and len(match_str) > 0:
                                    rect = fitz.Rect(bbox)
                                    base_plus_text = f"{matched_url}{match_str}"
                                    delimiter = "&" if "?" in base_plus_text else "?"
                                    full_link = f"{base_plus_text}{delimiter}utm_source={urllib.parse.quote(source)}&utm_medium={urllib.parse.quote(medium)}&utm_campaign={urllib.parse.quote(campaign)}&utm_content={urllib.parse.quote(match_str)}"
                                    if (page_num, rect) in already_linked:
                                        continue
                                    final_page.insert_link({
                                        "from": rect,
                                        "uri": full_link,
                                        "kind": 2,
                                        "page": final_page.number
                                    })
                                    preview_page.insert_link({
                                        "from": rect,
                                        "uri": full_link,
                                        "kind": 2,
                                        "page": preview_page.number
                                    })
                                    if underline:
                                        text_color_int = 0
                                        text_color_rgb = int_to_rgb(text_color_int)
                                        font_height = bbox[3] - bbox[1]
                                        underline_y = bbox[1] + font_height * 0.9
                                        final_page.draw_line(
                                            p1=(bbox[0], underline_y),
                                            p2=(bbox[2], underline_y),
                                            color=text_color_rgb,
                                            width=0.5
                                        )
                                    red_rects.append((page_num, rect))
                                    already_linked.add((page_num, rect))
                                    link_count += 1
                                    found = True
                                    break
                            if not found:
                                match_words = match_str.split()
                                for span_text, bbox in span_data:
                                    for mword in match_words:
                                        if mword == span_text and len(mword) > 0:
                                            rect = fitz.Rect(bbox)
                                            base_plus_text = f"{matched_url}{mword}"
                                            delimiter = "&" if "?" in base_plus_text else "?"
                                            full_link = f"{base_plus_text}{delimiter}utm_source={urllib.parse.quote(source)}&utm_medium={urllib.parse.quote(medium)}&utm_campaign={urllib.parse.quote(campaign)}&utm_content={urllib.parse.quote(mword)}"
                                            if (page_num, rect) in already_linked:
                                                continue
                                            final_page.insert_link({
                                                "from": rect,
                                                "uri": full_link,
                                                "kind": 2,
                                                "page": final_page.number
                                            })
                                            preview_page.insert_link({
                                                "from": rect,
                                                "uri": full_link,
                                                "kind": 2,
                                                "page": preview_page.number
                                            })
                                            if underline:
                                                text_color_int = 0
                                                text_color_rgb = int_to_rgb(text_color_int)
                                                font_height = bbox[3] - bbox[1]
                                                underline_y = bbox[1] + font_height * 0.9
                                                final_page.draw_line(
                                                    p1=(bbox[0], underline_y),
                                                    p2=(bbox[2], underline_y),
                                                    color=text_color_rgb,
                                                    width=0.5
                                                )
                                            red_rects.append((page_num, rect))
                                            already_linked.add((page_num, rect))
                                            link_count += 1
                                            break

    return final_pdf, preview_pdf, red_rects, link_count

# === IDD logic ===
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

@app.post("/process")
async def process_file(
    file: UploadFile = File(...),
    job_type: str = Form(...),
    target_format: Optional[List[str]] = Form(None),
    base_url: Optional[List[str]] = Form(None),
    source: str = Form(...),
    medium: str = Form(...),
    campaign: str = Form(...),
    utm_content: Optional[str] = Form(None),
    filename: Optional[str] = Form(None),
    underline: Optional[str] = Form("false"),
    diagnostics: Optional[str] = Form("false")
):
    ext = file.filename.lower().rsplit(".", 1)[-1]
    if ext == "pdf":
        try:
            underline_bool = str(underline).lower() == "true"
            diagnostics_bool = str(diagnostics).lower() == "true"
            format_map = None
            if job_type == "links_and_utm":
                if not target_format or not base_url:
                    raise ValueError("target_format and base_url are required for links_and_utm jobs.")
                if len(target_format) != len(base_url):
                    raise ValueError("Mismatch between number of target_format and base_url entries.")
                format_map_dict = {tf: bu for tf, bu in zip(target_format, base_url)}
                format_map = json.dumps(format_map_dict)
            contents = await file.read()
            final_pdf, _, _, link_count = process_pdf_logic(contents, source, medium, campaign, job_type, format_map, underline_bool, diagnostics_bool)

            output_name = f"{filename.strip()}.pdf" if filename else f"{file.filename.rsplit('.', 1)[0]}_processed.pdf"

            buffer = io.BytesIO()
            final_pdf.save(buffer)
            buffer.seek(0)

            headers = {
                "X-Link-Count": str(link_count),
                "Content-Disposition": f'attachment; filename="{output_name}"'
            }
            return StreamingResponse(buffer, media_type="application/pdf", headers=headers)
        except ValueError as ve:
            return JSONResponse(status_code=400, content={"error": str(ve)})
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})
    elif ext == "indd":
        # === INDD logic: submit job, return job_id immediately, let frontend poll ===
        try:
            job_id = str(uuid.uuid4())
            filename_on_disk = f"{job_id}_{file.filename}"
            upload_path = os.path.join(UPLOAD_DIR, filename_on_disk)

            with open(upload_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)

            # Convert formats to regex
            format_list = [line.strip() for line in target_format if line.strip()] if target_format else []
            regex_patterns = [format_to_regex(fmt) for fmt in format_list]

            job_data = {
                "job_id": job_id,
                "input_file": f"uploads/{filename_on_disk}",
                "output_file": f"processed/{job_id}_processed.indd",
                "report_file": f"reports/{job_id}_hyperlink_report.txt",
                "regexPatterns": regex_patterns,
                "baseURL": base_url[0] if base_url else "",
                "utmParams": {
                    "utm_source": source,
                    "utm_medium": medium,
                    "utm_campaign": campaign
                }
            }

            job_file = os.path.join(JOB_DIR, f"{job_id}.json")
            with open(job_file, "w") as jf:
                json.dump(job_data, jf, indent=2)

            s3_client.upload_file(upload_path, S3_BUCKET, job_data["input_file"])
            s3_client.upload_file(job_file, S3_BUCKET, f"jobs/{job_id}.json")

            # Return job info for polling
            return JSONResponse({
                "message": "INDD file received and uploaded to S3. Processing will start shortly.",
                "job_id": job_id,
                "poll_url": f"/job_status/{job_id}"
            })
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": f"INDD processing failed: {e}"})
    else:
        return JSONResponse(status_code=400, content={"error": "Unsupported file type"})

@app.post("/preview")
async def process_preview_pdf(
    file: UploadFile = File(...),
    job_type: str = Form(...),
    target_format: Optional[List[str]] = Form(None),
    base_url: Optional[List[str]] = Form(None),
    source: str = Form(...),
    medium: str = Form(...),
    campaign: str = Form(...),
    utm_content: Optional[str] = Form(None),
    filename: Optional[str] = Form(None),
    underline: Optional[str] = Form("false"),
    diagnostics: Optional[str] = Form("false")
):
    ext = file.filename.lower().rsplit(".", 1)[-1]
    if ext == "pdf":
        try:
            underline_bool = str(underline).lower() == "true"
            diagnostics_bool = str(diagnostics).lower() == "true"
            format_map = None
            if job_type == "links_and_utm":
                if not target_format or not base_url:
                    raise ValueError("target_format and base_url are required for links_and_utm jobs.")
                if len(target_format) != len(base_url):
                    raise ValueError("Mismatch between number of target_format and base_url entries.")
                format_map_dict = {tf: bu for tf, bu in zip(target_format, base_url)}
                format_map = json.dumps(format_map_dict)
            contents = await file.read()
            _, preview_pdf, red_rects, link_count = process_pdf_logic(contents, source, medium, campaign, job_type, format_map, underline_bool, diagnostics_bool)

            for page_num, rect in red_rects:
                preview_pdf[page_num].draw_rect(rect, color=(1, 0, 0), width=1)

            buffer = io.BytesIO()
            preview_pdf.save(buffer)
            buffer.seek(0)

            headers = {"X-Link-Count": str(link_count)}
            return StreamingResponse(buffer, media_type="application/pdf", headers=headers)
        except ValueError as ve:
            return JSONResponse(status_code=400, content={"error": str(ve)})
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})
    elif ext == "indd":
        # No preview for INDD, just acknowledge
        return JSONResponse({
            "message": "Preview not available for INDD files. Please process the job and use the download links when ready."
        })
    else:
        return JSONResponse(status_code=400, content={"error": "Unsupported file type"})

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
        status["processed_url"] = generate_presigned_url(processed_key)
    if s3_key_exists(report_key):
        status["report_ready"] = True
        status["report_url"] = generate_presigned_url(report_key)

    return JSONResponse(status)

@app.get("/download/{job_id}/{filetype}")
def download_file(job_id: str, filetype: str):
    if filetype == "processed":
        key = f"processed/{job_id}_processed.indd"
        filename = f"{job_id}_processed.indd"
        media_type = "application/octet-stream"
    elif filetype == "report":
        key = f"reports/{job_id}_hyperlink_report.txt"
        filename = f"{job_id}_hyperlink_report.txt"
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

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/")
def root():
    return {"status": "Backend up and running!", "usage": "POST your PDF/IDD and required fields to /preview or /process"}

@app.exception_handler(Exception)
async def custom_exception_handler(request: Request, exc: Exception):
    if hasattr(exc, "status_code") and exc.status_code == 422:
        return JSONResponse({"error": "Missing or invalid fields in request."}, status_code=422)
    return JSONResponse({"error": str(exc)}, status_code=500)
