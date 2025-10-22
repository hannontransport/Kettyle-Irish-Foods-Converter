import os
import sys
import time
import logging
from datetime import datetime
import ftplib
import pandas as pd
import xml.etree.ElementTree as ET
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging.handlers import TimedRotatingFileHandler
from config import Config

WATCH_FOLDER = Config.WATCH_FOLDER
PROCESSED_FOLDER = Config.PROCESSED_FOLDER
ERROR_FOLDER = Config.ERROR_FOLDER
FTP_HOST = Config.FTP_HOST
FTP_PORT = Config.FTP_PORT
FTP_USERNAME = Config.FTP_USERNAME
FTP_PASSWORD = Config.FTP_PASSWORD
DOWNLOAD_FOLDER = Config.DOWNLOAD_FOLDER
UPLOAD_FOLDER = Config.UPLOAD_FOLDER
POLL_TIME = Config.POLL_TIME
SMTP_SERVER = Config.SMTP_SERVER
SMTP_PORT = Config.SMTP_PORT
SMTP_USERNAME = Config.SMTP_USERNAME
SMTP_PASSWORD = Config.SMTP_PASSWORD
FROM_EMAIL = Config.FROM_EMAIL
TO_EMAIL = Config.TO_EMAIL
COLUMNS_FILE = Config.COLUMNS_FILE

def setup_logger():
    if not os.path.exists("logs"):
        os.makedirs("logs")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = TimedRotatingFileHandler("logs/KettyleIrishFoodsConverter.log", when="midnight", interval=1, backupCount=30, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    logger.addHandler(handler)
    return logger

logger = setup_logger()

def send_email(subject, body):
    msg = MIMEMultipart()
    msg["From"] = FROM_EMAIL
    msg["To"] = TO_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(FROM_EMAIL, TO_EMAIL, msg.as_string())
        server.quit()
    except Exception as e:
        logger.error(f"Email send failed: {e}")

def clean_text(value):
    import datetime
    if pd.isna(value) or str(value).strip() in ["", "nan", "NaT", "None", "#N/A"]:
        return ""
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.strftime("%Y-%m-%d")
    val = str(value).strip()
    if val.endswith(".0") and val.replace(".", "", 1).isdigit():
        val = val[:-2]
    return val

def load_mapping(csv_path):
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    df.columns = df.columns.str.strip().str.lower()
    mappings = {}
    for _, row in df.iterrows():
        section = str(row.get("section", "")).strip().lower()
        tag = str(row.get("tag", "")).strip()
        source = str(row.get("source", "")).strip().upper()
        matchmode = str(row.get("matchmode", "")).strip()
        if section and tag and source:
            if section not in mappings:
                mappings[section] = []
            mappings[section].append({"tag": tag, "source": source, "matchmode": matchmode})
    return mappings

def indent(elem, level=0):
    i = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        for child in elem:
            indent(child, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def write_xml(filepath, output_xml, mapping_csv=COLUMNS_FILE):
    import openpyxl, re
    from difflib import get_close_matches
    mappings = load_mapping(mapping_csv)
    df = pd.read_excel(filepath, sheet_name=0, engine="openpyxl", header=3)
    df = df.replace(r"^\s*$", "", regex=True).fillna("")
    df = df[~(df.applymap(lambda x: str(x).strip() == "").all(axis=1))]
    def normalize(h): return re.sub(r"[^A-Z0-9]", "", str(h).upper())
    df.columns = [normalize(c) for c in df.columns]
    wb = openpyxl.load_workbook(filepath, data_only=True)
    ws = wb.active

    root = ET.Element("transportbookings")
    booking_el = ET.SubElement(root, "transportbooking")

    for m in mappings.get("header", []):
        tag = m["tag"]
        src = m["source"].upper()
        mm = m.get("matchmode", "")
        val = ""
        if src.startswith("CELL"):
            cell_ref = src.split()[-1]
            try:
                val = clean_text(ws[cell_ref].value)
            except:
                val = ""
        else:
            val = clean_text(src)
        if val:
            attrib = {"matchmode": mm} if mm else {}
            ET.SubElement(booking_el, tag, attrib).text = val

    shipments_el = ET.SubElement(booking_el, "shipments")

    shipment_ref_col = None
    ref_map = next((m for m in mappings.get("shipment", []) if m["tag"].lower() == "reference"), None)
    if ref_map:
        src = ref_map["source"].upper()
        if src.startswith("COLUMN"):
            col_letter = src.split()[-1].strip()
            col_idx = ord(col_letter) - ord("A")
            shipment_ref_col = df.columns[col_idx] if len(df.columns) > col_idx else None
        else:
            shipment_ref_col = normalize(src)

    key_columns = [c for c in ["COLLECTIONREFERENCE", "DELIVERYREFERENCE", "GOODSDESCRIPTION"] if c in df.columns]
    df_valid = df[df[key_columns].apply(lambda r: any(clean_text(v) for v in r), axis=1)]

    for _, row in df_valid.iterrows():
        shipment_el = ET.SubElement(shipments_el, "shipment")
        if shipment_ref_col:
            ref_val = clean_text(row.get(shipment_ref_col, ""))
            if ref_val:
                ET.SubElement(shipment_el, "reference").text = ref_val

        pickup_el = ET.SubElement(shipment_el, "pickupaddress")
        for m in mappings.get("pickup", []):
            val = clean_text(row.get(m["source"], ""))
            if val:
                mm = m.get("matchmode", "")
                attrib = {"matchmode": mm} if mm else {}
                ET.SubElement(pickup_el, m["tag"], attrib).text = val

        delivery_el = ET.SubElement(shipment_el, "deliveryaddress")
        for m in mappings.get("delivery", []):
            val = clean_text(row.get(m["source"], ""))
            if val:
                mm = m.get("matchmode", "")
                attrib = {"matchmode": mm} if mm else {}
                ET.SubElement(delivery_el, m["tag"], attrib).text = val

        cargo_el = ET.SubElement(shipment_el, "cargo")
        unitid_added = False
        for m in mappings.get("cargo", []):
            val = clean_text(row.get(m["source"], ""))
            if not val:
                continue
            mm = m.get("matchmode", "")
            attrib = {"matchmode": mm} if mm else {}
            if m["tag"].lower() == "unitamount" and not unitid_added:
                unitid_added = True
                uid_map = next((c for c in mappings.get("cargo", []) if c["tag"].lower() == "unitid"), None)
                if uid_map:
                    uid_mm = uid_map.get("matchmode", "")
                    uid_attrib = {"matchmode": uid_mm} if uid_mm else {}
                    ET.SubElement(cargo_el, "unitid", uid_attrib).text = "EuroPallet"
            ET.SubElement(cargo_el, m["tag"], attrib).text = val

    indent(root)
    ET.ElementTree(root).write(output_xml, encoding="utf-8", xml_declaration=True)
    logger.info(f"XML written successfully: {output_xml}")

def list_xlsx_files(ftp, directory):
    files = []
    def parse_line(line):
        parts = line.split(";")
        if len(parts) > 1:
            filename = parts[-1].strip()
            if filename.lower().endswith(".xlsx"):
                files.append(filename)
    try:
        ftp.cwd(directory)
        ftp.retrlines("MLSD", parse_line)
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        send_email("Kettyle EDI - File Listing Failed", "Error listing files on FTP.")
    return files

def move_file(ftp, from_path, to_directory):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.basename(from_path)
        new_name = f"{timestamp}_{filename}"
        ftp.rename(from_path, f"{to_directory}/{new_name}")
    except Exception as e:
        logger.error(f"Error moving file {from_path}: {str(e)}")
        send_email("Kettyle EDI - File Move Failed", f"Could not move file {filename}.")

def download_file(ftp, remote_path, local_path):
    try:
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_path}", f.write)
    except Exception as e:
        logger.error(f"Error downloading {remote_path}: {str(e)}")
        send_email("Kettyle EDI - File Download Failed", f"Could not download {remote_path}.")
        raise

def upload_file(ftp, local_path, remote_path):
    try:
        with open(local_path, "rb") as f:
            ftp.storbinary(f"STOR {remote_path}", f)
    except Exception as e:
        logger.error(f"Error uploading {local_path}: {str(e)}")
        send_email("Kettyle EDI - File Upload Failed", f"Could not upload {local_path}.")

def main():
    previous_files = []
    while True:
        try:
            ftp = ftplib.FTP()
            ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
            ftp.login(FTP_USERNAME, FTP_PASSWORD)
            ftp.set_pasv(True)
            current_files = list_xlsx_files(ftp, WATCH_FOLDER)
            new_files = [f for f in current_files if f not in previous_files]
            if new_files:
                logger.info(f"New files detected: {new_files}")
                for file in new_files:
                    try:
                        remote_path = f"{WATCH_FOLDER}/{file}"
                        local_path = f"{DOWNLOAD_FOLDER}/{file}"
                        download_file(ftp, remote_path, local_path)
                        xml_output = os.path.splitext(local_path)[0] + ".xml"
                        write_xml(local_path, xml_output)
                        upload_file(ftp, xml_output, f"{UPLOAD_FOLDER}/{os.path.basename(xml_output)}")
                        move_file(ftp, remote_path, PROCESSED_FOLDER)
                    except Exception as e:
                        move_file(ftp, remote_path, ERROR_FOLDER)
                        logger.error(f"Processing stopped due to error: {str(e)}")
                        break
            else:
                logger.info("No new files detected.")
            previous_files = current_files
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            send_email("Kettyle EDI - Unexpected Error", "Unexpected error. Check logs for details.")
        finally:
            try:
                ftp.quit()
            except:
                pass
        time.sleep(POLL_TIME)

if __name__ == "__main__":
    main()
