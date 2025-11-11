import os
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

# --- config ---
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

# defaults like your original
DEFAULT_MATCHMODE_RULES = {
    "customer_id": "1",
    "address_id": "1",
    "city_id": "4",
    "product_id": "1",
    "unit_id": "1",
    "country_id": "2"
}

# ------------------------------------------------
# logging
# ------------------------------------------------
def setup_logger():
    if not os.path.exists("logs"):
        os.makedirs("logs")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = TimedRotatingFileHandler(
        "logs/KettyleIrishFoodsConverter.log",
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(console)

    return logger

logger = setup_logger()

# ------------------------------------------------
# email
# ------------------------------------------------
def send_email(subject, body):
    try:
        msg = MIMEMultipart()
        msg["From"] = FROM_EMAIL
        msg["To"] = TO_EMAIL
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(FROM_EMAIL, TO_EMAIL, msg.as_string())
        server.quit()
    except Exception as e:
        logger.error(f"Email send failed: {e}", exc_info=True)

# ------------------------------------------------
# helpers
# ------------------------------------------------
def clean_text(val):
    """Return a trimmed string or '' – this is where we kill vlookup blanks."""
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if s in ("", "nan", "None", "NaT", "#N/A"):
        return ""
    return s

def load_column_mapping(path=COLUMNS_FILE):
    """
    Load mapping like your working (koberg) version:
    section, column_index, xml_tag, matchmode, row (optional)
    """
    logger.debug(f"Loading column mapping from {path}")
    df = pd.read_csv(path, encoding="utf-8-sig").fillna("")
    df.columns = df.columns.str.strip().str.lower()

    # we’ll support these sections
    mappings = {
        "header": [],
        "ref": [],
        "pickup": [],
        "delivery": [],
        "cargo": [],
        "unit_id": [],
    }
    for _, row in df.iterrows():
        sec = row.get("section", "").strip().lower()
        if sec not in mappings:
            continue

        # get column index (must be int)
        try:
            col_idx = int(float(row.get("column_index", 0)))
        except Exception:
            continue

        item = {
            "col": col_idx,
            "tag": str(row.get("xml_tag", "")).strip(),
        }

        # optional row
        if row.get("row", ""):
            try:
                item["row"] = int(float(row["row"]))
            except Exception:
                pass

        # optional matchmode
        mm = str(row.get("matchmode", "")).strip()
        if mm:
            item["matchmode"] = mm

        mappings[sec].append(item)

    logger.debug(f"Loaded mapping sections: { {k: len(v) for k,v in mappings.items()} }")
    return mappings

def get_attrib(item):
    tag = item.get("tag", "").lower()
    # CSV wins
    mm = item.get("matchmode")
    if not mm and tag in DEFAULT_MATCHMODE_RULES:
        mm = DEFAULT_MATCHMODE_RULES[tag]
    return {"matchmode": mm} if mm else {}

# ------------------------------------------------
# XML writer (index-based like your working one)
# ------------------------------------------------
def write_xml(filepath, output_xml, mapping_csv=COLUMNS_FILE):
    logger.info(f"Converting Excel -> XML for {filepath}")
    # read raw excel – no header
    df = pd.read_excel(filepath, header=None, engine="openpyxl")
    logger.debug(f"Excel shape: {df.shape}")

    mappings = load_column_mapping(mapping_csv)

    # make sure invalid columns are removed from mapping
    max_cols = df.shape[1]
    for sec, entries in mappings.items():
        valid = [e for e in entries if 0 <= e["col"] < max_cols]
        mappings[sec] = valid

    root = ET.Element("transportbookings")
    booking_el = ET.SubElement(root, "transportbooking")

    # HEADER (usually fixed rows, e.g. row 0 or row 1)
    for item in mappings["header"]:
        row_idx = item.get("row", 0)
        val = ""
        try:
            val = clean_text(df.iat[row_idx, item["col"]])
        except Exception:
            val = ""
        if not val:
            continue
        attrib = get_attrib(item)
        ET.SubElement(booking_el, item["tag"], attrib).text = val
        logger.debug(f"header: {item['tag']} = {val}")

    shipments_el = ET.SubElement(booking_el, "shipments")

    # rows start at 2 in your working script – keep the same
    for i in range(2, len(df)):
        row = df.iloc[i]

        # find a reference for this row
        ref_val = ""
        for item in mappings.get("ref", []):
            cand = clean_text(row[item["col"]])
            if cand and cand.lower() != "shipment number":
                ref_val = cand
                break

        # gather all possible values for this row from pickup/delivery/cargo/unit
        # so we can skip truly empty rows (your “ignore anything that has no text” bit)
        row_has_data = False

        # quick probe
        for sec_name in ("pickup", "delivery", "cargo", "unit_id"):
            for item in mappings.get(sec_name, []):
                if clean_text(row[item["col"]]):
                    row_has_data = True
                    break
            if row_has_data:
                break

        # if no ref AND no data, skip
        if not ref_val and not row_has_data:
            logger.debug(f"Row {i}: empty after cleaning, skipping.")
            continue

        shipment_el = ET.SubElement(shipments_el, "shipment")

        # write ref even if blank (but usually we have one)
        ET.SubElement(shipment_el, "edireference").text = ref_val
        ET.SubElement(shipment_el, "reference").text = ref_val
        logger.debug(f"Row {i}: created shipment, ref='{ref_val}'")

        # pickup
        pickup_el = ET.SubElement(shipment_el, "pickupaddress")
        for item in mappings["pickup"]:
            val = clean_text(row[item["col"]])
            if not val:
                continue
            attrib = get_attrib(item)
            ET.SubElement(pickup_el, item["tag"], attrib).text = val
            logger.debug(f"Row {i}: pickup {item['tag']} = {val}")

        # delivery
        delivery_el = ET.SubElement(shipment_el, "deliveryaddress")
        for item in mappings["delivery"]:
            val = clean_text(row[item["col"]])
            if not val:
                continue
            attrib = get_attrib(item)
            ET.SubElement(delivery_el, item["tag"], attrib).text = val
            logger.debug(f"Row {i}: delivery {item['tag']} = {val}")

        # cargo
        cargo_el = ET.SubElement(shipment_el, "cargo")
        for item in mappings["cargo"]:
            val = clean_text(row[item["col"]])
            if not val:
                continue
            attrib = get_attrib(item)
            ET.SubElement(cargo_el, item["tag"], attrib).text = val
            logger.debug(f"Row {i}: cargo {item['tag']} = {val}")

        # unit(s)
        unit_entries = [item for item in mappings.get("unit_id", []) if clean_text(row[item["col"]])]
        if len(unit_entries) == 1:
            item = unit_entries[0]
            ET.SubElement(cargo_el, "unit_id", {"matchmode": "1"}).text = item["tag"]
            ET.SubElement(cargo_el, "unitamount").text = clean_text(row[item["col"]])
            logger.debug(f"Row {i}: single unit {item['tag']} = {clean_text(row[item['col']])}")
        elif len(unit_entries) > 1:
            gls = ET.SubElement(cargo_el, "goodslines")
            for item in unit_entries:
                gl = ET.SubElement(gls, "goodsline")
                ET.SubElement(gl, "unit_id", {"matchmode": "1"}).text = item["tag"]
                ET.SubElement(gl, "unitamount").text = clean_text(row[item["col"]])
                logger.debug(f"Row {i}: goodsline unit {item['tag']} = {clean_text(row[item['col']])}")

    # write xml
    ET.ElementTree(root).write(output_xml, encoding="utf-8", xml_declaration=True)
    logger.info(f"XML written successfully: {output_xml}")

# ------------------------------------------------
# FTP helpers
# ------------------------------------------------
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
        logger.error(f"MLSD failed, falling back to NLST: {e}")
        files = [f for f in ftp.nlst() if f.lower().endswith(".xlsx")]

    return files

def move_file(ftp, from_path, to_directory):
    filename = os.path.basename(from_path)
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"{timestamp}_{filename}"
        ftp.rename(from_path, f"{to_directory}/{new_name}")
    except Exception as e:
        logger.error(f"Error moving file {from_path}: {e}", exc_info=True)
        send_email("Kettyle EDI - File Move Failed", f"Could not move file {filename}.")

def download_file(ftp, remote_path, local_path):
    try:
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_path}", f.write)
    except Exception as e:
        logger.error(f"Error downloading {remote_path}: {e}", exc_info=True)
        send_email("Kettyle EDI - File Download Failed", f"Could not download {remote_path}.")
        raise

def upload_file(ftp, local_path, remote_path):
    try:
        with open(local_path, "rb") as f:
            ftp.storbinary(f"STOR {remote_path}", f)
    except Exception as e:
        logger.error(f"Error uploading {local_path}: {e}", exc_info=True)
        send_email("Kettyle EDI - File Upload Failed", f"Could not upload {local_path}.")

# ------------------------------------------------
# main loop
# ------------------------------------------------
def main():
    previous_files = []
    while True:
        ftp = None
        try:
            logger.debug(f"Connecting to FTP {FTP_HOST}:{FTP_PORT} ...")
            ftp = ftplib.FTP()
            ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
            ftp.login(FTP_USERNAME, FTP_PASSWORD)
            ftp.set_pasv(True)

            current_files = list_xlsx_files(ftp, WATCH_FOLDER)
            new_files = [f for f in current_files if f not in previous_files]

            if new_files:
                logger.info(f"New files detected: {new_files}")
                for file in new_files:
                    remote_path = f"{WATCH_FOLDER}/{file}"
                    local_path = f"{DOWNLOAD_FOLDER}/{file}"
                    try:
                        download_file(ftp, remote_path, local_path)
                        xml_output = os.path.splitext(local_path)[0] + ".xml"
                        write_xml(local_path, xml_output)
                        upload_file(ftp, xml_output, f"{UPLOAD_FOLDER}/{os.path.basename(xml_output)}")
                        move_file(ftp, remote_path, PROCESSED_FOLDER)
                    except Exception as e:
                        logger.error(f"Processing stopped for {file}: {e}", exc_info=True)
                        move_file(ftp, remote_path, ERROR_FOLDER)
            else:
                logger.info("No new files detected.")

            previous_files = current_files
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            send_email("Kettyle EDI - Unexpected Error", f"Unexpected error occurred: {e}")
        finally:
            try:
                if ftp:
                    ftp.quit()
            except:
                pass

        time.sleep(POLL_TIME)

if __name__ == "__main__":
    main()
