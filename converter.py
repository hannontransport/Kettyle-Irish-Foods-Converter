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

# --- CONFIG ---
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

MATCHMODE_RULES = {
    "customer_id": "1",
    "address_id": "1",
    "city_id": "4",
    "product_id": "1",
    "unit_id": "1",
}


def setup_logger():
    if not os.path.exists("logs"):
        os.makedirs("logs")

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    file_handler = TimedRotatingFileHandler(
        "logs/KettyleIrishFoodsConverter.log",
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(console_handler)

    return logger


logger = setup_logger()


def send_email(subject, body):
    try:
        logger.debug(f"Sending email: {subject}")
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
    logger.debug(f"Loading mapping from: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    df.columns = df.columns.str.strip().str.lower()
    mappings = {}
    for _, row in df.iterrows():
        section = str(row.get("section", "")).strip().lower()
        tag = str(row.get("tag", "")).strip()
        source = str(row.get("source", "")).strip()
        if section and tag and source:
            mappings.setdefault(section, []).append({"tag": tag, "source": source})
    logger.debug(f"Loaded mapping sections: {list(mappings.keys())}")
    return mappings


def get_matchmode(tag):
    return MATCHMODE_RULES.get(tag.lower(), "")


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

    logger.info(f"Starting XML write for file: {filepath}")
    mappings = load_mapping(mapping_csv)

    def normalize(h):
        s = re.sub(r"[^A-Z0-9]", "", str(h).upper())
        # fix EXCEL 'adress' -> 'address'
        s = s.replace("ADRESS", "ADDRESS")
        return s

    # 1) read excel
    logger.debug("Reading Excel data (header row index = 3)...")
    df_raw = pd.read_excel(filepath, sheet_name=0, engine="openpyxl", header=3)
    logger.debug(f"Excel read complete. Raw shape: {df_raw.shape}")
    logger.debug(f"Raw columns: {list(df_raw.columns)}")

    # 2) clean / drop empty rows
    df = df_raw.replace(r"^\s*$", "", regex=True).fillna("")
    before_drop = df.shape[0]
    df = df[~(df.replace("", pd.NA).isna().all(axis=1))]
    after_drop = df.shape[0]
    logger.debug(f"Dropped empty rows: {before_drop - after_drop}. Remaining rows: {after_drop}")

    # 3) normalise column names
    df.columns = [normalize(c) for c in df.columns]
    logger.debug(f"Normalized columns: {list(df.columns)}")

    # keep original headers for unit names
    norm_to_raw = {normalize(c): str(c).strip() for c in df_raw.columns}

    # workbook for cell-based header reads
    wb = openpyxl.load_workbook(filepath, data_only=True)
    ws = wb.active

    root = ET.Element("transportbookings")
    booking_el = ET.SubElement(root, "transportbooking")
    header_reference = ""

    # ---------- HEADER ----------
    logger.debug("Building header section...")
    for m in mappings.get("header", []):
        tag = m["tag"]
        src = m["source"]
        val = ""
        if src.upper().startswith("CELL"):
            cell_ref = src.split()[-1]
            try:
                val = clean_text(ws[cell_ref].value)
                logger.debug(f"Header CELL {cell_ref} -> {tag} = '{val}'")
            except Exception as e:
                logger.error(f"Could not read cell {cell_ref} for header {tag}: {e}", exc_info=True)
        else:
            val = clean_text(src)
            logger.debug(f"Header CONST {src} -> {tag} = '{val}'")
        if val:
            mm = get_matchmode(tag)
            attrib = {"matchmode": mm} if mm else {}
            ET.SubElement(booking_el, tag, attrib).text = val
            if tag.lower() == "reference":
                header_reference = val

    if header_reference:
        ET.SubElement(booking_el, "edireference").text = header_reference
        logger.debug(f"Header reference set to: {header_reference}")

    shipments_el = ET.SubElement(booking_el, "shipments")

    # ---------- SHIPMENT REFERENCE ----------
    shipment_ref_col = None
    ref_map = next((m for m in mappings.get("shipment", []) if m["tag"].lower() == "reference"), None)
    if ref_map:
        src = ref_map["source"]
        src_upper = src.upper()
        if src_upper.startswith("COLUMN"):
            col_letter = src_upper.split()[-1].strip()
            col_idx = ord(col_letter) - ord("A")
            if len(df.columns) > col_idx:
                shipment_ref_col = df.columns[col_idx]
                logger.debug(f"Shipment reference from COLUMN {col_letter} -> column '{shipment_ref_col}'")
            else:
                logger.warning(f"Mapped shipment reference column {col_letter} not found in DataFrame")
        else:
            shipment_ref_col = normalize(src)
            logger.debug(f"Shipment reference from source '{src}' -> normalized '{shipment_ref_col}'")
    else:
        logger.debug("No shipment reference mapping found in CSV.")

    # ---------- ROW FILTER ----------
    candidate_keys = ["COLLECTIONREFERENCE", "DELIVERYREFERENCE", "GOODSDESCRIPTION"]
    key_columns = [c for c in candidate_keys if c in df.columns]
    logger.debug(f"Candidate key columns present: {key_columns}")

    if key_columns:
        df_valid = df[df[key_columns].apply(lambda r: any(clean_text(v) for v in r), axis=1)]
        logger.debug(f"Valid rows after key filter: {df_valid.shape[0]}")
    else:
        logger.warning("No expected key columns found in sheet; processing ALL rows.")
        df_valid = df

    if df_valid.shape[0] == 0:
        logger.warning("NO VALID ROWS FOUND after filtering — XML will have <shipments/> only.")

    # detect unit-like columns
    unit_columns = [c for c in df.columns if "PALLET" in c or "UNIT" in c]
    logger.debug(f"Detected unit columns: {unit_columns}")

    # ---------- BUILD SHIPMENTS ----------
    for idx, (row_idx, row) in enumerate(df_valid.iterrows(), start=1):
        logger.debug(f"---- Shipment row #{idx} (excel row index {row_idx}) ----")
        logger.debug(f"Row data (first 10 cols): {row.to_dict()}")
        shipment_el = ET.SubElement(shipments_el, "shipment")

        # shipment reference
        if shipment_ref_col:
            shipment_ref = clean_text(row.get(shipment_ref_col, ""))
            logger.debug(f"Row {idx} shipment_ref from '{shipment_ref_col}' = '{shipment_ref}'")
            if shipment_ref:
                ET.SubElement(shipment_el, "reference").text = shipment_ref
                ET.SubElement(shipment_el, "edireference").text = shipment_ref
        else:
            logger.debug(f"Row {idx} has no shipment_ref_col configured")

        # pickup
        pickup_el = ET.SubElement(shipment_el, "pickup")
        for m in mappings.get("pickup", []):
            src_norm = normalize(m["source"])
            if src_norm not in row.index:
                logger.debug(f"Row {idx} pickup source '{m['source']}' -> '{src_norm}' NOT FOUND in columns")
            val = clean_text(row.get(src_norm, ""))
            if val:
                mm = get_matchmode(m["tag"])
                attrib = {"matchmode": mm} if mm else {}
                ET.SubElement(pickup_el, m["tag"], attrib).text = val
                logger.debug(f"Row {idx} pickup {m['tag']}='{val}'")

        # delivery
        delivery_el = ET.SubElement(shipment_el, "delivery")
        for m in mappings.get("delivery", []):
            src_norm = normalize(m["source"])
            if src_norm not in row.index:
                logger.debug(f"Row {idx} delivery source '{m['source']}' -> '{src_norm}' NOT FOUND in columns")
            val = clean_text(row.get(src_norm, ""))
            if val:
                mm = get_matchmode(m["tag"])
                attrib = {"matchmode": mm} if mm else {}
                ET.SubElement(delivery_el, m["tag"], attrib).text = val
                logger.debug(f"Row {idx} delivery {m['tag']}='{val}'")

        # cargo
        cargo_el = ET.SubElement(shipment_el, "cargo")
        for m in mappings.get("cargo", []):
            src_norm = normalize(m["source"])
            if src_norm not in row.index:
                logger.debug(f"Row {idx} cargo source '{m['source']}' -> '{src_norm}' NOT FOUND in columns")
            val = clean_text(row.get(src_norm, ""))
            if val:
                mm = get_matchmode(m["tag"])
                attrib = {"matchmode": mm} if mm else {}
                ET.SubElement(cargo_el, m["tag"], attrib).text = val
                logger.debug(f"Row {idx} cargo {m['tag']}='{val}'")

        # unit columns
        for col in unit_columns:
            cell_val = clean_text(row.get(col, ""))
            logger.debug(f"Row {idx} checking unit column '{col}' -> '{cell_val}'")
            if cell_val:
                unit_header = norm_to_raw.get(col, col)
                mm = get_matchmode("unit_id")
                ET.SubElement(cargo_el, "unit_id", {"matchmode": mm}).text = unit_header
                ET.SubElement(cargo_el, "unitamount").text = cell_val
                logger.debug(f"Row {idx} cargo unit: header='{unit_header}', amount='{cell_val}'")
                break

    # write xml
    indent(root)
    ET.ElementTree(root).write(output_xml, encoding="utf-8", xml_declaration=True)
    logger.info(f"XML written successfully: {output_xml}")


# -------- FTP HELPERS --------
def list_xlsx_files(ftp, directory):
    logger.debug(f"Listing XLSX files in {directory}")
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
    except Exception:
        logger.debug("MLSD failed, falling back to NLST", exc_info=True)
        files = [f for f in ftp.nlst() if f.lower().endswith(".xlsx")]
    logger.debug(f"Found XLSX files: {files}")
    return files


def move_file(ftp, from_path, to_directory):
    filename = os.path.basename(from_path)
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"{timestamp}_{filename}"
        logger.debug(f"Moving remote file {from_path} -> {to_directory}/{new_name}")
        ftp.rename(from_path, f"{to_directory}/{new_name}")
    except Exception as e:
        logger.error(f"Error moving file {from_path}: {str(e)}", exc_info=True)
        send_email("Kettyle EDI - File Move Failed", f"Could not move file {filename}.")


def download_file(ftp, remote_path, local_path):
    try:
        logger.debug(f"Downloading {remote_path} -> {local_path}")
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_path}", f.write)
    except Exception as e:
        logger.error(f"Error downloading {remote_path}: {str(e)}", exc_info=True)
        send_email("Kettyle EDI - File Download Failed", f"Could not download {remote_path}.")
        raise


def upload_file(ftp, local_path, remote_path):
    try:
        logger.debug(f"Uploading {local_path} -> {remote_path}")
        with open(local_path, "rb") as f:
            ftp.storbinary(f"STOR {remote_path}", f)
    except Exception as e:
        logger.error(f"Error uploading {local_path}: {str(e)}", exc_info=True)
        send_email("Kettyle EDI - File Upload Failed", f"Could not upload {local_path}.")


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
            logger.debug("FTP connected and in PASV mode")

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
                        logger.error(f"Processing file {file} failed: {str(e)}", exc_info=True)
                        move_file(ftp, remote_path, ERROR_FOLDER)
            else:
                logger.info("No new files detected.")
            previous_files = current_files
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}", exc_info=True)
            send_email("Kettyle EDI - Unexpected Error", f"Unexpected error occurred: {str(e)}")
        finally:
            try:
                if ftp:
                    ftp.quit()
            except:
                pass
        time.sleep(POLL_TIME)


if __name__ == "__main__":
    main()
