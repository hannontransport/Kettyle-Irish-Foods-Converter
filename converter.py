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
    if not os.path.exists('logs'):
        os.makedirs('logs')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = TimedRotatingFileHandler(
        'logs/KettyleIrishFoodsConverter.log',
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    logger.addHandler(handler)
    return logger

logger = setup_logger()

def send_email(subject, body):
    try:
        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = TO_EMAIL
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(FROM_EMAIL, TO_EMAIL, msg.as_string())
        server.quit()
    except Exception as e:
        logger.error(f"Email send failed: {e}")

def clean_text(value):
    if pd.isna(value):
        return ''
    val = str(value).strip()
    if val in ['#N/A', 'nan', 'NaT', 'None', '']:
        return ''
    return val

def load_mapping(csv_path):
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip().str.lower()
    mappings = {}
    for _, row in df.iterrows():
        section = clean_text(row['section']).lower()
        tag = clean_text(row['tag'])
        source = clean_text(row['source']).upper()
        if section not in mappings:
            mappings[section] = []
        mappings[section].append({'tag': tag, 'source': source})
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
    import openpyxl
    import re
    from difflib import get_close_matches

    mappings = load_mapping(mapping_csv)

    df = pd.read_excel(filepath, sheet_name=0, engine='openpyxl', header=3)

    df = df.replace(r'^\s*$', '', regex=True)
    df = df.fillna('')
    df = df[~(df.applymap(lambda x: str(x).strip() == '').all(axis=1))]

    logger.info(f"🧹 Cleaned DataFrame — remaining rows after removing empty VLOOKUP rows: {len(df)}")

    def normalize_header(h):
        h = str(h).upper()
        h = re.sub(r'[^A-Z0-9]', '', h)
        return h

    raw_headers = list(df.columns)
    df.columns = [normalize_header(c) for c in df.columns]

    logger.info(f"RAW Excel headers (row 4): {raw_headers}")
    logger.info(f"NORMALIZED Excel headers: {list(df.columns)}")

    for section, entries in mappings.items():
        for m in entries:
            src_clean = normalize_header(m['source'])
            match = get_close_matches(src_clean, df.columns, n=1, cutoff=0.7)
            if match:
                m['source'] = match[0]
                logger.info(f"Matched mapping: {section.upper()} → {m['tag']} = {m['source']}")
            else:
                logger.warning(f"No header match for '{m['source']}' in section '{section}'")

    root = ET.Element('transportbookings')
    booking_el = ET.SubElement(root, 'transportbooking')

    ref_value = ''
    try:
        wb = openpyxl.load_workbook(filepath, data_only=True)
        ws = wb.active
        ref_value = clean_text(ws['D2'].value or ws['C2'].value)
    except Exception as e:
        logger.warning(f"Could not read booking reference (D2/C2): {e}")

    if ref_value:
        ET.SubElement(booking_el, 'reference').text = ref_value
        logger.info(f"Booking reference: {ref_value}")
    else:
        logger.warning("No booking reference found in D2 or C2.")

    shipments_el = ET.SubElement(booking_el, 'shipments')
    logger.info(f"Total shipment rows detected: {len(df)}")

    for i, row in df.iterrows():
        if not any(clean_text(v) for v in row.values):
            continue

        shipment_el = ET.SubElement(shipments_el, 'shipment')

        pickup_el = ET.SubElement(shipment_el, 'pickupaddress')
        for m in mappings.get('pickup', []):
            val = clean_text(row.get(m['source'], ''))
            if val:
                ET.SubElement(pickup_el, m['tag']).text = val

        delivery_el = ET.SubElement(shipment_el, 'deliveryaddress')
        for m in mappings.get('delivery', []):
            val = clean_text(row.get(m['source'], ''))
            if val:
                ET.SubElement(delivery_el, m['tag']).text = val

        cargo_el = ET.SubElement(shipment_el, 'cargo')
        for m in mappings.get('cargo', []):
            val = clean_text(row.get(m['source'], ''))
            if not val:
                continue
            if m['tag'].lower() == 'unitamount':
                ET.SubElement(cargo_el, 'unitid').text = 'EuroPallet'
            ET.SubElement(cargo_el, m['tag']).text = val

        if i < 3:
            logger.info(f"🧾 Shipment row {i + 5} preview:")
            for col, val in row.items():
                if val:
                    logger.info(f"  {col}: {val}")

    indent(root)
    tree = ET.ElementTree(root)
    tree.write(output_xml, encoding='utf-8', xml_declaration=True)
    logger.info(f"✅ XML written successfully: {output_xml}")

def list_xlsx_files(ftp, directory):
    try:
        ftp.cwd(directory)
        return [f for f in ftp.nlst() if f.lower().endswith('.xlsx')]
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        send_email("Kettyle Irish Foods EDI - File Listing Failed", f"Error listing files in {directory}.")
        return []


def move_file(ftp, from_path, to_directory):
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = os.path.basename(from_path)
        new_file_name = f"{timestamp}_{file_name}"
        ftp.rename(from_path, f"{to_directory}/{new_file_name}")
    except Exception as e:
        logger.error(f"Error moving file {from_path} to {to_directory}: {e}")
        send_email("Kettyle Irish Foods EDI - File Move Failed", f"Failed to move {file_name} to {to_directory}")


def download_file(ftp, remote_path, local_path):
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'wb') as f:
            ftp.retrbinary(f'RETR {remote_path}', f.write)
    except Exception as e:
        logger.error(f"Error downloading {remote_path}: {e}")
        send_email("Kettyle Irish Foods EDI - File Download Failed", f"Failed to download {remote_path}")
        raise


def upload_file(ftp, local_path, remote_path):
    try:
        with open(local_path, 'rb') as f:
            ftp.storbinary(f'STOR {remote_path}', f)
    except Exception as e:
        logger.error(f"Error uploading {local_path}: {e}")
        send_email("Kettyle Irish Foods EDI - File Upload Failed", f"Failed to upload {local_path}")

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
                    remote_path = f"{WATCH_FOLDER}/{file}"
                    local_path = os.path.join(DOWNLOAD_FOLDER, file)
                    try:
                        download_file(ftp, remote_path, local_path)
                        xml_output_path = os.path.splitext(local_path)[0] + ".xml"
                        write_xml(local_path, xml_output_path)
                        upload_file(ftp, xml_output_path, f"{UPLOAD_FOLDER}/{os.path.basename(xml_output_path)}")
                        move_file(ftp, remote_path, PROCESSED_FOLDER)
                    except Exception as e:
                        logger.error(f"Processing error for {file}: {e}")
                        move_file(ftp, remote_path, ERROR_FOLDER)
                        break
            else:
                logger.info("No new files detected.")

            previous_files = current_files
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            send_email("Kettyle Irish Foods EDI - Unexpected Error", f"Unexpected error occurred. Check logs.")
        finally:
            try:
                ftp.quit()
            except:
                pass
        time.sleep(POLL_TIME)

if __name__ == "__main__":
    main()
