import os
import time
import logging
from datetime import datetime
import ftplib
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import xml.etree.ElementTree as ET
from logging.handlers import TimedRotatingFileHandler
from config import Config
import math
import re

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
TO_EMAIL = Config.FROM_EMAIL

if not os.path.exists('logs'):
    os.makedirs('logs/')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler('logs/KettyleIrishFoodsConverter.log', when='midnight', interval=1, backupCount=30, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

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
    if value is None:
        return ''
    if isinstance(value, float) and math.isnan(value):
        return ''
    return str(value).strip()

def find_header_row(excel_path):
    temp_df = pd.read_excel(excel_path, sheet_name='Blad1', header=None, engine='openpyxl')
    for i, row in temp_df.iterrows():
        if any(re.search(r'REFERENCE', str(cell), re.I) for cell in row):
            return i
    return 0

def convert_excel_to_xml(excel_path, xml_output_path):
    try:
        header_row = find_header_row(excel_path)
        logger.info(f"Reading Excel file: {excel_path} (header row {header_row})")
        df = pd.read_excel(excel_path, sheet_name='Blad1', engine='openpyxl', header=header_row)
        df.fillna('', inplace=True)
        if df.empty:
            logger.warning("Excel file is empty. Skipping.")
            return
        root = ET.Element('transportbookings')
        booking_el = ET.SubElement(root, 'transportbooking')
        overall_ref = clean_text(df.iloc[0].get('Reference'))
        if overall_ref:
            ET.SubElement(booking_el, 'reference').text = overall_ref
        shipments_el = ET.SubElement(booking_el, 'shipments')
        for _, row in df.iterrows():
            shipment_el = ET.SubElement(shipments_el, 'shipment')
            shipment_ref = clean_text(row.get('Reference'))
            if shipment_ref:
                ET.SubElement(shipment_el, 'reference').text = shipment_ref
            pickup_el = ET.SubElement(shipment_el, 'pickupaddress')
            ET.SubElement(pickup_el, 'address_id').text = clean_text(row.get('COLLECTION REFERENCE'))
            ET.SubElement(pickup_el, 'date').text = clean_text(row.get('LOADING'))
            ET.SubElement(pickup_el, 'name').text = clean_text(row.get('COLLECTION NAME & ADDRESS *'))
            delivery_el = ET.SubElement(shipment_el, 'deliveryaddress')
            ET.SubElement(delivery_el, 'date').text = clean_text(row.get('UNLOADING'))
            ET.SubElement(delivery_el, 'address_id').text = clean_text(row.get('COLLECTION REFERENCE'))
            ET.SubElement(delivery_el, 'address1').text = clean_text(row.get('DELIVERY NAME & ADDRESS'))
            ET.SubElement(delivery_el, 'city_id').text = clean_text(row.get('DELIVERY CITY'))
            ET.SubElement(delivery_el, 'deliverytime').text = clean_text(row.get('DELIVERY TIME'))
            cargo_el = ET.SubElement(shipment_el, 'cargo')
            ET.SubElement(cargo_el, 'product_id').text = clean_text(row.get('GOODS DESCRIPTION'))
            euro_val = clean_text(row.get('EURO PALLET *'))
            if euro_val:
                ET.SubElement(cargo_el, 'unitid').text = 'EuroPallet'
                ET.SubElement(cargo_el, 'unitamount').text = euro_val
        tree = ET.ElementTree(root)
        tree.write(xml_output_path, encoding='utf-8', xml_declaration=True)
        logger.info(f"XML created successfully: {xml_output_path}")
    except Exception as e:
        logger.error(f"Error converting Excel to XML: {e}")
        send_email("Kettyle Irish Foods EDI - Conversion Failed", f"Error converting {os.path.basename(excel_path)}. Check logs for details.")
        raise

def list_xlsx_files(ftp, directory):
    file_list = []
    try:
        ftp.cwd(directory)
        files = ftp.nlst()
        for f in files:
            if f.lower().endswith('.xlsx'):
                file_list.append(f)
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        send_email("Kettyle Irish Foods EDI - File Listing Failed", f"Error listing files in {directory}.")
    return file_list

def move_file(ftp, from_path, to_directory):
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = os.path.basename(from_path)
        new_file_name = f"{timestamp}_{file_name}"
        to_path = f"{to_directory}/{new_file_name}"
        ftp.rename(from_path, to_path)
        time.sleep(1)
    except Exception as e:
        logger.error(f"Error moving file {from_path} to {to_directory}: {e}")
        send_email("Kettyle Irish Foods EDI - File Move Failed", f"Failed to move {file_name} to {to_directory}")

def download_file(ftp, remote_path, local_path):
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            ftp.size(remote_path)
        except ftplib.error_perm:
            logger.warning(f"Remote file does not exist: {remote_path}")
            raise FileNotFoundError(f"Remote file not found: {remote_path}")
        with open(local_path, 'wb') as local_file:
            ftp.retrbinary(f'RETR {remote_path}', local_file.write)
    except Exception as e:
        logger.error(f"Error downloading file {remote_path}: {e}")
        send_email("Kettyle Irish Foods EDI - File Download Failed", f"Failed to download {remote_path}")
        raise

def upload_file(ftp, local_path, remote_path):
    try:
        with open(local_path, 'rb') as local_file:
            ftp.storbinary(f'STOR {remote_path}', local_file)
    except Exception as e:
        logger.error(f"Error uploading file {local_path}: {e}")
        send_email("Kettyle Irish Foods EDI - File Upload Failed", f"Failed to upload {local_path}")

def main():
    previous_files = []
    while True:
        try:
            os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
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
                        logger.info(f"Downloading {file}")
                        download_file(ftp, remote_path, local_path)
                        xml_output_path = os.path.splitext(local_path)[0] + ".xml"
                        convert_excel_to_xml(local_path, xml_output_path)
                        upload_path = f"{UPLOAD_FOLDER}/{os.path.basename(xml_output_path)}"
                        logger.info(f"Uploading XML: {upload_path}")
                        upload_file(ftp, xml_output_path, upload_path)
                        move_file(ftp, remote_path, PROCESSED_FOLDER)
                    except Exception as e:
                        logger.error(f"Processing error for {file}: {e}")
                        try:
                            move_file(ftp, remote_path, ERROR_FOLDER)
                        except Exception as move_err:
                            logger.error(f"Failed to move errored file {file}: {move_err}")
                        continue
                current_files = list_xlsx_files(ftp, WATCH_FOLDER)
            else:
                logger.info("No new files detected.")
            previous_files = current_files
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            send_email("Kettyle Irish Foods EDI - Unexpected Error", "Unexpected error occurred. Check logs for details.")
        finally:
            try:
                ftp.quit()
            except Exception:
                pass
        time.sleep(POLL_TIME)

if __name__ == "__main__":
    main()
