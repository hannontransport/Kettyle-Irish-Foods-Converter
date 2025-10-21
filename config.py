import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    WATCH_FOLDER = os.environ.get('WATCH_FOLDER')
    PROCESSED_FOLDER = os.environ.get('PROCESSED_FOLDER')
    ERROR_FOLDER = os.environ.get('ERROR_FOLDER')
    FTP_HOST = os.environ.get('FTP_HOST')
    FTP_PORT = int(os.environ.get('FTP_PORT'))
    FTP_USERNAME = os.environ.get('FTP_USERNAME')
    FTP_PASSWORD = os.environ.get('FTP_PASSWORD')
    DOWNLOAD_FOLDER = os.environ.get('DOWNLOAD_FOLDER')
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER')
    POLL_TIME = int(os.environ.get('POLL_TIME'))
    SMTP_SERVER = os.environ.get('SMTP_SERVER')
    SMTP_PORT = int(os.environ.get('SMTP_PORT'))
    SMTP_USERNAME = os.environ.get('SMTP_USERNAME')
    SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD')
    FROM_EMAIL = os.environ.get('FROM_EMAIL')
    TO_EMAIL = os.environ.get('TO_EMAIL')
    COLUMNS_FILE = os.environ.get('COLUMNS_FILE')