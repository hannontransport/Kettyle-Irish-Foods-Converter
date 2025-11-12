# Impluse-Plants-Converter

A Python tool to convert Kettyle Irish Foods Excel to XML 

## Installation

Follow these steps to install and configure the Kettyle Irish Foods Converter.

### 1. Clone the Repository

```bash
git clone git@github.com:hannontransport/Kettyle-Irish-Foods-Converter.git
```

### 2. Configure Firewall Rules

   Ensure the necessary EC2 instances have the following outbound rules:
   ```
   FTP Server
   SMTP Server 
   ```

### 3. Create a .env file

Create a .env file in the project root directory and populate it with the following enviroment varibales 

```
FTP_HOST=<your_ftp_host>
FTP_PORT=<your_ftp_port>
FTP_USERNAME=<your_ftp_username>
FTP_PASSWORD=<your_ftp_password>
WATCH_FOLDER_PATH=<path_to_watch_folder>
PROCESSED_FOLDER_PATH=<path_to_processed_folder>
UPLOAD_FOLDER=<path_to_upload_folder>
DOWNLOAD_FOLDER=<path_to_download_folder>
POLL_TIME=<polling_time_in_seconds>
SMTP_SERVER=<your_smtp_server>
SMTP_PORT=<your_smtp_port>
SMTP_USERNAME=<your_smtp_username>
SMTP_PASSWORD=<your_smtp_password>
FROM_EMAIL=<your_from_email>
TO_EMAIL=<your_to_email>

```

### 4. Create Python Virtual Env
```
python -m venv myenv
```
### 5. Install Required Packages
install the necessary Python Packages using pip 
```
pip install -r requirements.txt
```
### 6. Make Script Executable
```
chmod +x script.sh
```
### Usage 

### Starting the tool

To start the tool, execute the start.sh script 
```
bin/start.sh
```
### Stoping the tool

To stop the tool, execute the stop.sh script 
```
bin/stop.sh
```

Note: Please note variables within the start.sh and stop.sh scripts are currently hard coded therefore will need adjusted to suit. This will be corrected at a later stage.