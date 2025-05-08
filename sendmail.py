import email, smtplib, ssl

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import os
import sys
from time import sleep
from datetime import timedelta
from datetime import datetime
#import MySQLdb
import re
import ast
import csv
import os
import glob

global disTime
global disMail
global rpiTime
global rpiTimeStr
global rpiTimeInt
global disTimeStr
global time_object
global time_object2
global emailCounter
import shutil


files_path = "outputs/monthly-table/"

subject = "VIGAN SERVER DATA"
body = ""

sender_email = "corepointph@gmail.com"
receiver_email = "kjmacni@gmail.com"
receiver_email1 = "pagasa.abrbffwc@gmail.com"

password = ""

# Create a multipart message and set headers
message = MIMEMultipart()
message["From"] = sender_email
message["To"] = receiver_email
message["Subject"] = "VIGAN SERVER DATA"
message["Bcc"] = receiver_email  # Recommended for mass emails

# Add body to email
message.attach(MIMEText(body, "plain"))


def compress_folder(folder_path, output_zip_path):
    """Compresses a folder to a zip archive.

    Args:
        folder_path: The path to the folder to compress.
        output_zip_path: The path where the zip archive will be created.
    """
    try:
        shutil.make_archive(output_zip_path, 'zip', folder_path)
        print(f"Folder '{folder_path}' compressed to '{output_zip_path}.zip' successfully.")
    except Exception as e:
        print(f"Error compressing folder: {e}")
        


folder_to_compress = files_path  # Replace with the actual folder path
output_zip_file = "monthly-table"  # Replace with the desired output zip file name (without .zip)
compress_folder(folder_to_compress, output_zip_file)
filename = "monthly-table.zip"

def sendMail():
    try:
        #print(csv_list[0])
        # Open PDF file in binary mode
        with open(filename, "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email    
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment part
        part.add_header(
            "Content-Disposition",
            "attachment; filename= monthly-table.zip",
        )

        # Add attachment to message and convert message to string
        message.attach(part)
        text = message.as_string()

        # Log in to server using secure context and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, text)
            server.sendmail(sender_email, receiver_email1, text)
            
    except:
        print("No network")
        pass


print("Sending monthly-table.zip")
sendMail()
print("Email Sent")


"""
for i, csv in enumerate(csv_list):
    print(f"Sending {csv}")        #working
    sendMail(index=i, filename=csv)
    print("Email Sent")
"""
