import streamlit as st
import pandas as pd
import io
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime

# --- Page Configuration ---
st.set_page_config(
    page_title="S3 Bucket Manager",
    page_icon="🗃️",
    layout="centered"
)

# --- Configuration & Secrets Handling ---
# Tries to get secrets from Streamlit Cloud, falls back to .env for local development
try:
    AWS_ACCESS_KEY_ID = st.secrets["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = st.secrets["AWS_SECRET_ACCESS_KEY"]
    AWS_REGION = st.secrets["AWS_DEFAULT_REGION"]
    BUCKET = st.secrets["BUCKET_NAME"]
    APP_PASSWORD = st.secrets["APP_PASSWORD"]
    ROL_KEY = st.secrets.get("ROL_KEY", "rolodex.csv")
    CONTACTS_KEY = st.secrets.get("CONTACTS_KEY", "partnercontacts.csv")
except (FileNotFoundError, KeyError):
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
    BUCKET = os.getenv("BUCKET_NAME")
    APP_PASSWORD = os.getenv("APP_PASSWORD")
    ROL_KEY = os.getenv("ROL_KEY", "rolodex.csv")
    CONTACTS_KEY = os.getenv("CONTACTS_KEY", "partnercontacts.csv")

# --- Password Protection ---
def check_password():
    def password_entered():
        if st.session_state["password"] == APP_PASSWORD:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.write("Please enter the password to access the S3 Bucket Manager.")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.error("😕 Password incorrect. Please try again.")
        return False
    else:
        return True

# --- Main Application Logic ---
if check_password():
    st.title("🗃️ S3 Bucket Manager")
    st.markdown("An interface to transform, upload, and manage data files.")

    # --- S3 Client Initialization ---
    @st.cache_resource
    def get_s3_client():
        if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BUCKET]):
            st.error("Missing AWS credentials or bucket name. Please check secrets configuration.")
            return None
        try:
            session = boto3.session.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION,
            )
            return session.client("s3")
        except Exception as e:
            st.error(f"Error initializing S3 connection: {e}")
            return None

    s3 = get_s3_client()

    # --- S3 Helper Functions ---
    def backup_and_upload_bytes(data_bytes, s3_key, s3_client):
        backup_key = f"backups/{os.path.basename(s3_key)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        try:
            st.info(f"Backing up existing '{s3_key}'...")
            s3_client.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": s3_key}, Key=backup_key)
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                st.warning(f"No existing file for '{s3_key}'. A backup was not created.")
            else:
                st.warning(f"Could not create backup for '{s3_key}': {e}")
        st.info(f"Uploading transformed file to '{s3_key}'...")
        s3_client.put_object(Bucket=BUCKET, Key=s3_key, Body=data_bytes, ContentType="text/csv")

    def list_files_in_bucket(s3_client):
        try:
            files = []
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=BUCKET)
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        files.append(obj["Key"])
            return files
        except Exception as e:
            st.error(f"Could not list files in bucket. Check IAM permissions. Error: {e}")
            return None

    # --- Main App Interface with Tabs ---
    upload_tab, delete_tab = st.tabs(["📤 Upload & Transform", "🗑️ Delete Files"])

    with upload_tab:
        st.header("Upload, Transform, and Load Files to S3")
        
        # --- Partner Contacts Section ---
        st.subheader("Partner Contacts File")
        contacts_file = st.file_uploader("Upload Partner Contacts CSV", type="csv", key="contacts_uploader")
        if st.button("Transform & Upload Contacts"):
            if contacts_file and s3:
                with st.spinner("Processing Partner Contacts file..."):
                    try:
                        # Read contacts CSV
                        encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
                        df = None
                        for encoding in encodings_to_try:
                            try:
                                contacts_file.seek(0)
                                df = pd.read_csv(contacts_file, encoding=encoding)
                                break
                            except UnicodeDecodeError: continue
                        
                        if df is None: raise ValueError("Could not decode contacts file.")

                        # Transform contacts data
                        df.rename(columns={"Account Name": "Partner", "Account Owner": "Partner Manager"}, inplace=True)
                        st.success("✅ Contacts columns renamed.")
                        
                        # Upload to S3
                        csv_bytes = df.to_csv(index=False).encode('utf-8')
                        backup_and_upload_bytes(csv_bytes, CONTACTS_KEY, s3)
                        st.success(f"✅ Successfully uploaded transformed data to `{CONTACTS_KEY}`.")
                    except Exception as e:
                        st.error(f"An error occurred with the Contacts file: {e}")
            elif not s3: st.error("S3 client not initialized.")
            else: st.warning("Please upload a Partner Contacts file first.")

        st.markdown("---")

        # --- Rolodex Section ---
        st.subheader("Rolodex File")
        rolodex_file = st.file_uploader("Upload Rolodex CSV/TSV", type="csv", key="rolodex_uploader")
        if st.button("Transform & Upload Rolodex"):
            if rolodex_file and s3:
                with st.spinner("Processing Rolodex file..."):
                    try:
                        # Read rolodex tab-separated file
                        encodings_to_try = ['utf-16', 'utf-8', 'latin-1']
                        df = None
                        for encoding in encodings_to_try:
                            try:
                                rolodex_file.seek(0)
                                df = pd.read_csv(rolodex_file, encoding=encoding, sep='\t')
                                break
                            except (UnicodeDecodeError, pd.errors.ParserError): continue
                        
                        if df is None: raise ValueError("Could not decode or parse Rolodex file.")
                        
                        first_col_name = df.columns[0]
                        
                        # Define and apply transformations
                        def extract_link(text):
                            try:
                                if not isinstance(text, str): return ""
                                start = text.find('"') + 1
                                end = text.find('"', start)
                                return text[start:end].strip() if start > 0 and end > 0 else ""
                            except Exception: return ""

                        def extract_friendly_name(text):
                            try:
                                if not isinstance(text, str) or not text.upper().startswith('=HYPERLINK'): return text
                                sep = ';' if ';' in text else ','
                                if sep not in text: return text
                                friendly_part = text.split(sep, 1)[1]
                                start = friendly_part.find('"') + 1
                                end = friendly_part.find('"', start)
                                return friendly_part[start:end].strip() if start > 0 and end > 0 else text
                            except Exception: return text
                        
                        df.insert(1, "Documentation Link", df[first_col_name].apply(extract_link))
                        df[first_col_name] = df[first_col_name].apply(extract_friendly_name)
                        st.success("✅ Rolodex data transformed.")

                        # Upload to S3
                        csv_bytes = df.to_csv(index=False).encode('utf-8')
                        backup_and_upload_bytes(csv_bytes, ROL_KEY, s3)
                        st.success(f"✅ Successfully uploaded transformed data to `{ROL_KEY}`.")
                    except Exception as e:
                        st.error(f"An error occurred with the Rolodex file: {e}")
            elif not s3: st.error("S3 client not initialized.")
            else: st.warning("Please upload a Rolodex file first.")

    with delete_tab:
        st.header("Delete Files from S3")
        st.warning("⚠️ **Warning:** Deleting files is permanent and cannot be undone.")
        if not s3:
            st.error("Cannot list files: S3 client is not initialized.")
        else:
            all_files = list_files_in_bucket(s3)
            if all_files is not None:
                files_to_delete = st.multiselect("Select files to delete:", options=all_files)
                if files_to_delete:
                    st.subheader("Confirmation")
                    st.write("You have selected the following files for deletion:")
                    for f in files_to_delete: st.write(f"- `{f}`")
                    
                    if st.checkbox("Yes, I want to permanently delete these files."):
                        if st.button("Delete Selected Files"):
                            with st.spinner("Deleting files..."):
                                try:
                                    delete_payload = [{"Key": key} for key in files_to_delete]
                                    s3.delete_objects(Bucket=BUCKET, Delete={"Objects": delete_payload})
                                    st.success(f"✅ Successfully deleted {len(files_to_delete)} files.")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Deletion failed: {e}")

