import streamlit as st
import boto3
import os
import io
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# --- Configuration & Secrets Handling ---
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
    # --- Initialize S3 Client ---
    def get_s3_client():
        if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BUCKET]):
            st.error("Missing AWS credentials or bucket name. Please check your secrets configuration.")
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

    st.set_page_config(page_title="S3 Bucket Manager", page_icon="🗃️", layout="centered")
    st.title("🗃️ S3 Bucket Manager")
    st.markdown("An easy interface to manage data files for the Patrick project.")

    # --- Helper Functions ---
    def clean_csv_file(file_obj):
        file_obj.seek(0)
        try:
            content = file_obj.read().decode("utf-8", errors="replace")
            if not content.strip():
                st.warning("The uploaded file is empty. An empty file will be uploaded.")
                return b''
            content_stream = io.StringIO(content)
            df = pd.read_csv(content_stream, engine='python', sep=None, on_bad_lines='skip', dtype=str)
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            df.columns = df.columns.str.strip()
            df.fillna("", inplace=True)
            output_stream = io.StringIO()
            df.to_csv(output_stream, index=False)
            output_stream.seek(0)
            return output_stream.getvalue().encode("utf-8")
        except Exception as e:
            raise ValueError(f"Failed to process CSV. Check file format. Original error: {e}")

    def upload_to_s3(file_obj, s3_key, s3_client):
        backup_key = f"backups/{os.path.basename(s3_key)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        try:
            st.info(f"Backing up existing '{s3_key}'...")
            s3_client.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": s3_key}, Key=backup_key)
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                st.warning(f"No existing file for '{s3_key}'. A backup was not created.")
            else:
                st.warning(f"Could not create backup for '{s3_key}': {e}")
        st.info(f"Cleaning and validating data for '{s3_key}'...")
        cleaned_data_bytes = clean_csv_file(file_obj)
        st.info(f"Uploading cleaned file to '{s3_key}'...")
        s3_client.put_object(Bucket=BUCKET, Key=s3_key, Body=cleaned_data_bytes, ContentType="text/csv")

    def list_files_in_bucket(s3_client):
        """ Returns a list of files, or None if an error occurs. """
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
            st.error(f"Could not list files in bucket. Please check your AWS IAM permissions. Error: {e}")
            return None

    # --- Main App Interface ---
    upload_tab, delete_tab = st.tabs(["📤 Upload Files", "🗑️ Delete Files"])

    with upload_tab:
        # ... (Upload tab code is unchanged) ...
        st.header("Upload New Data Files")
        st.markdown(f"Files uploaded here will overwrite `{ROL_KEY}` and `{CONTACTS_KEY}` in the S3 bucket.")
        
        rolodex_file = st.file_uploader(f"Upload Rolodex (will become `{ROL_KEY}`)", type=["csv"], key="rolodex")
        contacts_file = st.file_uploader(f"Upload Partner Contacts (will become `{CONTACTS_KEY}`)", type=["csv"], key="contacts")
        
        if st.button("🚀 Upload to S3"):
            if not s3:
                st.error("Cannot upload: S3 client is not initialized.")
            elif not rolodex_file and not contacts_file:
                st.error("Please upload at least one CSV file to continue.")
            else:
                with st.spinner("Processing uploads..."):
                    if rolodex_file:
                        try:
                            upload_to_s3(rolodex_file, ROL_KEY, s3)
                            st.success(f"✅ Successfully uploaded and updated `{ROL_KEY}`.")
                        except Exception as e:
                            st.error(f"❌ Upload failed for Rolodex file: {e}")
                    if contacts_file:
                        try:
                            upload_to_s3(contacts_file, CONTACTS_KEY, s3)
                            st.success(f"✅ Successfully uploaded and updated `{CONTACTS_KEY}`.")
                        except Exception as e:
                            st.error(f"❌ Upload failed for Contacts file: {e}")

    with delete_tab:
        st.header("Delete Files from S3")
        st.warning("⚠️ **Warning:** Deleting files is permanent and cannot be undone.")
        if not s3:
            st.error("Cannot list files: S3 client is not initialized.")
        else:
            all_files = list_files_in_bucket(s3)

            # --- THIS IS THE FIX ---
            # Only show the multiselect if the list of files was successfully retrieved
            if all_files is not None:
                files_to_delete = st.multiselect(
                    "Select files to delete:",
                    options=all_files,
                    help="You can select multiple files."
                )

                if files_to_delete:
                    st.markdown("---")
                    st.subheader("Confirmation")
                    st.write("You have selected the following files for deletion:")
                    for f in files_to_delete:
                        st.write(f"- `{f}`")
                    
                    confirm = st.checkbox("Yes, I want to permanently delete these files.")
                    
                    if st.button("Delete Selected Files", disabled=not confirm):
                        with st.spinner("Deleting files..."):
                            try:
                                delete_payload = [{"Key": key} for key in files_to_delete]
                                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": delete_payload})
                                st.success(f"✅ Successfully deleted {len(files_to_delete)} files.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Deletion failed: {e}")
