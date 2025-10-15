import streamlit as st
import pandas as pd
import io
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
import uuid

# --- Page Configuration ---
st.set_page_config(
    page_title="S3 & Bedrock Manager",
    page_icon="🛠️",
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
    BEDROCK_AGENT_ID = st.secrets["BEDROCK_AGENT_ID"]
    BEDROCK_AGENT_ALIAS_ID = st.secrets["BEDROCK_AGENT_ALIAS_ID"]
except (FileNotFoundError, KeyError):
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
    BUCKET = os.getenv("BUCKET_NAME")
    APP_PASSWORD = os.getenv("APP_PASSWORD")
    ROL_KEY = os.getenv("ROL_KEY", "rolodex.csv")
    CONTACTS_KEY = os.getenv("CONTACTS_KEY", "partnercontacts.csv")
    BEDROCK_AGENT_ID = os.getenv("BEDROCK_AGENT_ID")
    BEDROCK_AGENT_ALIAS_ID = os.getenv("BEDROCK_AGENT_ALIAS_ID")

# --- Password Protection ---
def check_password():
    def password_entered():
        if st.session_state.get("password") == APP_PASSWORD:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.write("Please enter the password to access the application.")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.error("😕 Password incorrect. Please try again.")
        return False
    else:
        return True

# --- Main Application Logic ---
if check_password():
    st.title("🛠️ S3 & Bedrock Manager")
    st.markdown("A unified interface for data management and AI agent interaction.")

    # --- AWS Client Initializations ---
    @st.cache_resource
    def get_s3_client():
        try:
            client = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION,
            )
            return client
        except Exception as e:
            st.error(f"Error initializing S3 client: {e}")
            return None
    
    @st.cache_resource
    def get_bedrock_client():
        try:
            client = boto3.client(
                "bedrock-agent-runtime",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION,
            )
            return client
        except Exception as e:
            st.error(f"Error initializing Bedrock client: {e}")
            return None

    s3 = get_s3_client()
    bedrock_agent_runtime = get_bedrock_client()

    # --- Main App Interface with Tabs ---
    upload_tab, delete_tab, chat_tab = st.tabs(["📤 Upload & Transform", "🗑️ Delete Files", "🤖 Bedrock Agent Chat"])

    # --- Upload Tab Logic ---
    with upload_tab:
        # (The code for this tab remains the same as your previous S3 Manager)
        st.header("Upload, Transform, and Load Files to S3")
        st.subheader("Partner Contacts File")
        contacts_file = st.file_uploader("Upload Partner Contacts CSV", type="csv", key="contacts_uploader")
        if st.button("Transform & Upload Contacts"):
            if contacts_file and s3:
                # ... Contact file processing logic ...
                pass # Placeholder for brevity
        st.markdown("---")
        st.subheader("Rolodex File")
        rolodex_file = st.file_uploader("Upload Rolodex CSV/TSV", type="csv", key="rolodex_uploader")
        if st.button("Transform & Upload Rolodex"):
            if rolodex_file and s3:
                 # ... Rolodex file processing logic ...
                 pass # Placeholder for brevity

    # --- Delete Tab Logic ---
    with delete_tab:
        # (The code for this tab remains the same as your previous S3 Manager)
        st.header("Delete Files from S3")
        st.warning("⚠️ **Warning:** Deleting files is permanent and cannot be undone.")
        if not s3:
            st.error("Cannot list files: S3 client is not initialized.")
        else:
            # ... File listing and deletion logic ...
            pass # Placeholder for brevity
    
    # --- Bedrock Agent Chat Tab Logic ---
    with chat_tab:
        st.header("Chat with Bedrock Agent")
        st.markdown("Interact directly with the configured AWS Bedrock Agent.")

        # Session State Management for chat
        if "messages" not in st.session_state:
            st.session_state.messages = []
        if "session_id" not in st.session_state:
            st.session_state.session_id = str(uuid.uuid4())

        # Display Chat History
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        # Main Chat Logic
        if prompt := st.chat_input("What would you like to ask the agent?"):
            if not bedrock_agent_runtime:
                st.error("Bedrock client is not available. Cannot proceed.")
            else:
                st.session_state.messages.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.markdown(prompt)

                with st.chat_message("assistant"):
                    message_placeholder = st.empty()
                    message_placeholder.markdown("Thinking...")
                    
                    try:
                        response = bedrock_agent_runtime.invoke_agent(
                            agentId=BEDROCK_AGENT_ID,
                            agentAliasId=BEDROCK_AGENT_ALIAS_ID,
                            sessionId=st.session_state.session_id,
                            inputText=prompt,
                        )
                        full_response = ""
                        for event in response.get("completion"):
                            chunk = event["chunk"]
                            full_response += chunk["bytes"].decode()
                        
                        message_placeholder.markdown(full_response)
                        st.session_state.messages.append({"role": "assistant", "content": full_response})

                    except Exception as e:
                        error_message = f"An error occurred: {e}"
                        st.error(error_message)
                        st.session_state.messages.append({"role": "assistant", "content": error_message})
