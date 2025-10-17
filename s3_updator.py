import streamlit as st
import pandas as pd
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import uuid
import random

# --- Page Configuration ---
st.set_page_config(
    page_title="S3 & Bedrock Manager",
    page_icon="🛠️",
    layout="wide"
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
            if "password" in st.session_state:
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

    # --- AWS Client Initializations (kept for other tabs) ---
    @st.cache_resource
    def get_s3_client(access_key, secret_key, region):
        try:
            client = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
            return client
        except Exception as e:
            st.error(f"Error initializing S3 client: {e}")
            return None
    
    @st.cache_resource
    def get_bedrock_client(access_key, secret_key, region):
        try:
            client = boto3.client("bedrock-agent-runtime", aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
            return client
        except Exception as e:
            st.error(f"Error initializing Bedrock client: {e}")
            return None

    s3 = get_s3_client(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
    bedrock_agent_runtime = get_bedrock_client(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
    
    # --- Helper Functions (used across tabs) ---
    def backup_and_upload_bytes(data_bytes, s3_key, s3_client):
        backup_key = f"backups/{os.path.basename(s3_key)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        try:
            st.info(f"Backing up existing '{s3_key}'...")
            s3_client.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": s3_key}, Key=backup_key)
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404': st.warning(f"No existing file for '{s3_key}'. A backup was not created.")
            else: st.warning(f"Could not create backup for '{s3_key}': {e}")
        st.info(f"Uploading transformed file to '{s3_key}'...")
        s3_client.put_object(Bucket=BUCKET, Key=s3_key, Body=data_bytes, ContentType="text/csv")

    def list_files_in_bucket(s3_client):
        try:
            files = []
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=BUCKET):
                if "Contents" in page:
                    for obj in page["Contents"]: files.append(obj["Key"])
            return files
        except Exception as e:
            st.error(f"Could not list files in bucket. Check IAM permissions. Error: {e}")
            return None
    
    @st.cache_data(ttl=300)
    def get_s3_file_timestamp(_s3_client, file_key):
        if not _s3_client: return "S3 client not available."
        try:
            response = _s3_client.head_object(Bucket=BUCKET, Key=file_key)
            last_modified_utc = response['LastModified']
            return f"Last updated: {last_modified_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404': return f"Error: File '{file_key}' not found in S3."
            elif error_code == '403': return f"Error: Permission denied for '{file_key}'. Ensure user has 's3:HeadObject' permission."
            else: return f"An S3 client error occurred: {e.response['Error']['Message']}"
        except Exception as e: return f"An unexpected error occurred: {e}"

    # --- Main App Interface with Tabs ---
    upload_tab, delete_tab, chat_tab, metrics_tab = st.tabs(["📤 Upload & Transform", "🗑️ Delete Files", "🤖 Bedrock Agent Chat", "📊 Performance Metrics"])

    # --- Upload Tab Logic ---
    with upload_tab:
        st.header("Upload, Transform, and Load Files to S3")
        # ... (rest of the upload tab code remains the same) ...
        col1_up, col2_up = st.columns(2)
        with col1_up:
            st.subheader("Partner Contacts File")
            contacts_timestamp = get_s3_file_timestamp(s3, CONTACTS_KEY)
            st.caption(contacts_timestamp)
            contacts_file = st.file_uploader("Upload Partner Contacts CSV", type="csv", key="contacts_uploader")
            if st.button("Transform & Upload Contacts"):
                if contacts_file and s3:
                    with st.spinner("Processing Partner Contacts file..."):
                        try:
                            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
                            df = None
                            for encoding in encodings:
                                try:
                                    contacts_file.seek(0)
                                    df = pd.read_csv(contacts_file, encoding=encoding)
                                    break
                                except UnicodeDecodeError: continue
                            if df is None: raise ValueError("Could not decode contacts file.")
                            df.rename(columns={"Account Name": "Partner", "Account Owner": "Partner Manager"}, inplace=True)
                            st.success("✅ Contacts columns renamed.")
                            csv_bytes = df.to_csv(index=False).encode('utf-8')
                            backup_and_upload_bytes(csv_bytes, CONTACTS_KEY, s3)
                            st.success(f"✅ Successfully uploaded transformed data to `{CONTACTS_KEY}`.")
                        except Exception as e: st.error(f"An error occurred with the Contacts file: {e}")
        with col2_up:
            st.subheader("Rolodex File")
            rolodex_timestamp = get_s3_file_timestamp(s3, ROL_KEY)
            st.caption(rolodex_timestamp)
            rolodex_file = st.file_uploader("Upload Rolodex CSV/TSV", type="csv", key="rolodex_uploader")
            if st.button("Transform & Upload Rolodex"):
                if rolodex_file and s3:
                    with st.spinner("Processing Rolodex file..."):
                        try:
                            encodings = ['utf-16', 'utf-8', 'latin-1']
                            df = None
                            for encoding in encodings:
                                try:
                                    rolodex_file.seek(0)
                                    df = pd.read_csv(rolodex_file, encoding=encoding, sep='\t')
                                    break
                                except (UnicodeDecodeError, pd.errors.ParserError): continue
                            if df is None: raise ValueError("Could not decode or parse Rolodex file.")
                            first_col = df.columns[0]
                            def extract_link(t):
                                try:
                                    if not isinstance(t, str): return ""
                                    s = t.find('"') + 1; e = t.find('"', s)
                                    return t[s:e].strip() if s > 0 and e > 0 else ""
                                except Exception: return ""
                            def extract_friendly(t):
                                try:
                                    if not isinstance(t, str) or not t.upper().startswith('=HYPERLINK'): return t
                                    sep = ';' if ';' in t else ',';
                                    if sep not in t: return t
                                    p = t.split(sep, 1)[1]; s = p.find('"') + 1; e = p.find('"', s)
                                    return p[s:e].strip() if s > 0 and e > 0 else t
                                except Exception: return t
                            df.insert(1, "Documentation Link", df[first_col].apply(extract_link))
                            df[first_col] = df[first_col].apply(extract_friendly)
                            st.success("✅ Rolodex data transformed.")
                            csv_bytes = df.to_csv(index=False).encode('utf-8')
                            backup_and_upload_bytes(csv_bytes, ROL_KEY, s3)
                            st.success(f"✅ Successfully uploaded transformed data to `{ROL_KEY}`.")
                        except Exception as e: st.error(f"An error occurred with the Rolodex file: {e}")


    # --- Delete Tab Logic ---
    with delete_tab:
        st.header("Delete Files from S3")
        st.warning("⚠️ **Warning:** Deleting files is permanent and cannot be undone.")
        if not s3: st.error("Cannot list files: S3 client is not initialized.")
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
                                    s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": key} for key in files_to_delete]})
                                    st.success(f"✅ Successfully deleted {len(files_to_delete)} files.")
                                    st.rerun()
                                except Exception as e: st.error(f"❌ Deletion failed: {e}")

    # --- Bedrock Agent Chat Tab Logic ---
    with chat_tab:
        st.header("Chat with Bedrock Agent")
        st.markdown("Interact directly with the configured AWS Bedrock Agent.")
        if "messages" not in st.session_state: st.session_state.messages = []
        if "session_id" not in st.session_state: st.session_state.session_id = str(uuid.uuid4())

        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
        if prompt := st.chat_input("What would you like to ask the agent?"):
            st.chat_message("user").markdown(prompt)
            st.session_state.messages.append({"role": "user", "content": prompt})

            if not bedrock_agent_runtime:
                st.error("Bedrock client is not available. Cannot proceed.")
            else:
                with st.chat_message("assistant"):
                    with st.spinner("Thinking..."):
                        try:
                            response = bedrock_agent_runtime.invoke_agent(agentId=BEDROCK_AGENT_ID, agentAliasId=BEDROCK_AGENT_ALIAS_ID, sessionId=st.session_state.session_id, inputText=prompt)
                            full_response = ""
                            for event in response.get("completion", []):
                                chunk = event["chunk"]
                                full_response += chunk["bytes"].decode()
                            
                            st.markdown(full_response)
                            st.session_state.messages.append({"role": "assistant", "content": full_response})
                        except Exception as e:
                            error_message = f"An error occurred: {e}"
                            st.error(error_message)
                            st.session_state.messages.append({"role": "assistant", "content": error_message})
    
    # --- Performance Metrics Tab ---
    with metrics_tab:
        st.header("Patrick Agent - Performance Dashboard")

        # --- MOCK DATA GENERATION ---
        @st.cache_data(ttl=60) # Cache for 60 seconds
        def generate_mock_data():
            """Creates a fake DataFrame simulating DynamoDB logs."""
            num_entries = 100
            data = []
            for i in range(num_entries):
                # Generate a timestamp within the last 7 days
                timestamp = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 6), hours=random.randint(0, 23))
                
                # Simulate latency
                latency = random.randint(800, 7500) # milliseconds
                
                # Simulate feedback (70% positive, 20% negative, 10% no feedback)
                feedback_options = ['positive'] * 7 + ['negative'] * 2 + [None]
                feedback = random.choice(feedback_options)

                # Simulate token usage
                input_tokens = random.randint(1500, 4500)
                output_tokens = random.randint(50, 300)

                data.append({
                    "timestamp": timestamp,
                    "userMessage": f"Sample user query #{i+1}",
                    "agentResponse": f"Sample agent response #{i+1}",
                    "agentLatency": latency,
                    "feedbackStatus": feedback,
                    "inputTokens": input_tokens,
                    "outputTokens": output_tokens,
                    "status": "SUCCESS"
                })
            
            df = pd.DataFrame(data)
            # Ensure timestamp is in datetime format and sort
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.sort_values(by="timestamp", ascending=False, inplace=True)
            return df

        # --- METRIC CALCULATION ---
        def calculate_metrics(df):
            """Calculates all key metrics from the log data."""
            # Filter for data from the last 7 days
            seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
            recent_df = df[df['timestamp'] >= seven_days_ago]
            
            total_queries = len(recent_df)
            avg_latency_ms = recent_df['agentLatency'].mean()
            
            # Feedback metrics
            feedback_counts = recent_df['feedbackStatus'].value_counts()
            positive_feedback = feedback_counts.get('positive', 0)
            total_feedback = feedback_counts.sum()
            positive_rate = (positive_feedback / total_feedback * 100) if total_feedback > 0 else 0
            
            # Token & Cost metrics
            total_input_tokens = recent_df['inputTokens'].sum()
            total_output_tokens = recent_df['outputTokens'].sum()
            total_tokens = total_input_tokens + total_output_tokens
            
            # Pricing: Claude 3.5 Sonnet (replace with Haiku if confirmed)
            # Input: $3 / 1M tokens, Output: $15 / 1M tokens
            # Pricing: Claude 3 Haiku
            input_cost_per_million = 0.25 # $0.25 per 1M tokens
            output_cost_per_million = 1.25 # $1.25 per 1M tokens

            total_cost = (total_input_tokens / 1_000_000 * input_cost_per_million) + \
                         (total_output_tokens / 1_000_000 * output_cost_per_million)
            
            avg_cost_per_query = total_cost / total_queries if total_queries > 0 else 0
            
            return {
                "total_queries": total_queries,
                "avg_latency_sec": avg_latency_ms / 1000 if not pd.isna(avg_latency_ms) else 0,
                "positive_feedback_rate": positive_rate,
                "total_tokens": total_tokens,
                "total_cost": total_cost,
                "avg_cost_per_query": avg_cost_per_query
            }

        # --- DASHBOARD UI ---
        # NOTE: In a real app, you would fetch from DynamoDB here instead of generating mock data.
        log_df = generate_mock_data()
        metrics = calculate_metrics(log_df)

        st.markdown("### Key Metrics (Last 7 Days)")
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Queries", f"{metrics['total_queries']:,}")
        col2.metric("Avg. Agent Latency", f"{metrics['avg_latency_sec']:.2f} s")
        col3.metric("Positive Feedback", f"{metrics['positive_feedback_rate']:.1f}%")
        col4.metric("Total Cost (Est.)", f"${metrics['total_cost']:.2f}")

        st.markdown("---")
        
        col5, col6 = st.columns(2)
        with col5:
            st.subheader("Token Consumption")
            st.metric("Total Tokens Used", f"{metrics['total_tokens']:,}")
            st.metric("Avg. Cost per Query", f"${metrics['avg_cost_per_query']:.4f}")

        with col6:
            st.subheader("Daily Query Volume")
            daily_counts = log_df.set_index('timestamp').resample('D').size()
            daily_counts.index = daily_counts.index.strftime('%b %d')
            st.bar_chart(daily_counts)
        
        st.markdown("---")
        
        st.subheader("Recent Interactions")
        st.dataframe(
            log_df[['timestamp', 'userMessage', 'agentResponse', 'agentLatency', 'feedbackStatus']].head(20),
            use_container_width=True
        )