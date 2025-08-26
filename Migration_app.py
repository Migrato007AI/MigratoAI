from flask import Flask, request, render_template,redirect,url_for,jsonify

import os
import glob
import difflib
import subprocess
import requests
import json
import base64
import sqlparse
import time
import logging
from azure.identity import InteractiveBrowserCredential
import logging
from logging.handlers import RotatingFileHandler
import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io
from io import StringIO
from io import BytesIO
import threading
from azure.storage.blob import (
    BlobServiceClient,
    generate_blob_sas,
    generate_container_sas,
    BlobSasPermissions,
)
import psycopg2

applied_results = []
logger_array = []

STORAGE_ACCOUNT_NAME = os.environ.get('STORAGE_ACCOUNT_NAME')
STORAGE_ACCOUNT_KEY_BASE64 = os.environ.get('STORAGE_ACCOUNT_KEY_BASE64')
STORAGE_CONTAINER_NAME = os.environ.get('STORAGE_CONTAINER_NAME')
CONFIG_DB_HOST = os.environ.get('CONFIG_DB_HOST')
CONFIG_DB_PORT = os.environ.get('CONFIG_DB_PORT')
CONFIG_DB_NAME = os.environ.get('CONFIG_DB_NAME')
CONFIG_DB_USER = os.environ.get('CONFIG_DB_USER')
CONFIG_DB_PASSWORD = os.environ.get('CONFIG_DB_PASSWORD')

import base64
import datetime
import logging
import threading
from io import BytesIO, StringIO

from azure.storage.blob import BlobServiceClient


import requests
import datetime
import hashlib
import hmac
import base64

import base64
import hmac
import hashlib
from urllib.parse import quote_plus
from datetime import datetime, timedelta


from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta

def generate_sas_token(account_name, account_key, container_name, blob_name, expiry_minutes=60):
    try:
        sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(minutes=expiry_minutes)
        )

        url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        hyperlink = f'<a href="{url}" download="{blob_name}">Download {blob_name}</a>'
        return hyperlink
    except Exception as e:
        print(f"Error generating SAS token: {e}")
        raise e


def build_authorization_header(account_name, account_key, verb, content_length, content_type, date, canonicalized_headers, canonicalized_resource):
    string_to_sign = (
        f"{verb}\n"                      # VERB
        "\n"                             # Content-Encoding
        "\n"                             # Content-Language
        f"{content_length}\n"            # Content-Length
        "\n"                             # Content-MD5
        f"{content_type}\n"              # Content-Type
        "\n"                             # Date
        "\n"                             # If-Modified-Since
        "\n"                             # If-Match
        "\n"                             # If-None-Match
        "\n"                             # If-Unmodified-Since
        "\n"                             # Range
        f"{canonicalized_headers}"
        f"{canonicalized_resource}"
    )

    decoded_key = base64.b64decode(account_key)  # Correct decoding of base64 key
    signature = hmac.new(decoded_key, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
    encoded_signature = base64.b64encode(signature).decode()
    auth_header = f"SharedKey {account_name}:{encoded_signature}"
    return auth_header


def upload_blob(account_name, account_key, container_name, blob_name, data, content_type="text/plain"):
    url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}"

    method = "PUT"
    request_time = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    content_length = str(len(data))
    content_type = content_type
    x_ms_headers = {
        'x-ms-blob-type': 'BlockBlob',
        'x-ms-date': request_time,
        'x-ms-version': '2020-10-02'
    }

    # Construct canonicalized headers string
    canonicalized_headers = ''
    for header in sorted(x_ms_headers.keys()):
        canonicalized_headers += f"{header}:{x_ms_headers[header]}\n"

    # Construct canonicalized resource string
    canonicalized_resource = f"/{account_name}/{container_name}/{blob_name}"

    authorization_header = build_authorization_header(
        account_name,
        account_key,
        method,
        content_length,
        content_type,
        request_time,
        canonicalized_headers,
        canonicalized_resource
    )

    headers = {
        'x-ms-blob-type': 'BlockBlob',
        'x-ms-date': request_time,
        'x-ms-version': '2020-10-02',
        'Content-Type': content_type,
        'Content-Length': content_length,
        'Authorization': authorization_header
    }

    response = requests.put(url, headers=headers, data=data)
    if response.status_code in (201, 202):
        print("Upload successful!")
    else:
        print(f"Upload failed with status code {response.status_code}: {response.text}")


logger_array=[]
def setup_in_memory_logger(logger_name='in_memory_logger', level=logging.DEBUG):
    """
    Creates and returns a logger that stores logs in memory.

    Returns:
        logger (logging.Logger): Configured logger instance.
        log_stream (io.StringIO): StringIO buffer containing logs.
    """
    log_stream = io.StringIO()
    stream_handler = logging.StreamHandler(log_stream)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Remove other handlers if any to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(stream_handler)

    return logger, log_stream

logger, log_stream = setup_in_memory_logger('migration_logger')
log_stream.truncate(0)  
log_stream.seek(0)

def save_postgres_sql_to_storage(cr_number, obj_name, source_def, dest_def, logger):
    try:
        source_blob_path = f"{cr_number}/Postgres/Source"
        blob_name = f"{obj_name}.sql"
        blob_name = f"{source_blob_path}/{blob_name}"
        print(logger_array)
        print(type(source_def))
        #upload_blob(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY_BASE64, STORAGE_ACCOUNT_NAME, blob_name, source_def,logger, content_type="text/plain")
        upload_blob(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY_BASE64, STORAGE_CONTAINER_NAME, blob_name, source_def)
        
        logger.info(f"Uploaded PostgreSQL source definition for '{obj_name}' to '{source_blob_path}'")
        logger_array.append(log_stream.getvalue())
        
        
        SAS_URL_Source_DB = generate_sas_token(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY_BASE64, STORAGE_CONTAINER_NAME, blob_name)
        logger.info(f"Generated SAS token for PostgreSQL source definition for source '{obj_name}'")
        logger_array.append(log_stream.getvalue())
        
        dest_blob_path = f"{cr_number}/Postgres/Backup/{obj_name}.sql"
        blob_name = f"{dest_blob_path}/{obj_name}.sql"
        upload_blob(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY_BASE64, STORAGE_CONTAINER_NAME, blob_name, dest_def)
        
        logger.info(f"Uploaded PostgreSQL destination definition for '{obj_name}' to '{dest_blob_path}'")
        logger_array.append(log_stream.getvalue())
        
        SAS_URL_Dest_DB=generate_sas_token(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY_BASE64, STORAGE_CONTAINER_NAME, blob_name)
        logger.info(f"Generated SAS token for PostgreSQL destination definition for destination '{obj_name}'")
        logger_array.append(log_stream.getvalue())
        
        return SAS_URL_Source_DB, SAS_URL_Dest_DB

    except Exception as e:
        logger.error(f"Error uploading PostgreSQL SQL for '{obj_name}': {e}")
        logger_array.append(log_stream.getvalue())


def save_synapse_json_to_storage(cr_number, obj_name, s_json, d_json, logger):
    try:
        source_blob_path = f"{cr_number}/Synapse/Source"
        blob_name = f"{obj_name}.json"
        blob_name = f"{source_blob_path}/{blob_name}"
        
        
        upload_blob(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY_BASE64, STORAGE_CONTAINER_NAME, blob_name, json.dumps(s_json))
        
        logger.info(f"Uploaded Synapse source JSON for '{obj_name}' to '{source_blob_path}'")
        logger_array.append(log_stream.getvalue())
        try:
            SAS_URL_Source_Synapse = generate_sas_token(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY_BASE64, STORAGE_CONTAINER_NAME, blob_name)
            logger.info(f"Generated SAS token for Synapse source JSON for source '{obj_name}'")
            logger_array.append(log_stream.getvalue())
        except Exception as e:
            logger.error(f"Error generating SAS token for Synapse source JSON for '{obj_name}': {e}")
            logger_array.append(log_stream.getvalue())


        if d_json:
            dest_blob_path = f"{cr_number}/Synapse/Dest"
            # d_json can be either a string content or a local filepath; handle accordingly
            blob_name = f"{dest_blob_path}/{obj_name}.json"
            upload_blob(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY_BASE64, STORAGE_CONTAINER_NAME, blob_name, json.dumps(d_json))
            SAS_URL_dest_Synapse = generate_sas_token(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY_BASE64, STORAGE_CONTAINER_NAME, blob_name)
            logger.info(f"Generated SAS token for Synapse destination JSON for destination '{obj_name}'")
            logger_array.append(log_stream.getvalue())
        else:
            SAS_URL_dest_Synapse = None
        return SAS_URL_Source_Synapse, SAS_URL_dest_Synapse
    except Exception as e:
        logger.error(f"Error uploading Synapse JSON for '{obj_name}': {e}")
        logger_array.append(log_stream.getvalue())






def get_token():
    credential = InteractiveBrowserCredential()
    arm_scope = "https://management.azure.com/.default"
    arm_token = credential.get_token(arm_scope).token

    # Get Synapse workspace token
    synapse_scope = "https://dev.azuresynapse.net/.default"
    synapse_token = credential.get_token(synapse_scope).token
    token ={}
    token['arm_token']=arm_token
    token['synapse_token']= synapse_token
    return token

def get_token_db():
    credential = InteractiveBrowserCredential()
    db_scope = "https://ossrdbms-aad.database.windows.net/.default"
    db_token = credential.get_token(db_scope).token
    return db_token

def list_subscriptions(token):
    url = "https://management.azure.com/subscriptions?api-version=2020-01-01"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()



def list_tenants(access_token):
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    # List tenants endpoint via Microsoft Graph
    response = requests.get("https://graph.microsoft.com/v1.0/organization", headers=headers)
    response.raise_for_status()
    tenants = response.json()
    return tenants

def get_synapse_git_config(subscription_id, resource_group, workspace_name,token):
    
    url = (
        f"https://management.azure.com/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Synapse/workspaces/{workspace_name}/gitConfiguration"
        f"?api-version=2023-05-01"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        git_config = response.json()
        # Extract relevant info
        repo_type = git_config.get("repositoryType", "Not linked")
        repo_url = git_config.get("repositoryUrl", "N/A")
        root_folder = git_config.get("rootFolder", "N/A")
        collaboration_branch = git_config.get("collaborationBranch", "N/A")
        last_commit_id = git_config.get("lastCommitId", "N/A")

        return git_config
    else:
        print(f"Failed to get Git configuration: {response.status_code} {response.text}")
        return None

def synapse_workspace_properties(workspace_name: str, synapse_token: str):
    synapse_url = f"https://{workspace_name}.dev.azuresynapse.net/workspace?api-version=2020-12-01"
    synapse_headers = {
        "Authorization": f"Bearer {synapse_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(synapse_url, headers=synapse_headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def list_synapse_workspaces(token, subscription_id):
    url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Synapse/workspaces?api-version=2021-06-01"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def list_postgres_servers(token, subscription_id):
    """
    Returns a list of PostgreSQL servers with their names and resource groups.
    """
    url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.DBforPostgreSQL/flexibleServers?api-version=2022-12-01"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    
    servers = response.json().get("value", [])
    server_list = []
    for server in servers:
        server_name = server["name"]
        resource_group = server['id'].split('/')[4]
        server_list.append({
            'server_name': server_name,
            'resource_group': resource_group
        })
    return server_list



def list_azure_db_servers(token, subscription_id, db_type):
    providers = {
        'postgresql': {
            'provider': 'Microsoft.DBforPostgreSQL/flexibleServers',
            'api_version': '2022-12-01'
        },
        'sql': {
            'provider': 'Microsoft.Sql/servers',
            'api_version': '2022-11-01-preview'
        },
        'mysql': {
            'provider': 'Microsoft.DBforMySQL/flexibleServers',
            'api_version': '2022-12-01'
        },
        'oracle': {
            'provider': 'Microsoft.Oracle/servers',
            'api_version': '2022-01-01'
        }
    }
    
    if db_type.lower() not in providers:
        raise ValueError(f"Unsupported database type: {db_type}. Supported types: {list(providers.keys())}")
    
    provider_info = providers[db_type.lower()]
    url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/{provider_info['provider']}?api-version={provider_info['api_version']}"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        # You may want to handle errors differently depending on your use case
        raise Exception(f"Failed to get servers: {response.status_code} - {response.text}")
    
    servers = response.json().get("value", [])
    server_list = []
    for server in servers:
        server_name = server.get("name")
        resource_group = server.get('id', '').split('/')[4] if server.get('id') else None
        server_list.append({
            'server_name': server_name,
            'resource_group': resource_group
        })
    
    return server_list

def list_postgres_databases(token, subscription_id, server_name, resource_group):
    """
    Returns a list of database names for a given PostgreSQL server.
    """
    url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DBforPostgreSQL/flexibleServers/{server_name}/databases?api-version=2022-12-01"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    databases = resp.json().get('value', [])
    db_names = [db['name'] for db in databases]
    return db_names
        
        

def list_azure_databases(token, subscription_id, server_name, resource_group, db_type):

    api_versions = {
        'postgresql': '2022-12-01',
        'sql': '2022-11-01-preview',
        'mysql': '2022-12-01',
        'oracle': '2022-01-01'  # Adjust if Oracle API version differs
    }
    
    base_providers = {
        'postgresql': 'Microsoft.DBforPostgreSQL/flexibleServers',
        'sql': 'Microsoft.Sql/servers',
        'mysql': 'Microsoft.DBforMySQL/flexibleServers',
        'oracle': 'Microsoft.Oracle/servers'
    }
    
    db_type_lower = db_type.lower()
    if db_type_lower not in api_versions or db_type_lower not in base_providers:
        raise ValueError(f"Unsupported database type: {db_type}. Supported types: {list(api_versions.keys())}")
    
    api_version = api_versions[db_type_lower]
    provider = base_providers[db_type_lower]
    
    url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/{provider}/{server_name}/databases?api-version={api_version}"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to get databases: {response.status_code} - {response.text}")
    
    databases = response.json().get('value', [])
    db_names = [db['name'] for db in databases]
    return db_names        

def prettify_sql(sql_text):
    url = "https://sqlformat.org/api/v1/format"
    payload = {
        "sql": sql_text,
        "reindent": True,
        "keyword_case": "upper"
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        return response.text  # The formatted SQL
    else:
        raise Exception(f"Formatting API failed with status {response.status_code}: {response.text}")

def generate_html_diff(source, dest):
    """
    Generate side-by-side HTML diff highlighting differences between source and dest SQL.
    """
    source_lines = source.splitlines()
    dest_lines = dest.splitlines()
    html_diff = difflib.HtmlDiff(tabsize=4, wrapcolumn=80).make_table(
        source_lines, dest_lines, fromdesc='Source', todesc='Destination', context=True, numlines=5
    )
    return html_diff

API_VERSION = "2020-12-01"


def get_access_token(tenant_id, client_id, client_secret, scope="https://dev.azuresynapse.net/.default"):
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        'client_id': client_id,
        'scope': scope,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception(f"Token error: {response.text}")


def list_synapse_objects(token, workspace, obj_type):
    url = f"https://{workspace}.dev.azuresynapse.net/{obj_type}s?api-version={API_VERSION}"
    headers = {
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    all_objects = []
    with requests.Session() as session:
        session.headers.update(headers)
        while url:
            resp = session.get(url)
            if resp.status_code == 200:
                data = resp.json()
                all_objects.extend(data.get('value', []))
                url = data.get('nextLink')
            else:
                raise Exception(f"List error: {resp.text}")
    return [obj['name'] for obj in all_objects]

def list_github_contents(token,Synapse_name,repo, branch='main',obj_type=''):
    # path can be empty string for root
    url = f"https://api.github.com/repos/{repo}/contents/{Synapse_name}/{obj_type}"
    params = {'ref': branch}
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, params=params)
    Object_name = []
    if resp.status_code == 200:
        for i in resp.json():
            Object_name.append(i["name"].replace('.json',''))
        return Object_name
    
    else:
        raise Exception(f"Failed to get contents: {resp.text}")
    
def recursive_clean(obj, keys_to_remove):
    if isinstance(obj, dict):
        for key in list(obj.keys()):
            if key in keys_to_remove:
                del obj[key]
            else:
                recursive_clean(obj.get(key), keys_to_remove)
    elif isinstance(obj, list):
        for item in obj:
            recursive_clean(item, keys_to_remove)
            
def get_etag_destination_json_synapse(token, workspace, obj_type, obj_name):
    url = f"https://{workspace}.dev.azuresynapse.net/{obj_type}s/{obj_name}?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        res = resp.json()
        return res['etag']
    

def get_object_json(token, workspace, obj_type, obj_name):
    url = f"https://{workspace}.dev.azuresynapse.net/{obj_type}s/{obj_name}?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        res = resp.json()
        for key in ['type']:
            if key in res:
                del res[key]
        keys_to_remove= ['id', 'etag', 'lastPublishTime']
        recursive_clean(res,keys_to_remove)
        return res
    else:
        raise Exception(f"Get object error: {resp.text}")
    

    

def get_github_json(token,Synapse_name,repo, branch='main',obj_type='',Object_name=''):
    # path can be empty string for root
    if obj_type=='linkedservice':
        obj_type='linkedService'
    url = f"https://api.github.com/repos/{repo}/contents/{Synapse_name}/{obj_type}/{Object_name}.json"
    params = {'ref': branch}
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        decoded_bytes = base64.b64decode(resp.json()['content'])
        decoded_str = decoded_bytes.decode('utf-8')
        data = json.loads(decoded_str)
        return data
    else:
        raise Exception(f"Failed to get contents: {resp.text}")



def commit_json_to_github(token,Synapse_name,repo, branch='main',obj_type='',Object_name='',json_object={},commit_message=''):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    if obj_type=='linkedservice':
        obj_type='linkedService'

    # Step 1: Get SHA of existing file (if it exists)
    get_url = f"https://api.github.com/repos/{repo}/contents/{Synapse_name}/{obj_type}/{Object_name}"

    # Step 2: Prepare content (base64 encode JSON string)
    content_str = json.dumps(json_object, indent=2)
    content_b64 = base64.b64encode(content_str.encode('utf-8')).decode('utf-8')

    # Step 3: Create or update file via PUT
    put_url = get_url
    data = {
        "message": commit_message,
        "content": content_b64,
        "branch": branch
    }

    r = requests.put(put_url, headers=headers, json=data)
    if r.status_code in [200, 201]:
        print(f"Committed file {Synapse_name}/{obj_type.lower()}/{Object_name} to branch {branch} successfully.")
        
    else:
        raise Exception(f"Failed to commit file: {r.status_code} {r.text}")
    
def check_synapse_object_deployed(url,headers,s_json):
    time.sleep(20)
    resp = requests.get(url, headers=headers)
    res = resp.json()
    for key in ['type']:
            if key in res:
                del res[key]
    keys_to_remove= ['id', 'etag', 'lastPublishTime']
    recursive_clean(res,keys_to_remove)
    Source = json.loads(s_json)
    recursive_clean(Source,keys_to_remove)
    if json.dumps(res)== json.dumps(Source):
        return True
    else: False


def deploy_object_json(token, workspace, obj_type, obj_name, json_data):
    url = f"https://{workspace}.dev.azuresynapse.net/{obj_type}s/{obj_name}?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    resp = requests.put(url, headers=headers, json=json_data)
    if resp.status_code in [200,202,201]:
        return True
    else:
        return False
    


app = Flask(__name__)
app.secret_key = 'pv=nrt'

def get_pg_connection(params, access_token=None):
    """
    Connect to PostgreSQL using either username/password or AAD access token (MFA).
    If access_token is provided, it is used as the password with 'Bearer ' prefix.
    """
    conn_args = {
        'host': params['host'],
        'port': params['port'],
        'dbname': params['dbname'],
        'user': params['user'],
        'sslmode': 'require'
    }
    if access_token:
        conn_args['password'] = f"{access_token}"
    else:
        conn_args['password'] = params['password']    
    return psycopg2.connect(**conn_args)

def get_pg_connection_string(params, access_token=None):
    """
    Connect to PostgreSQL using either username/password or AAD access token (MFA).
    If access_token is provided, it is used as the password with 'Bearer ' prefix.
    """
    conn_args = {
        'host': params['host'],
        'port': params['port'],
        'dbname': params['dbname'],
        'user': params['user'],
        'sslmode': 'require'
    }
    if access_token:
        conn_args['password'] = f"{access_token}"
    else:
        conn_args['password'] = params['password']    
    return conn_args

@app.route('/')
def login():
    params = {
                'host': CONFIG_DB_HOST,
                'port': CONFIG_DB_PORT,
                'dbname': CONFIG_DB_NAME,
                'user': CONFIG_DB_USER,
                'password': CONFIG_DB_PASSWORD,
                'sslmode': 'require'
            } 
    conn = get_pg_connection(params)
    cur = conn.cursor()
    cur.execute("SELECT distinct projectname from migration_app.projectinfo;")
    projectnames = [row[0] for row in cur.fetchall()]
    print(projectnames)
    return render_template('login.html', projectnames=projectnames)

@app.route('/login', methods=['POST'])
def index():
    role = request.form.get('role') 
    if role != 'admin' :
        params = {
                'host': CONFIG_DB_HOST,
                'port': CONFIG_DB_PORT,
                'dbname': CONFIG_DB_NAME,
                'user': CONFIG_DB_USER,
                'password': CONFIG_DB_PASSWORD,
                'sslmode': 'require'
            } 
        conn = get_pg_connection(params)
        cur = conn.cursor()
        cur.execute(f"SELECT Count(1) from migration_app.get_userprojectroleresource_mapping('{request.form['Username']}') where projectname = '{request.form['ProjectName']}' and approve_deny = 'Approved' ;")
        count = cur.fetchone()[0]
        cur.execute(f"SELECT resourcename,resourcetype from migration_app.get_userprojectroleresource_mapping('{request.form['Username']}') where projectname = '{request.form['ProjectName']}' and approve_deny = 'Approved';")
        role_resource_raw = cur.fetchall()
        
        if count == 0 and role!= 'admin':
            return render_template('user_request_form.html',username =request.form['Username'], projectname=request.form['ProjectName']) 

        if role != 'admin' and count > 0:
            resource = [{ 'resourcename': row[0], 'resourcetype': row[1] } for row in role_resource_raw]
            Token = get_token()
            SYNAPSE_TOKEN = Token["synapse_token"]
            WORKSPACE_TOKEN = Token['arm_token']
            print(resource)
            return render_template('index.html',SYNAPSE_TOKEN=SYNAPSE_TOKEN,WORKSPACE_TOKEN=WORKSPACE_TOKEN,resource=resource)
    
    if role == 'admin':
        print(request.form)
        flag = request.form.get('flag', '0')
        if flag == '1':
            params = {
                        'host': CONFIG_DB_HOST,
                        'port': CONFIG_DB_PORT,
                        'dbname': CONFIG_DB_NAME,
                        'user': CONFIG_DB_USER,
                        'password': CONFIG_DB_PASSWORD,
                        'sslmode': 'require'
                    } 
            conn = get_pg_connection(params)
            cur = conn.cursor()
            cur.execute("SELECT resourcename from migration_app.resource where resourcetype = 'Database';")
            db_resources = cur.fetchall()

            cur.execute("SELECT resourcename from migration_app.resource where resourcetype = 'ETL';")
            etl_resources = cur.fetchall()
            cur.close()
            conn.close()
            print(db_resources)
            print(etl_resources)
            return render_template('signup.html', db_resources=db_resources, etl_resources=etl_resources)
        else:
            Adminusername = request.form['Adminusername']
            params = {
                        'host': CONFIG_DB_HOST,
                        'port': CONFIG_DB_PORT,
                        'dbname': CONFIG_DB_NAME,
                        'user': CONFIG_DB_USER,
                        'password': CONFIG_DB_PASSWORD,
                        'sslmode': 'require'
                    } 
            conn = get_pg_connection(params)
            cur = conn.cursor()
            cur.execute(f"select  distinct  Projectname,resourcename,git_commit_required,organization_name,reponame from migration_app.get_project_info_admin('{Adminusername}');")
            projects_raw = cur.fetchall()
            
            projects = [
            {"ProjectName": row[0], "ResourceName": row[1], "GitCommitRequired": row[2], "OrganizationName": row[3], "RepoName": row[4]}
            for row in projects_raw
            ]
            cur.close()
            conn.close()
            if projects:
                return render_template('Admin_Resource_Setup.html', projects=projects, Adminusername=Adminusername)

@app.route('/Admin/ProjectResources',methods=['POST'])
def Admin_mapPrtojectResources():
    Adminusername = request.form['Adminusername']
    params = {
                        'host': CONFIG_DB_HOST,
                        'port': CONFIG_DB_PORT,
                        'dbname': CONFIG_DB_NAME,
                        'user': CONFIG_DB_USER,
                        'password': CONFIG_DB_PASSWORD,
                        'sslmode': 'require'
                    } 
    conn = get_pg_connection(params)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(1) count FROM migration_app.get_project_info_admin('{Adminusername}');")
    count = cur.fetchone()
    print(request.form)
    for i in range(1, count[0] + 1):
        index = i - 1
        project_name = request.form.get(f'projects[{index}].ProjectName')
        resource_name = request.form.get(f'projects[{index}].ResourceName')
        git_commit_required = request.form.get(f'projects[{index}].GitCommitRequired')
        organization_name = request.form.get(f'projects[{index}].OrganizationName')
        repo_name = request.form.get(f'projects[{index}].RepoName')
        print(project_name, resource_name, git_commit_required, organization_name, repo_name)
        cur.execute(f"call migration_app.sp_insert_user_project_admin_info('{Adminusername}'::character varying, '{project_name}'::character varying, '{resource_name}'::character varying, '{git_commit_required}'::boolean, '{organization_name}'::character varying, '{repo_name}'::character varying);")
        conn.commit()

    cur.execute(f"SELECT distinct projectname,username,rolename,resourcename,approve_deny From migration_app.get_project_info_admin_user('{Adminusername}');");
    projects_raw = cur.fetchall()
    projects = [
            {"ProjectName": row[0], "username": row[1], "rolename": row[2], "resourcename": row[3], "Approve_Deny": row[4]}
            for row in projects_raw
            ]
    cur.execute("Select rolename from migration_app.role")
    rolenames = cur.fetchall()
    rolenames = [role[0] for role in rolenames]

    cur.execute("Select ResourceName from migration_App.resource A inner join migration_app.projectResource B on A.resourceid = B.resourceid inner join migration_app.admininfo C on B.projectid = C.projectid where C.username = %s;", (Adminusername,))
    resourcenames = cur.fetchall()
    resourcenames = [resource[0] for resource in resourcenames]

    cur.close()
    conn.close()
    return render_template('user_dashboard.html', Adminusername=Adminusername, projects=projects, rolenames=rolenames, resourcenames=resourcenames)

@app.route('/map_user_roles', methods=['POST'])
def map_user_roles():
    Adminusername = request.form['Adminusername']
    params = {
                        'host': CONFIG_DB_HOST,
                        'port': CONFIG_DB_PORT,
                        'dbname': CONFIG_DB_NAME,
                        'user': CONFIG_DB_USER,
                        'password': CONFIG_DB_PASSWORD,
                        'sslmode': 'require'
                    } 
    conn = get_pg_connection(params)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(1) count FROM migration_app.get_project_info_admin_user('{Adminusername}');")
    count = cur.fetchone()
    print(request.form)
    for i in range(1, count[0] + 1):
        index = i
        project_name = request.form.get(f'projectname_{index}')
        username = request.form.get(f'username_{index}')
        rolename = request.form.get(f'rolename_{index}')
        resourcename = request.form.get(f'resourcename_{index}')
        Approve_Deny = request.form.get(f'Approve_Deny_{index}')
        cur.execute(f"call migration_app.sp_map_user_role_resources( '{username}', '{project_name}', '{rolename}', '{resourcename}', '{Approve_Deny}' )")
        conn.commit()
        print(project_name, username, rolename, resourcename, Approve_Deny)
    cur.close()
    conn.close()
    return redirect(url_for('login'))

@app.route('/user_request_form', methods=['POST'])
def user_request_form():
    username = request.form['username']
    projectname = request.form['projectname']
    params = {
        'host': CONFIG_DB_HOST,
        'port': CONFIG_DB_PORT,
        'dbname': CONFIG_DB_NAME,
        'user': CONFIG_DB_USER,
        'password': CONFIG_DB_PASSWORD,
        'sslmode': 'require'
    }
    conn = get_pg_connection(params)
    cur = conn.cursor()
    cur.execute(f"call migration_app.sp_insert_user('{username}', '{projectname}');")
    conn.commit()
    cur.close()
    conn.close()
    return redirect(url_for('login'))

@app.route('/api/admin/login', methods=['POST'])
def admin_login_check():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    params = {
    'host': CONFIG_DB_HOST,
    'port': CONFIG_DB_PORT,
    'dbname': CONFIG_DB_NAME,
    'user': CONFIG_DB_USER,
    'password': CONFIG_DB_PASSWORD,
    'sslmode': 'require'
    } 
    conn = get_pg_connection(params)
    cur = conn.cursor()
    cur.execute("SELECT * FROM migration_app.admininfo WHERE username = %s AND password = %s", (username, password))
    admin = cur.fetchone()
    if admin:
        print("Login successful")
        return jsonify({"success": True, "message": "Login successful"})
    else:
        print("Invalid credentials")
        return jsonify({"success": False, "message": "Invalid credentials"})


@app.route('/admin/signup',methods = ['POST'])
def Admin_Signup():
    params = {
    'host': CONFIG_DB_HOST,
    'port': CONFIG_DB_PORT,
    'dbname': CONFIG_DB_NAME,
    'user': CONFIG_DB_USER,
    'password': CONFIG_DB_PASSWORD,
    'sslmode': 'require'
    } 
    conn = get_pg_connection(params)
    cur = conn.cursor()
    print(request.form)
    username = request.form['AdminSignupusername']
    password = request.form['AdminsignupPassword']
    officialEmail = request.form['officialEmail']
    companyName = request.form['companyName']
    TeamName = request.form['TeamName']
    projectName = request.form['projectName']
    projectDescription = request.form['projectDescription']
    dbResources = request.form['dbResources']
    etlResources = request.form['etlResources']
    blobName =request.form['blobName']
    containerName = request.form['containerName']
    accountKey = request.form['accountKey']
    print(dbResources,etlResources)
    cur.execute(f"call migration_app.sp_admininfo_dataload('{username}'::character varying, '{password}'::character varying, '{officialEmail}'::character varying, '{companyName}'::character varying, '{TeamName}'::character varying, '{projectName}'::character varying, '{projectDescription}'::character varying, '{dbResources}'::text, '{etlResources}'::text, '{blobName}'::text, '{containerName}'::text, '{accountKey}'::text)")
    print(f"call migration_app.sp_admininfo_dataload('{username}'::character varying, '{password}'::character varying, '{officialEmail}'::character varying, '{companyName}'::character varying, '{TeamName}'::character varying, '{projectName}'::character varying, '{projectDescription}'::character varying, '{dbResources}'::text, '{etlResources}'::text, '{blobName}'::text, '{containerName}'::text, '{accountKey}'::text)")
    conn.commit()
    cur.close()
    conn.close()
    return redirect(url_for('login'))


@app.route('/handle_deployment', methods=['POST'])
def handle_deployment():
    deployment = request.form.get('deployment')
    logger.info(f"Log file created at: {STORAGE_CONTAINER_NAME}/Logs/{datetime.now().strftime('%Y-%m-%d')}.log")
    logger_array.append(log_stream.getvalue())
    if deployment == 'Postgres Deployment':
        SYNAPSE_TOKEN = request.form['SYNAPSE_TOKEN']
        WORKSPACE_TOKEN = request.form['WORKSPACE_TOKEN']
        logger.info("ARM token received")
        logger_array.append(log_stream.getvalue())
        SUBSCRIPTION_LIST = list_subscriptions(WORKSPACE_TOKEN)
        logger.info("Fetched subscription list")
        logger_array.append(log_stream.getvalue())
        SUBSCRIPTION_ARRAY =[]
        

        for SUBSCRIPTION_LIST in SUBSCRIPTION_LIST.get("value", []):
                SUBSCRIPTION_JSON ={}
                SUBSCRIPTION_JSON['subscription_id']= SUBSCRIPTION_LIST['subscriptionId']
                SUBSCRIPTION_JSON['tenant_id'] = SUBSCRIPTION_LIST['tenantId']
                SUBSCRIPTION_JSON['subscription_name']= SUBSCRIPTION_LIST['displayName']
                servers = list_postgres_servers(WORKSPACE_TOKEN, SUBSCRIPTION_LIST['subscriptionId'])
                postgres_server= []
                for server in servers:
                    server_json ={}
                    server_name = server['server_name']
                    resource_group = server['resource_group']
                    databases = list_postgres_databases(WORKSPACE_TOKEN, SUBSCRIPTION_LIST['subscriptionId'], server_name, resource_group)
                    server_json['server_name'] = server_name
                    server_json['resource_group'] = resource_group
                    server_json['databases'] = databases
                    postgres_server.append(server_json)
                    
                SUBSCRIPTION_JSON['server_list'] = postgres_server
                SUBSCRIPTION_ARRAY.append(SUBSCRIPTION_JSON)
        logger.info("Processed subscription data for Postgres DB Deployment")
        logger_array.append(log_stream.getvalue())
        return render_template('step1.html',WORKSPACE_TOKEN=WORKSPACE_TOKEN,SYNAPSE_TOKEN=SYNAPSE_TOKEN,subscription_data= (SUBSCRIPTION_ARRAY))
    
    elif deployment == 'Azure Synapse Deployment':
        
        SYNAPSE_TOKEN = request.form['SYNAPSE_TOKEN']
        WORKSPACE_TOKEN = request.form['WORKSPACE_TOKEN']
        logger.info("Synapse token received")
        logger_array.append(log_stream.getvalue())
        SUBSCRIPTION_LIST = list_subscriptions(WORKSPACE_TOKEN)
        logger.info("Fetched subscription list for Synapse Deployment")
        logger_array.append(log_stream.getvalue())
        SUBSCRIPTION_ARRAY =[]
        

        for SUBSCRIPTION_LIST in SUBSCRIPTION_LIST.get("value", []):
                SUBSCRIPTION_JSON ={}
                SUBSCRIPTION_JSON['subscription_id']= SUBSCRIPTION_LIST['subscriptionId']
                SUBSCRIPTION_JSON['tenant_id'] = SUBSCRIPTION_LIST['tenantId']
                SUBSCRIPTION_JSON['subscription_name']= SUBSCRIPTION_LIST['displayName']
                workspaces =list_synapse_workspaces(WORKSPACE_TOKEN, SUBSCRIPTION_LIST['subscriptionId'])
                
                workspace_array=[]
                
                for ws in workspaces.get("value", []):
                    workspace_json ={}
                    Synapse_workspace = ws['name']
                    Resource_Group = ws['id'].split('/')[4]
                    GIT_PROPERTIES = synapse_workspace_properties(Synapse_workspace, SYNAPSE_TOKEN)
                    workspace_json['Synapse_workspace']= Synapse_workspace
                    if GIT_PROPERTIES != None:
                        #workspace_array.append(Synapse_workspace)
                        repo_config = GIT_PROPERTIES.get("properties", {}).get("workspaceRepositoryConfiguration")
                        if repo_config:
                            
                            RepoName = repo_config['accountName']+'/'+repo_config['repositoryName']
                            FolderName = repo_config['rootFolder']
                            flag =1
                            
                        else:
                            RepoName=''
                            FolderName=''
                            flag =0
                            
                        
                        workspace_json['RepoName']=RepoName
                        workspace_json['FolderName']=FolderName
                        workspace_json['flag']=flag
                        workspace_array.append(workspace_json)    
                SUBSCRIPTION_JSON['workspace_list'] = workspace_array     
                SUBSCRIPTION_ARRAY.append(SUBSCRIPTION_JSON)
        logger.info("Processed subscription data for Synapse Deployment")
        logger_array.append(log_stream.getvalue())
        return render_template('step1_credentials.html',subscription_data=json.dumps(SUBSCRIPTION_ARRAY),SYNAPSE_TOKEN=SYNAPSE_TOKEN
                            ,WORKSPACE_TOKEN= WORKSPACE_TOKEN)
    else:
        logger.error("Invalid deployment selection")
        logger_array.append(log_stream.getvalue())
        return "Invalid selection", 400




@app.route('/step1_synapse')
def step1_synapse():
    # Render form for credentials and CRNumber input
    return render_template('step1_credentials.html')


@app.route('/step2_synapse', methods=['POST'])
def step2_synapse():
    # Receive credentials and CRNumber via POST from step1
    try:
        sourceWorkspace =''
        sourceRepoName =''
        sourceFolderName =''
        sourceBranch = ''
        sourcePAT = ''
        destWorkspace =''
        destRepoName =''
        destFolderName =''
        destBranch = ''
        destPAT = ''
        sourceTenantId = ''
        sourceWorkspace = ''
        destTenantId = ''
        destWorkspace = ''
        crNumber = request.form['crNumber']
        
        
        
        logger.info(f"Starting Synapse migration process for CR Number: {crNumber}")
        logger_array.append(log_stream.getvalue())
        
        SYNAPSE_TOKEN = request.form['SYNAPSE_TOKEN']
        source_objects = {}
        dest_objects = {}
        if not('-dev-' in request.form['sourceWorkspace']) and not('-dev-' in request.form['destWorkspace']):
            sourceTenantId = request.form['sourceTenantId']
            sourceWorkspace = request.form['sourceWorkspace']
            object_types = ['linkedService', 'dataset', 'dataflow', 'pipeline', 'trigger']
            s_token = SYNAPSE_TOKEN
            destTenantId = request.form['destTenantId']
            destWorkspace = request.form['destWorkspace']
            object_types = ['linkedService', 'dataset', 'dataflow', 'pipeline', 'trigger']
            d_token = SYNAPSE_TOKEN
            for t in object_types:
                dest_objects[t] = list_synapse_objects(d_token, destWorkspace, t)
                dest_objects_json = json.dumps(dest_objects)
                source_objects[t] = list_synapse_objects(s_token, sourceWorkspace, t)
                source_objects_json = json.dumps(source_objects)
                
            
        
        if not('-dev-' in request.form['destWorkspace']) and ('-dev-' in request.form['sourceWorkspace']):
            destTenantId = request.form['destTenantId']
            destWorkspace = request.form['destWorkspace']
            object_types = ['linkedService', 'dataset', 'dataflow', 'pipeline', 'trigger']
            d_token = SYNAPSE_TOKEN
            for t in object_types:
                dest_objects[t] = list_synapse_objects(d_token, destWorkspace, t)
                dest_objects_json = json.dumps(dest_objects)
            sourceWorkspace = request.form['sourceWorkspace']
            sourceRepoName = request.form['sourceRepoName']
            sourceFolderName = request.form['sourceFolderName']
            sourceBranch = request.form['sourceBranch']
            sourcePAT = request.form['sourcePAT']
            object_types = ['linkedService', 'dataset', 'dataflow', 'pipeline', 'trigger']
            for t in object_types:
                source_objects[t] = list_github_contents(sourcePAT,sourceFolderName,sourceRepoName,sourceBranch,t)
                source_objects_json = json.dumps(source_objects)
        
        if not('-dev-' in request.form['sourceWorkspace']) and ('-dev-' in request.form['destWorkspace']):
            sourceTenantId = request.form['sourceTenantId']
            sourceWorkspace = request.form['sourceWorkspace']
            object_types = ['linkedService', 'dataset', 'dataflow', 'pipeline', 'trigger']
            s_token = SYNAPSE_TOKEN
            for t in object_types:
                source_objects[t] = list_synapse_objects(s_token, sourceWorkspace, t)
                source_objects_json = json.dumps(source_objects)
            destRepoName = request.form['destRepoName']
            destWorkspace = request.form['destWorkspace']
            destFolderName = request.form['destFolderName']
            destBranch = request.form['destBranch']
            destPAT = request.form['destPAT']
            object_types = ['linkedService', 'dataset', 'dataflow', 'pipeline', 'trigger']
            for t in object_types:
                dest_objects[t] = list_github_contents(destPAT,destFolderName,destRepoName,destBranch,t)
                dest_objects_json = json.dumps(dest_objects)    

        if ('-dev-' in request.form['destWorkspace']) and ('-dev-' in request.form['sourceWorkspace']):
            destWorkspace = request.form['destWorkspace']
            destRepoName = request.form['destRepoName']
            destFolderName = request.form['destFolderName']
            destBranch = request.form['destBranch']
            destPAT = request.form['destPAT']
            sourceWorkspace = request.form['sourceWorkspace']
            sourceRepoName = request.form['sourceRepoName']
            sourceFolderName = request.form['sourceFolderName']
            sourceBranch = request.form['sourceBranch']
            sourcePAT = request.form['sourcePAT']
            object_types = ['linkedService', 'dataset', 'dataflow', 'pipeline', 'trigger']
            for t in object_types:
                dest_objects[t] = list_github_contents(destPAT,destFolderName,destRepoName,destBranch,t)
                dest_objects_json = json.dumps(dest_objects)
                source_objects[t] = list_github_contents(sourcePAT,sourceFolderName,sourceRepoName,sourceBranch,t)
                source_objects_json = json.dumps(source_objects)
        
        logger.info(f"Fetched Synapse objects for source workspace: {sourceWorkspace} and destination workspace: {destWorkspace}")
        logger_array.append(log_stream.getvalue())
        
        return render_template('step2_select_objects.html',
                               source_objects=source_objects,
                               dest_objects=dest_objects,
                               object_types=object_types,
                               # Pass credentials and crNumber as hidden fields in the form
                               sourceTenantId=sourceTenantId,
                               SYNAPSE_TOKEN = SYNAPSE_TOKEN,
                               sourceWorkspace=sourceWorkspace,
                               destTenantId=destTenantId,
                               destWorkspace=destWorkspace,
                               sourceRepoName =sourceRepoName,
                               sourceFolderName =sourceFolderName,
                               sourceBranch = sourceBranch,
                               sourcePAT = sourcePAT,
                               destRepoName =destRepoName,
                               destFolderName =destFolderName,
                               destBranch = destBranch,
                               destPAT = destPAT,
                               crNumber=crNumber,
                               source_objects_json=source_objects_json,
                               dest_objects_json=dest_objects_json
                               )
    except Exception as e:
        logger.error(f"Error in Fetching the Synapse objects: {e}")
        logger_array.append(log_stream.getvalue())
        return f"Error fetching objects: {e}", 500


@app.route('/step3_synapse', methods=['POST'])
def step3_synapse():
    try:
        # Extract credentials & crNumber from form hidden fields
        
        sourceTenantId = request.form['sourceTenantId']
        SYNAPSE_TOKEN = request.form['SYNAPSE_TOKEN']
        sourceWorkspace = request.form['sourceWorkspace']

        destTenantId = request.form['destTenantId']
        
        destWorkspace = request.form['destWorkspace']

        destRepoName = request.form['destRepoName']
        destFolderName = request.form['destFolderName']
        destBranch = request.form['destBranch']
        destPAT = request.form['destPAT']
        
        sourceRepoName = request.form['sourceRepoName']
        sourceFolderName = request.form['sourceFolderName']
        sourceBranch = request.form['sourceBranch']
        sourcePAT = request.form['sourcePAT']

        crNumber = request.form['crNumber']
        
        # Extract object lists JSON (for potential use, optional)
        source_objects = json.loads(request.form['source_objects_json'])
        dest_objects = json.loads(request.form['dest_objects_json'])
        object_types = request.form.getlist('objectType[]')
        object_names = request.form.getlist('objectName[]')
        orders = request.form.getlist('order[]')

        
        
        migration_plan = []
        for i in range(0,len(object_types)):
                    migration_plan.append({
                        'objectType': object_types[i],
                        'objectName': object_names[i],
                        'order':  orders[i]
                    })

        mig_plan = []
        # Get tokens
        comparisons = []
        if not('-dev-' in request.form['sourceWorkspace']) and not('-dev-' in request.form['destWorkspace']):
            
            s_token = SYNAPSE_TOKEN
            d_token = SYNAPSE_TOKEN

        # Compare source and destination JSONs for selected objects
        
            for obj in migration_plan:
                obj_type = obj['objectType']
                obj_name = obj['objectName']
                
                
                s_json = get_object_json(s_token, sourceWorkspace, obj_type, obj_name)
                try:
                    d_json = get_object_json(d_token, destWorkspace, obj_type, obj_name)
                except:
                    d_json = None
                keys_to_remove= ['id', 'etag', 'lastPublishTime']
                for key in ['type']:
                    if key in s_json:
                        del s_json[key]
                recursive_clean(s_json,keys_to_remove)   
        
                if d_json:
                    for key in ['type']:
                        if key in d_json:
                            del d_json[key]
                    recursive_clean(d_json,keys_to_remove)
        
                diffs = "Different" if json.dumps(s_json, sort_keys=True) != json.dumps(d_json, sort_keys=True if d_json else True) else "Identical"
            
                SAS_URL_Source_Synapse, SAS_URL_dest_Synapse = save_synapse_json_to_storage(crNumber, obj['objectName'], s_json, d_json , logger)
                mig_plan.append({
                        'objectType': obj_type,
                        'objectName': obj_name,
                        'order':  obj['order'],
                        'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse,
                        'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse
                    })
                comparisons.append({
                'objectType': obj_type,
                'objectName': obj_name,
                'diff': diffs,
                'sourceJson': json.dumps(s_json, indent=2),
                'destJson': json.dumps(d_json, indent=2) if d_json else "Not present",
                'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse,
                'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse
                })
                
        
        if not('-dev-' in request.form['sourceWorkspace']) and ('-dev-' in request.form['destWorkspace']):
            
            s_token = SYNAPSE_TOKEN
            # d_token = get_access_token(destTenantId, destClientId, destClientSecret)
            
        # Compare source and destination JSONs for selected objects
        
            for obj in migration_plan:
                obj_type = obj['objectType']
                obj_name = obj['objectName']
            
                s_json = get_object_json(s_token, sourceWorkspace, obj_type, obj_name)
                try:
                    d_json = get_github_json(destPAT,destFolderName,destRepoName,destBranch,obj_type,obj_name) 
                except:
                    d_json = None
                keys_to_remove= ['id', 'etag', 'lastPublishTime']
                for key in ['type']:
                    if key in s_json:
                        del s_json[key]
                recursive_clean(s_json,keys_to_remove)   
        
                if d_json:
                    for key in ['type']:
                        if key in d_json:
                            del d_json[key]
                    recursive_clean(d_json,keys_to_remove)
        
                diffs = "Different" if json.dumps(s_json, sort_keys=True) != json.dumps(d_json, sort_keys=True if d_json else True) else "Identical"
                SAS_URL_Source_Synapse, SAS_URL_dest_Synapse = save_synapse_json_to_storage(crNumber, obj['objectName'], s_json, d_json if d_json is not None else '', logger)
                mig_plan.append({
                        'objectType': obj_type,
                        'objectName': obj_name,
                        'order':  obj['order'],
                        'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse,
                        'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse
                    })
                comparisons.append({
                'objectType': obj_type,
                'objectName': obj_name,
                'diff': diffs,
                'sourceJson': json.dumps(s_json, indent=2),
                'destJson': json.dumps(d_json, indent=2) if d_json else "Not present",
                'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse,
                'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse
                })
                
        if ('-dev-' in request.form['sourceWorkspace']) and not('-dev-' in request.form['destWorkspace']):
            #s_token = get_access_token(sourceTenantId, sourceClientId, sourceClientSecret)
            
            d_token = SYNAPSE_TOKEN
            
        # Compare source and destination JSONs for selected objects
        
            for obj in migration_plan:
                obj_type = obj['objectType']
                obj_name = obj['objectName']
            
                s_json = get_github_json(sourcePAT,sourceFolderName,sourceRepoName,sourceBranch,obj_type,obj_name)
                try:
                    d_json = get_object_json(d_token, destWorkspace, obj_type, obj_name)
                except:
                    d_json = None
                keys_to_remove= ['id', 'etag', 'lastPublishTime']
                for key in ['type']:
                    if key in s_json:
                        del s_json[key]
                recursive_clean(s_json,keys_to_remove)   

                if d_json:
                    for key in ['type']:
                        if key in d_json:
                            del d_json[key]
                    recursive_clean(d_json,keys_to_remove)
        
                diffs = "Different" if json.dumps(s_json, sort_keys=True) != json.dumps(d_json, sort_keys=True if d_json else True) else "Identical"
                SAS_URL_Source_Synapse, SAS_URL_dest_Synapse = save_synapse_json_to_storage(crNumber, obj['objectName'], s_json, d_json if d_json is not None else '', logger)
                mig_plan.append({
                        'objectType': obj_type,
                        'objectName': obj_name,
                        'order':  obj['order'],
                        'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse,
                        'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse
                    })
                comparisons.append({
                'objectType': obj_type,
                'objectName': obj_name,
                'diff': diffs,
                'sourceJson': json.dumps(s_json, indent=2),
                'destJson': json.dumps(d_json, indent=2) if d_json else "Not present",
                'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse,
                'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse
                })
                
        if ('-dev-' in request.form['sourceWorkspace']) and ('-dev-' in request.form['destWorkspace']):
            #s_token = get_access_token(sourceTenantId, sourceClientId, sourceClientSecret)
            #d_token = get_access_token(destTenantId, destClientId, destClientSecret)
            
        # Compare source and destination JSONs for selected objects
        
            for obj in migration_plan:
                obj_type = obj['objectType']
                obj_name = obj['objectName']
            
                s_json = get_github_json(sourcePAT,sourceFolderName,sourceRepoName,sourceBranch,obj_type,obj_name)
                try:
                    d_json = get_github_json(destPAT,destFolderName,destRepoName,destBranch,obj_type,obj_name)
                except:
                    d_json = None
                keys_to_remove= ['id', 'etag', 'lastPublishTime']
                for key in ['type']:
                    if key in s_json:
                        del s_json[key]
                recursive_clean(s_json,keys_to_remove)   
        
                if d_json:
                    for key in ['type']:
                        if key in d_json:
                            del d_json[key]
                    recursive_clean(d_json,keys_to_remove)
        
                diffs = "Different" if json.dumps(s_json, sort_keys=True) != json.dumps(d_json, sort_keys=True if d_json else True) else "Identical"
                SAS_URL_Source_Synapse, SAS_URL_dest_Synapse = save_synapse_json_to_storage(crNumber, obj['objectName'], s_json, d_json if d_json is not None else '', logger)
                mig_plan.append({
                        'objectType': obj_type,
                        'objectName': obj_name,
                        'order':  obj['order'],
                        'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse,
                        'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse
                    })
                comparisons.append({
                'objectType': obj_type,
                'objectName': obj_name,
                'diff': diffs,
                'sourceJson': json.dumps(s_json, indent=2),
                'destJson': json.dumps(d_json, indent=2) if d_json else "Not present",
                'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse,
                'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse
                })
                
        
        # Prepare migration_plan and credentials as JSON strings for passing forward
        migration_plan_json = json.dumps(mig_plan)
        
        
        
        return render_template('step3_compare.html',
                               comparisons=comparisons,
                               sourceTenantId=sourceTenantId,
                               SYNAPSE_TOKEN = SYNAPSE_TOKEN,
                               sourceWorkspace=sourceWorkspace,
                               destTenantId=destTenantId,
                               destWorkspace=destWorkspace,
                               sourceRepoName =sourceRepoName,
                               sourceFolderName =sourceFolderName,
                               sourceBranch = sourceBranch,
                               sourcePAT = sourcePAT,
                               destRepoName =destRepoName,
                               destFolderName =destFolderName,
                               destBranch = destBranch,
                               destPAT = destPAT,
                               crNumber=crNumber,
                               migration_plan_json=migration_plan_json,
                               s_json= s_json,
                               d_json=d_json)
    except Exception as e:
        logger.error(f"Error comparing Synapse objects: {e}")
        return f"Error comparing objects: {e}", 500


@app.route('/step4_synapse', methods=['POST'])
def step4_synapse():
    try:
        # Extract credentials & crNumber from form
        
        sourceTenantId = request.form['sourceTenantId']
        SYNAPSE_TOKEN = request.form['SYNAPSE_TOKEN']
        sourceWorkspace = request.form['sourceWorkspace']
        destTenantId = request.form['destTenantId']
        destWorkspace = request.form['destWorkspace']
        
        destRepoName = request.form['destRepoName']
        destFolderName = request.form['destFolderName']
        destBranch = request.form['destBranch']
        destPAT = request.form['destPAT']
        
        sourceRepoName = request.form['sourceRepoName']
        sourceFolderName = request.form['sourceFolderName']
        sourceBranch = request.form['sourceBranch']
        sourcePAT = request.form['sourcePAT']

        crNumber = request.form['crNumber']
        
        
        # Extract migration plan JSON and parse
        migration_plan = json.loads(request.form['migration_plan_json'])

        

        migration_plan = sorted(migration_plan, key=lambda x: x['order'])

        results = []
        
        
        
        if not('-dev-' in request.form['sourceWorkspace']) and not('-dev-' in request.form['destWorkspace']):
            s_token = SYNAPSE_TOKEN
            d_token = SYNAPSE_TOKEN
            for obj in migration_plan:
                obj_type = obj['objectType']
                obj_name = obj['objectName']
                SAS_URL_Source_Synapse = obj.get('SAS_URL_Source_Synapse', '')
                SAS_URL_dest_Synapse = obj.get('SAS_URL_Dest_Synapse', '')
                s_json = get_object_json(s_token, sourceWorkspace, obj_type, obj_name)
                # Backup source json
                keys_to_remove= ['id', 'etag', 'lastPublishTime']
                for key in ['type']:
                    if key in s_json:
                        del s_json[key]
                recursive_clean(s_json,keys_to_remove)   
                
                

                try:
                    d_json = get_object_json(d_token, destWorkspace, obj_type, obj_name)
                    etag=get_etag_destination_json_synapse(d_token, destWorkspace, obj_type, obj_name)

                except:
                # Destination object not exist, skip backup
                    pass
                try:
                    deploy_object_json(d_token, destWorkspace, obj_type, obj_name, s_json)
                    results.append({'objectName': obj_name, 'objectType': obj_type, 'status': 'Success', 'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse, 'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse if SAS_URL_dest_Synapse is not None else ''})
                    logger.info(f"Successfully deployed {obj_type} {obj_name} to {destWorkspace}")
                    logger_array.append(log_stream.getvalue())
                except Exception as e:
                    logger.error(f"Failed to deploy {obj_type} {obj_name} to {destWorkspace}: {e}")
                    logger_array.append(log_stream.getvalue())
                    results.append({'objectName': obj_name, 'objectType': obj_type, 'status': f'Failed: {e}', 'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse, 'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse if SAS_URL_dest_Synapse is not None else ''})
        
        if not('-dev-' in request.form['sourceWorkspace']) and ('-dev-' in request.form['destWorkspace']):
            s_token = SYNAPSE_TOKEN
            # d_token = get_access_token(destTenantId, destClientId, destClientSecret)

        # Compare source and destination JSONs for selected objects
        
            for obj in migration_plan:
                obj_type = obj['objectType']
                obj_name = obj['objectName']
                SAS_URL_Source_Synapse = obj.get('SAS_URL_Source_Synapse', '')
                SAS_URL_dest_Synapse = obj.get('SAS_URL_Dest_Synapse', '')
                s_json = get_object_json(s_token, sourceWorkspace, obj_type, obj_name)
                try:
                    d_json = get_github_json(destPAT,destFolderName,destRepoName,destBranch,obj_type,obj_name)    
                except:
                # Destination object not exist, skip backup
                    pass  
                try:
                    commit_message = f"Committing {obj_type} {obj_name} as part of {crNumber}"
                    commit_json_to_github(destPAT,destFolderName,destRepoName,destBranch,obj_type,obj_name,s_json,commit_message)
                    results.append({'objectName': obj_name, 'objectType': obj_type, 'status': 'Success', 'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse,  'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse if SAS_URL_dest_Synapse is not None else ''})
                    logger.info(f"Successfully committed {obj_type} {obj_name} to {destRepoName}/{destFolderName} on branch {destBranch}")
                    logger_array.append(log_stream.getvalue())
                except Exception as e:
                    results.append({'objectName': obj_name, 'objectType': obj_type, 'status': f'Failed: {e}', 'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse , 'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse if SAS_URL_dest_Synapse is not None else ''})
                    logger.error(f"Failed to commit {obj_type} {obj_name} to {destRepoName}/{destFolderName} on branch {destBranch}: {e}")
                    logger_array.append(log_stream.getvalue())
        
        if ('-dev-' in request.form['sourceWorkspace']) and not('-dev-' in request.form['destWorkspace']):
            #s_token = get_access_token(sourceTenantId, sourceClientId, sourceClientSecret)
            d_token = SYNAPSE_TOKEN

        # Compare source and destination JSONs for selected objects
        
            for obj in migration_plan:
                obj_type = obj['objectType']
                obj_name = obj['objectName']
                SAS_URL_Source_Synapse = obj.get('SAS_URL_Source_Synapse', '')
                SAS_URL_dest_Synapse = obj.get('SAS_URL_Dest_Synapse', '')
                s_json = get_github_json(sourcePAT,sourceFolderName,sourceRepoName,sourceBranch,obj_type,obj_name)
                keys_to_remove= ['id', 'etag', 'lastPublishTime']
                for key in ['type']:
                    if key in s_json:
                        del s_json[key]
                recursive_clean(s_json,keys_to_remove)
                
                try:
                    d_json = get_object_json(d_token, destWorkspace, obj_type, obj_name)
                    etag=get_etag_destination_json_synapse(d_token, destWorkspace, obj_type, obj_name)   
                except:
                # Destination object not exist, skip backup
                    pass  
                try:
                    commit_message = f"Committing {obj_type} {obj_name} as part of {crNumber}"
                    deploy_object_json(d_token, destWorkspace, obj_type, obj_name, s_json)
                    results.append({'objectName': obj_name, 'objectType': obj_type, 'status': 'Success', 'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse, 'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse if SAS_URL_dest_Synapse is not None else ''})
                    logger.info(f"Successfully deployed {obj_type} {obj_name} to {destWorkspace}")
                    logger_array.append(log_stream.getvalue())
                except Exception as e:
                    results.append({'objectName': obj_name, 'objectType': obj_type, 'status': f'Failed: {e}', 'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse, 'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse if SAS_URL_dest_Synapse is not None else ''})
                    logger.error(f"Failed to deploy {obj_type} {obj_name} to {destWorkspace}: {e}")
                    logger_array.append(log_stream.getvalue())
                    
        if ('-dev-' in request.form['sourceWorkspace']) and ('-dev-' in request.form['destWorkspace']):
            #s_token = get_access_token(sourceTenantId, sourceClientId, sourceClientSecret)
            #d_token = get_access_token(destTenantId, destClientId, destClientSecret)
            
        # Compare source and destination JSONs for selected objects
        
            for obj in migration_plan:
                
                obj_type = obj['objectType']
                obj_name = obj['objectName']
                SAS_URL_Source_Synapse = obj.get('SAS_URL_Source_Synapse', '')
                SAS_URL_dest_Synapse = obj.get('SAS_URL_Dest_Synapse', '')
                s_json = get_github_json(sourcePAT,sourceFolderName,sourceRepoName,sourceBranch,obj_type,obj_name)
                try:
                    d_json = get_github_json(destPAT,destFolderName,destRepoName,destBranch,obj_type,obj_name)     
                except:
                # Destination object not exist, skip backup
                    pass  
                try:
                    commit_message = f"Committing {obj_type} {obj_name} as part of {crNumber}"
                    commit_json_to_github(destPAT,destFolderName,destRepoName,destBranch,obj_type,obj_name,s_json,commit_message)
                    results.append({'objectName': obj_name, 'objectType': obj_type, 'status': 'Success', 'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse, 'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse if SAS_URL_dest_Synapse is not None else ''})
                    logger.info(f"Successfully committed {obj_type} {obj_name} to {destRepoName}/{destFolderName} on branch {destBranch}")
                    logger_array.append(log_stream.getvalue())
                except Exception as e:
                    results.append({'objectName': obj_name, 'objectType': obj_type, 'status': f'Failed: {e}', 'SAS_URL_Source_Synapse': SAS_URL_Source_Synapse, 'SAS_URL_Dest_Synapse': SAS_URL_dest_Synapse if SAS_URL_dest_Synapse is not None else ''})
                    logger.error(f"Failed to commit {obj_type} {obj_name} to {destRepoName}/{destFolderName} on branch {destBranch}: {e}")
                    logger_array.append(log_stream.getvalue())

        logs = '\n'.join(logger_array)
        # print("Captured logs:\n", logs)
        # Save logs to a file
        upload_blob(STORAGE_ACCOUNT_NAME,STORAGE_ACCOUNT_KEY_BASE64,STORAGE_CONTAINER_NAME, f'{crNumber}/Synapse_migration_logs.txt', logs)
        logs_sas_url_synapse= generate_sas_token(STORAGE_ACCOUNT_NAME,STORAGE_ACCOUNT_KEY_BASE64,STORAGE_CONTAINER_NAME,f'{crNumber}/Synapse_migration_logs.txt')
        results.append({'objectName': 'Logs', 'objectType': 'Synapse Migration Logs', 'status': 'Saved to Azure Blob Storage', 'SAS_URL_Source_Synapse': logs_sas_url_synapse, 'SAS_URL_Dest_Synapse': ''})
        # To clear logs from the buffer for reuse
        

        return render_template('step4_results.html', results=results)
    except Exception as e:
        logger.error(f"Error during migration step 4: {e}")
        logger_array.append(log_stream.getvalue())
        return f"Migration error: {e}", 500
    



# @app.route('/step1', methods=['GET'])
# def step1():
#     return render_template('step1.html')


    
# This route serves the step2 page as before (modified to include JS)
@app.route('/step2', methods=['POST'])
def step2():
    
    
    if request.form['source_auth_type'] == 'mfa':
        DB_TOKEN = get_token_db()
        logger.info("MFA token received for source database connection")
        logger_array.append(log_stream.getvalue())
        source_conn_params = {
            'host': f"{request.form['source_server']}.postgres.database.azure.com",
            'port': 5432,
            'dbname': request.form['source_dbname'],
            'user': request.form['source_user']
            }
        dest_conn_params = {
            'host': f"{request.form['dest_server']}.postgres.database.azure.com",
            'port': 5432,
            'dbname': request.form['dest_dbname'],
            'user': request.form['dest_user']
            }
        
        source_conn = get_pg_connection(source_conn_params,DB_TOKEN)
        source_conn_string = get_pg_connection_string(source_conn_params,DB_TOKEN)
        dest_conn_params = get_pg_connection_string(dest_conn_params,DB_TOKEN)
        dest_conn = get_pg_connection(dest_conn_params,DB_TOKEN)
        logger.info("Connected to source and destination databases using MFA token")
        logger_array.append(log_stream.getvalue())
        
    else:
        source_conn_params = {
            'host': f"{request.form['source_server']}.postgres.database.azure.com",
            'port': 5432,
            'dbname': request.form['source_dbname'],
            'user': request.form['source_user'],
            'password': request.form['source_password']
        }
        dest_conn_params = {
            'host': f"{request.form['dest_server']}.postgres.database.azure.com",
            'port': 5432,
            'dbname': request.form['dest_dbname'],
            'user': request.form['dest_user']
            }
        
        source_conn = get_pg_connection(source_conn_params)
        dest_conn = get_pg_connection(dest_conn_params)
        source_conn_string = get_pg_connection_string(source_conn_params)
        dest_conn_params = get_pg_connection_string(dest_conn_params)
        logger.info("Connected to source and destination databases without MFA token")
        logger_array.append(log_stream.getvalue())
    
    CRnumber = request.form['CRnumber']
    logger.info(f"CR Number received: {CRnumber}")
    logger_array.append(log_stream.getvalue())
    
    try:
        
        
        source_cur = source_conn.cursor()
        
        # Get distinct schemas
        source_cur.execute("SELECT DISTINCT schema_name FROM information_schema.schemata ORDER BY schema_name;")
        schemas = [row[0] for row in source_cur.fetchall()]
        logger.info(f"Fetched {len(schemas)} schemas from source database")
        logger_array.append(log_stream.getvalue())
        source_cur.close()
        source_conn.close()

    except Exception as e:
        logger.error(f"Error connecting to source database or fetching schemas: {e}")
        logger_array.append(log_stream.getvalue())
        return f"Error connecting to DB or fetching data: {e}"
    
    return render_template('step2.html', schemas=schemas, 
                        conn_params=source_conn_string,dest_conn_params=dest_conn_params
                        ,CRnumber = CRnumber,dest_conn=dest_conn,
                        source_conn=source_conn)

# Return fixed object types for a given schema (can be enhanced if needed)
@app.route('/get_object_types', methods=['POST'])
def get_object_types():
    data = request.json
    
    schema = data.get('schema')
    conn_params = data.get('conn_params')
    
    # For demo, object types fixed (you can change based on schema if required)
    object_types = ['table', 'view', 'function', 'procedure']

    return jsonify(object_types)

# Return object names for given schema and object type
@app.route('/get_object_names', methods=['POST'])
def get_object_names():
    data = request.json
    conn_params = data.get('conn_params')
    schema = data.get('schema')
    CRnumber= data.get('CRnumber')
    objtype = data.get('objtype')

    if not all([conn_params, schema, objtype]):
        return jsonify([])

    try:
        conn = get_pg_connection(conn_params)
        cur = conn.cursor()
        

        if objtype.lower() == 'table':
            
            cur.execute("""
                SELECT c.relname AS table_name
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s -- Replace with your schema name
                AND c.relkind = 'r'                  -- 'r' means ordinary table
                ORDER BY c.relname;
            """, ((schema),))
            rows = cur.fetchall()
            names = [row[0] for row in rows]
            
        elif objtype.lower() == 'view':
            cur.execute("""
                SELECT c.relname AS table_name
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s -- Replace with your schema name
                AND c.relkind = 'v'                  -- 'r' means ordinary table
                ORDER BY c.relname;
            """, ((schema),))
            rows = cur.fetchall()
            names = [row[0] for row in rows]    
            

        elif objtype.lower() in ('function'):
            cur.execute("""
                SELECT proname FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = %s and proname like %s
                ORDER BY proname
            """, (schema,'fn_%'))
            rows = cur.fetchall()
            names = [row[0] for row in rows]
            
            
        elif objtype.lower() in ( 'procedure'):
            cur.execute("""
                SELECT proname FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = %s and proname like %s
                ORDER BY proname
            """, (schema,'sp_%'))
            rows = cur.fetchall()
            names = [row[0] for row in rows]    

        else:
            names = []

        cur.close()
        conn.close()
        return jsonify(names)

    except Exception as e:
        print("Error fetching object names:", e)
        logger.error(f"Error fetching object names: {e}")
        logger_array.append(log_stream.getvalue())
        return jsonify([])

# ... (your other routes like step3, step4 unchanged)

@app.route('/step3', methods=['POST'])
def step3():
    source_conn_params = {
            'host': f"{request.form['host']}",
            'port': 5432,
            'dbname': request.form['dbname'],
            'user': request.form['user'],
            'password': request.form.get('password')
            }
    dest_conn_params = {
            'host': f"{request.form['dest_host']}",
            'port': 5432,
            'dbname': request.form['dest_dbname'],
            'user': request.form['dest_user'],
            'password': request.form.get('dest_password')
            }
    
    
    
    CRnumber = request.form['CRnumber']
    

    compare_data = []
    try:
        
        
        conn = get_pg_connection(source_conn_params)
        cur = conn.cursor()
        
        
        dest_conn = get_pg_connection(dest_conn_params)
        dest_cur = dest_conn.cursor()
        
        for i in range(25):
            schema = request.form.get(f'schema_{i}')
            objtype = request.form.get(f'objtype_{i}')
            objname = request.form.get(f'objname_{i}')

            if not (schema and objtype and objname):
                continue

            definition = ""

            if objtype.lower() in ['table', 'view']:
                # Get DDL using pg_get_tabledef or pg_dump alternative (simplified here)
                # For demo: fetch table columns as DDL substitute
                cur.execute(f"""
                    SELECT 'CREATE {objtype.upper()} {schema}.{objname} (' ||
                    string_agg(column_name || ' ' || data_type, ', ') || ');'
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    GROUP BY table_schema, table_name;
                """, (schema, objname))
                row = cur.fetchone()
                definition = row[0] if row else "Definition not found."
                
                dest_cur.execute(f"""
                    SELECT 'CREATE {objtype.upper()} {schema}.{objname} (' ||
                    string_agg(column_name || ' ' || data_type, ', ') || ');'
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    GROUP BY table_schema, table_name;
                """, (schema, objname))
                row = dest_cur.fetchone()
                dest_definition = row[0] if row else "Definition not found."

            elif objtype.lower() in ['function', 'procedure']:
                # Use pg_get_functiondef for functions/procedures

                cur.execute("""
                    SELECT pg_get_functiondef(p.oid)
                    FROM pg_proc p
                    JOIN pg_namespace n ON p.pronamespace = n.oid
                    WHERE n.nspname = %s AND p.proname = %s
                    LIMIT 1;
                """, (schema, objname))
                row = cur.fetchone()
                definition = row[0] if row else "Definition not found."
                
                dest_cur.execute("""
                    SELECT pg_get_functiondef(p.oid)
                    FROM pg_proc p
                    JOIN pg_namespace n ON p.pronamespace = n.oid
                    WHERE n.nspname = %s AND p.proname = %s
                    LIMIT 1;
                """, (schema, objname))
                row = dest_cur.fetchone()
                dest_definition = row[0] if row else "Definition not found."

            else:
                definition = "Object type not supported."
                dest_definition = "Object type not supported."

            definition = sqlparse.format(definition, reindent=True, keyword_case='upper')
            dest_definition = sqlparse.format(dest_definition, reindent=True, keyword_case='upper')
            SAS_URL_Source_DB, SAS_URL_Dest_DB = save_postgres_sql_to_storage(CRnumber, objname, definition, dest_definition,logger)
            
            compare_data.append({
                'schema': schema,
                'objname': objname,
                'source_definition': definition,
                'dest_definition': dest_definition,
                'objtype': objtype,
                'SAS_URL_Source_DB': SAS_URL_Source_DB,
                'SAS_URL_Dest_DB': SAS_URL_Dest_DB
            })
        # Close cursors and connections
            logger.info(f"Fetched definitions for {objtype} {objname} in schema {schema}")
            logger_array.append(log_stream.getvalue())
            
            
            
        cur.close()
        dest_cur.close()
        conn.close()
        dest_conn.close()

    except Exception as e:
        logger.error(f"Error fetching definitions: {e}")
        logger_array.append(log_stream.getvalue())
        return f"Error fetching definitions: {e}"

    return render_template('step7_compare.html',compare_data=compare_data,conn_params=source_conn_params,CRnumber=CRnumber,dest_conn_params=dest_conn_params)


@app.route('/step4_postgres', methods=['POST'])

def step4_postgres():
    
    
    if request.form.get('Github_commit')=='on':
        host = request.form['dest_host']
        port = request.form['dest_port']
        dbname = request.form['dest_dbname']
        user =  request.form['dest_user']
        password = request.form.get('dest_password', '')
        dest_conn_params = {
            'host': f"{request.form['dest_host']}",
            'port': 5432,
            'dbname': request.form['dest_dbname'],
            'user': request.form['dest_user'],
            'password': request.form.get('dest_password')
            }
        

        count = int(request.form.get('count', 0))
        files_data = []
        for i in range(count):
            schema = request.form.get(f'schema_{i}')
            objname = request.form.get(f'objname_{i}')
            source_def = request.form.get(f'source_def_{i}')
            dest_def = request.form.get(f'dest_def_{i}')
            obj_type = request.form.get(f'objtype_{i}')
            SAS_URL_Source_DB = request.form.get(f'SAS_URL_Source_DB_{i}')
            SAS_URL_Dest_DB = request.form.get(f'SAS_URL_Dest_DB_{i}')
            filepath = objname + '.sql'
            files_data.append({
                'file_path': filepath,
                'object_type': obj_type,
                'source_def': source_def,
                'dest_def': dest_def,
                'SAS_URL_Source_DB': SAS_URL_Source_DB,
                'SAS_URL_Dest_DB': SAS_URL_Dest_DB
            })
        return render_template('step5.html'
                            ,compare_data=(request.form.get('compare_data'))
                            ,DML_PRESENT= request.form.get('DML_PRESENT') 
                            ,CRnumber=request.form.get('CRnumber')
                            ,source_conn_params=request.form.get('source_conn_params')
                            ,dest_conn_params=dest_conn_params
                            ,files = files_data
                            ,github_base_path =''
                            )
    else:
        source_conn_params = request.form['source_conn_params']
        host = request.form['dest_host']
        port = request.form['dest_port']
        dbname = request.form['dest_dbname']
        user =  request.form['dest_user']
        password = request.form.get('dest_password', '')
        dest_conn_params = {
            'host': f"{request.form['dest_host']}",
            'port': 5432,
            'dbname': request.form['dest_dbname'],
            'user': request.form['dest_user'],
            'password': request.form.get('dest_password')
            }

        count = int(request.form.get('count', 0))
        files_data = []
        for i in range(count):
            schema = request.form.get(f'schema_{i}')
            objname = request.form.get(f'objname_{i}')
            source_def = request.form.get(f'source_def_{i}')
            dest_def = request.form.get(f'dest_def_{i}')
            obj_type = request.form.get(f'objtype_{i}')
            filepath = objname + '.sql'
            SAS_URL_Source_DB = request.form.get(f'SAS_URL_Source_DB_{i}')
            SAS_URL_Dest_DB = request.form.get(f'SAS_URL_Dest_DB_{i}')
            files_data.append({
                'file_path': filepath,
                'object_type': obj_type,
                'source_def': source_def,
                'dest_def': dest_def,
                'SAS_URL_Source_DB': SAS_URL_Source_DB,
                'SAS_URL_Dest_DB': SAS_URL_Dest_DB
            })
            CRnumber = request.form['CRnumber']
            DML_PRESENT= request.form.get('DML_PRESENT')
            dest_conn = get_pg_connection(dest_conn_params)
            dest_cur = dest_conn.cursor()
            try:
                    dest_cur.execute(source_def)
                    dest_conn.commit()
                    
                    results = {
                    'Object_name': objname,
                    'DB_status': f"Applied {objname} successfully.",
                    'SAS_URL_Source_DB': SAS_URL_Source_DB,
                    'SAS_URL_Dest_DB': SAS_URL_Dest_DB
                    }
                    print(f"Applied {objname} successfully.")
                    logger.info(f"Applied {objname} successfully to {request.form['dest_dbname']}.")
                    logger_array.append(log_stream.getvalue())
            except Exception as e:
                    print(e)
                    dest_conn.rollback()
                    logger.error(f"Failed to apply {objname}: {e}")
                    logger_array.append(log_stream.getvalue())
                    logger.warning("rollback executed")
                    logger_array.append(log_stream.getvalue())
                    results = {
                    'Object_name': objname,
                    'DB_status': f"Failed to apply {objname}: {e}",
                    'SAS_URL_Source_DB': None,
                    'SAS_URL_Dest_DB': None
                    }
            
            applied_results.append(results)
            print(f"Applied results: {applied_results}")
        dest_cur.close()
        dest_conn.close()
        if DML_PRESENT == 'on':
            return render_template("Upload_scripts.html",CRnumber=CRnumber,host= host,port=port
                                ,dbname=dbname,user=user,password=password,github_token='',github_repo=''
                                ,github_org='',github_branch='',do_git_commit='false')
        else:
            logs = '\n'.join(logger_array)
            # print("Captured logs:\n", logs)
            # Save logs to a file
            upload_blob(STORAGE_ACCOUNT_NAME,STORAGE_ACCOUNT_KEY_BASE64,STORAGE_CONTAINER_NAME, f'{CRnumber}/postgresdb_migration_logs.txt', logs)
            # To clear logs from the buffer for reuse
            SAS_URL_Logs = generate_sas_token(STORAGE_ACCOUNT_NAME,STORAGE_ACCOUNT_KEY_BASE64,STORAGE_CONTAINER_NAME, f'{CRnumber}/postgresdb_migration_logs.txt')
            res = {'Object_name': 'Logs', 'DB_status': 'Logs saved successfully', 'SAS_URL_Source_DB': None, 'SAS_URL_Dest_DB': SAS_URL_Logs}
            applied_results.append(res)
            return render_template('display_results.html',results= (applied_results))


@app.route('/get_github_paths', methods=['POST'])
def get_github_paths():
    data = request.get_json()
    repo = data.get('repo')
    branch = data.get('branch')
    token = data.get('token')
    if not repo or not branch or not token:
        print('Invalid input for get_github_paths')
        return jsonify({'paths': []})
    try:
        # repo is in the form 'org/repo'
        
        org, repo_name = repo.split('/')
        
        # Get the SHA of the branch
        branch_url = f'https://api.github.com/repos/{org}/{repo_name}/branches/{branch}'
        
        headers = {'Authorization': f'token {token}'}
        branch_resp = requests.get(branch_url, headers=headers)
        if branch_resp.status_code != 200:
            return jsonify({'paths': []})
        branch_data = branch_resp.json()
        tree_sha = branch_data['commit']['commit']['tree']['sha']
        # Get the full tree recursively
        tree_url = f'https://api.github.com/repos/{org}/{repo_name}/git/trees/{tree_sha}?recursive=1'
        tree_resp = requests.get(tree_url, headers=headers)
        if tree_resp.status_code != 200:
            return jsonify({'paths': []})
        tree_data = tree_resp.json()
        # Collect all folder paths (and optionally files)
        paths = set()
        for item in tree_data.get('tree', []):
            if item['type'] == 'tree':
                paths.add(item['path'])
            # Optionally, include files:
            # if item['type'] == 'blob':
            #     paths.add(item['path'])
        # Always include root
        paths.add('.')
        sorted_paths = sorted(paths)
        logger.info(f"Fetched {len(sorted_paths)} paths from GitHub repo {repo} on branch {branch}")
        logger_array.append(log_stream.getvalue())
        return jsonify({'paths': sorted_paths})
    except Exception:
        logger.error("Error fetching paths from GitHub")
        logger_array.append(log_stream.getvalue())
        return jsonify({'paths': []})

from flask import request, jsonify

GITHUB_API_BASE = "https://api.github.com"

@app.route('/api/github/orgs', methods=['POST'])
def github_orgs():
    data = request.get_json()
    token = data.get('token')
    
    if not token:
        return jsonify({'orgs': []}), 400

    headers = {'Authorization': f'token {token}'}
    # Get user's organizations
    url = f"{GITHUB_API_BASE}/user/orgs"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        return jsonify({'orgs': []}), resp.status_code
    orgs = resp.json()
    logger.info(f"Fetched {len(orgs)} organizations for the provided GitHub token")
    logger_array.append(log_stream.getvalue())
    return jsonify({'orgs': orgs})


@app.route('/api/github/repos', methods=['POST'])
def github_repos():
    data = request.get_json()
    token = data.get('token')
    org = data.get('org')
    if not token or not org:
        return jsonify({'repos': []}), 400

    headers = {'Authorization': f'token {token}'}
    # Get repos for the org
    url = f"{GITHUB_API_BASE}/orgs/{org}/repos"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        return jsonify({'repos': []}), resp.status_code
    repos = resp.json()
    logger.info(f"Fetched {len(repos)} repositories for organization {org}")
    logger_array.append(log_stream.getvalue())
    return jsonify({'repos': repos})


@app.route('/api/github/branches', methods=['POST'])
def github_branches():
    data = request.get_json()
    token = data.get('token')
    org = data.get('org')
    repo = data.get('repo')
    if not token or not org or not repo:
        return jsonify({'branches': []}), 400

    headers = {'Authorization': f'token {token}'}
    url = f"{GITHUB_API_BASE}/repos/{org}/{repo}/branches"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        return jsonify({'branches': []}), resp.status_code
    branches = resp.json()
    logger.info(f"Fetched {len(branches)} branches for repository {org}/{repo}")
    logger_array.append(log_stream.getvalue())
    return jsonify({'branches': branches})


from flask import request, render_template, redirect, url_for, flash

import requests
import base64
from flask import flash, redirect, url_for

@app.route('/step5', methods=['GET', 'POST'])
def step5():        
        github_token = request.form.get('github_token')
        github_repo = request.form.get('github_repo')  # e.g. username/repo
        github_org = request.form.get('github_org')
        github_branch = request.form.get('github_branch', 'working')
        github_base_path = request.form.get('github_base_path', '').strip('/')
        CRnumber = request.form.get('CRnumber')
        source_conn_params=request.form.get('source_conn_params')
        dest_conn_params=request.form.get('dest_conn_params')
        compare_data = request.form.get('compare_data')
        DML_PRESENT = request.form.get('DML_PRESENT')
        dest_host = request.form.get('dest_host')
        dest_port = request.form.get('dest_port', 5432)
        dest_dbname = request.form.get('dest_dbname')
        dest_user = request.form.get('dest_user')
        dest_password = request.form.get('dest_password', '')
        dest_conn_params = {
            'host': f"{request.form['dest_host']}",
            'port': 5432,
            'dbname': request.form['dest_dbname'],
            'user': request.form['dest_user'],
            'password': request.form.get('dest_password')
            }
        
        dest_conn = get_pg_connection(dest_conn_params)
        dest_cur = dest_conn.cursor()
        
        
        
        def get_file_sha(repo, path, branch,github_org):
            headers = {
            "Authorization": f"bearer {github_token}",
            "Accept": "application/vnd.github.v3+json"
            }
            url = f"https://api.github.com/repos/{github_org}/{repo}/contents/{path}?ref={branch}"
            
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                
                return r.json()['sha']
            
            return None

        def create_or_update_file(repo,github_org, path, branch, content, message, sha=None, description=''):
            headers = {
            "Authorization": f"bearer {github_token}",
            "Accept": "application/vnd.github.v3+json"
            }
            url = f"https://api.github.com/repos/{github_org}/{repo}/contents/{path}"
            data = {
                "message": message,
                "content": base64.b64encode(content.encode('utf-8')).decode('utf-8'),
                "branch": branch
                }
            if sha:
                data["sha"] = sha
            if description!= '':
                data["description"] = description
            r = requests.put(url, json=data, headers=headers)
            return r.status_code in (200, 201), r.json()  
        
        
        file_count = int(request.form.get('file_count', 0))
        files_info = []
        
        for i in range(file_count):
            file_path = request.form.get(f'file_path_{i}')
            folder_path = request.form.get(f'folder_path_{i}', '').strip('/')
            object_type = request.form.get(f'object_type_{i}')
            commit_desc = request.form.get(f'commit_desc_{i}')
            source_def = request.form.get(f'source_def_{i}')
            SAS_URL_Source_DB = request.form.get(f'SAS_URL_Source_DB_{i}')
            SAS_URL_Dest_DB = request.form.get(f'SAS_URL_Dest_DB_{i}')
            if not (file_path and folder_path):
                flash(f"Missing folder path or file path for item {i}")
                return redirect(request.url)

            filename = file_path
            repo_path = '/'.join(filter(None, [ folder_path, filename]))
            
            files_info.append({
                'local_file_path': file_path,
                'object_type': object_type,
                'git_repo_path': repo_path
            })

            
            commit_message = f"CR#{CRnumber}: Add/Update {repo_path}"
            
            sha = get_file_sha(github_repo, repo_path, github_branch,github_org)
            
            success, response = create_or_update_file(
                repo=github_repo,
                github_org=github_org,
                path=repo_path,
                branch=github_branch,
                content=source_def,
                message=commit_message,
                sha=sha,
                description=commit_desc
            )
            if success:
                logger.info(f"Committing {filename} to GitHub repo {github_repo} on branch {github_branch} ")
                logger_array.append(log_stream.getvalue())
            else:
                logger.error(f"Failed to commit {filename} to GitHub repo {github_repo} on branch {github_branch}: {response.get('message', 'Unknown error')}")
                logger_array.append(log_stream.getvalue())
            
            
            ### database deployment to destination 
            
            try:
                print(f"Applying {filename} definition to destination DB")
                dest_cur.execute(source_def)
                dest_conn.commit()
                results = {
                'Object_name': filename.replace('.sql', ''),
                'GIT_status': 'Success' if success else response.get('message', 'Unknown error'),
                'DB_status': f"Applied {filename} successfully.",
                'SAS_URL_Source_DB': SAS_URL_Source_DB,
                'SAS_URL_Dest_DB': SAS_URL_Dest_DB
                }
                print(f"Applied {filename} successfully.")
                
                logger.info(f"Applied {filename} successfully to {dest_dbname}.")
                logger_array.append(log_stream.getvalue())
                applied_results.append(results)
            except Exception as e:
                print(e)
                dest_conn.rollback()
                results = {
                'Object_name': filename.replace('.sql', ''),
                'GIT_status': 'Success' if success else response.get('message', 'Unknown error'),
                'DB_status': f"Failed to apply {filename}: {e}",
                'SAS_URL_Source_DB': None,
                'SAS_URL_Dest_DB': None
                }
                logger.error(f"Failed to apply {filename}: {e}")
                logger_array.append(log_stream.getvalue())
                applied_results.append(results)

          
        dest_cur.close()
        dest_conn.close()
        if DML_PRESENT == 'on':
            return render_template("Upload_scripts.html",CRnumber=CRnumber,host= dest_host,port=dest_port
                                ,dbname=dest_dbname,user=dest_user,password=dest_password
                                ,github_token=github_token,github_repo=github_repo
                                ,github_org=github_org,github_branch=github_branch,do_git_commit='true')
        else:
            print('applied_results:', applied_results)  
            logs = '\n'.join(logger_array)
            # print("Captured logs:\n", logs)
            # Save logs to a file
            upload_blob(STORAGE_ACCOUNT_NAME,STORAGE_ACCOUNT_KEY_BASE64,STORAGE_CONTAINER_NAME, f'{CRnumber}/postgresdb_migration_logs.txt', logs)
            # To clear logs from the buffer for reuse
            SAS_URL_Logs = generate_sas_token(STORAGE_ACCOUNT_NAME,STORAGE_ACCOUNT_KEY_BASE64,STORAGE_CONTAINER_NAME, f'{CRnumber}/postgresdb_migration_logs.txt')
            res = {'Object_name': 'Logs', 'DB_status': 'Logs saved successfully','GIT_STATUS':'', 'SAS_URL_Source_DB': '', 'SAS_URL_Dest_DB': SAS_URL_Logs}
            applied_results.append(res)
            return render_template('display_results.html',results= applied_results)


@app.route('/dml_upload', methods=['GET', 'POST'])
def dml_upload():
    
        dest_conn_params = {
            'host': f"{request.form['host']}",
            'port': request.form['port'],
            'dbname': request.form['dbname'],
            'user': request.form['user'],
            'password': request.form.get('password')
            }
        
        dest_conn = get_pg_connection(dest_conn_params)
        dest_cur = dest_conn.cursor()
        CRnumber = request.form.get('CRnumber')
        
        
        def get_file_sha(repo, path, branch,github_org):
            headers = {
            "Authorization": f"bearer {github_token}",
            "Accept": "application/vnd.github.v3+json"
            }
            url = f"https://api.github.com/repos/{github_org}/{repo}/contents/{path}?ref={branch}"
            
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                
                return r.json()['sha']
            
            return None

        def create_or_update_file(repo,github_org, path, branch, content, message, sha=None, description=''):
            headers = {
            "Authorization": f"bearer {github_token}",
            "Accept": "application/vnd.github.v3+json"
            }
            url = f"https://api.github.com/repos/{github_org}/{repo}/contents/{path}"
            data = {
                "message": message,
                "content": base64.b64encode(content.encode('utf-8')).decode('utf-8'),
                "branch": branch
                }
            if sha:
                data["sha"] = sha
            if description!= '':
                data["description"] = description
            r = requests.put(url, json=data, headers=headers)
            return r.status_code in (200, 201), r.json()  
        
        
        uploaded_files = request.files.getlist('files')
        
        script_types = []
        files_list = []
        for i in range(len(uploaded_files)):
            script_type = request.form.get(f'script_type_{i}')
            script_types.append(script_type)

    # Iterate through uploaded files
    
        for i, file_storage in enumerate(uploaded_files):
            filename = file_storage.filename
            file_content = file_storage.read().decode('utf-8')  # Assuming utf-8 encoding
            try:
                print(f"Applying {filename} definition to destination DB")
                dest_cur.execute(file_content)
                dest_conn.commit()
                results = {
                'Object_name': filename.replace('.sql', ''),
                'DB_status': f"Applied {filename} successfully.",
                'SAS_URL_Source_DB': None,
                'SAS_URL_Dest_DB': None
                }
                logger.info(f"Applied {filename} successfully to {request.form['dbname']}.")
                logger_array.append(log_stream.getvalue())
                print(f"Applied {filename} successfully.")
            except Exception as e:
                print(e)
                dest_conn.rollback()
                results = {
                'Object_name': filename.replace('.sql', ''),
                'DB_status': f"Failed to apply {filename}: {e}",
                'SAS_URL_Source_DB': None,
                'SAS_URL_Dest_DB': None
                }
                logger.error(f"Failed to apply {filename}: {e}")
                logger_array.append(log_stream.getvalue())
            
        
        # Now you have filename, file_content, and script_type
            
            
            if request.form.get('do_git_commit') == 'on':
                github_folder = script_types[i]
                github_token = request.form.get('github_token')
                github_repo = request.form.get('github_repo')  # e.g. username/repo
                github_branch = request.form.get('github_branch')
                github_org = request.form.get('github_org')
                filename = filename
                repo_path = '/'.join(filter(None, [ github_folder, filename]))
                commit_message = f"CR#{CRnumber}: Add/Update {repo_path}"
                sha = get_file_sha(github_repo, repo_path, github_branch,github_org)
                
                success, response = create_or_update_file(
                    repo=github_repo,
                    github_org=github_org,
                    path=repo_path,
                    branch=github_branch,
                    content=file_content,
                    message=commit_message,
                    sha=sha
                    )
                print( success, response )
                results['GIT_status']='Success' if success else response.get('message', 'Unknown error')
                if success:
                    logger.info(f"Committing {filename} to GitHub repo {github_repo} on branch {github_branch} ")
                    logger_array.append(log_stream.getvalue())
                else:
                    logger.error(f"Failed to commit {filename} to GitHub repo {github_repo} on branch {github_branch}: {response.get('message', 'Unknown error')}")
                    logger_array.append(log_stream.getvalue())

            applied_results.append(results)
            print(applied_results)
        dest_cur.close()
        dest_conn.close()
        logs = '\n'.join(logger_array)
            # print("Captured logs:\n", logs)
            # Save logs to a file
        upload_blob(STORAGE_ACCOUNT_NAME,STORAGE_ACCOUNT_KEY_BASE64,STORAGE_CONTAINER_NAME, f'{CRnumber}/postgresdb_migration_logs.txt', logs)
            # To clear logs from the buffer for reuse 
        return render_template('display_results.html', results=applied_results)




if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000)
