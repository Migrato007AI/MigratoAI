from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import psycopg2

# # 1. Hardcoded Service Principal credentials
# tenant_id = "your-tenant-id"
# client_id = "your-client-id"
# client_secret = "your-client-secret"

# # 2. Key Vault details
# key_vault_name = "myspmwestus2devapp01kv"
# key_vault_url = f"https://{key_vault_name}.vault.azure.net/"

# # 3. Secret name in Key Vault where connection string is stored
# secret_name = "direct-dev-postgre-db"

# # Authenticate to Azure Key Vault using Service Principal
# credential = ClientSecretCredential(
#     tenant_id=tenant_id,
#     client_id=client_id,
#     client_secret=client_secret
# )

# # Create a client to access Key Vault secrets
# secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

# # Retrieve the connection string from Key Vault
# retrieved_secret = secret_client.get_secret(secret_name)


def get_pg_connection(params, access_token=None):
    """
    Connect to PostgreSQL using either username/password or AAD access token (MFA).
    If access_token is provided, it is used as the password with 'Bearer ' prefix.
    """
    import psycopg2
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

params = {
    'host': 'spm-westus2-nprod-direct-etl.postgres.database.azure.com',
    'port': 5432,
    'dbname': 'varicent-dev-direct',
    'user': 'synapse_user',
    'password': 'jUh@w8s0F4$rb6&f',
    'sslmode': 'require'
} 
    
conn = get_pg_connection(params)
cur = conn.cursor()

cur.execute("SELECT DISTINCT schema_name FROM information_schema.schemata ORDER BY schema_name;")
schemas = cur.fetchall()
print("Schemas in the database:")
for schema in schemas:
    print(f" - {schema[0]}")


# # Connect to PostgreSQL using the retrieved connection string

