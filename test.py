## This the script to generate token Programatically without user in Session and using Client_id and Client_secret.
 
 
import requests 
   
# Replace these with your actual credentials 
## This is Clent ID and Secret for AskATT-Client APP
 
client_id = 'd6bd936f-5dfb-41d8-84a3-fe2191e3ba50', # This for your client ID from registration
client_secret = 'Wy38Q~83k1HU3V.Bi.VO5_fmvVb2YkmaqdrzYcHD' # This is your client secret from registration
 
auth_url = 'https://login.microsoftonline.com/e741d71c-c6b6-47b0-803c-0f3b32b07556/oauth2/v2.0/token'  ## This is OAuth URL with tenant ID.
   
# Replace these with the actual scope and grant type you need 
scope = 'api://95273ce2-6fec-4001-9716-a209d398184f/.default' ##Scope of AskATT-Stage APU for General QnA --> Orchestration
grant_type = 'client_credentials' 

# Prepare the payload for the token request 
payload = { 
    'client_id': client_id, 
    'client_secret': client_secret, 
    'scope': scope, 
    'grant_type': grant_type 
} 

# Make the request to the authentication server 
response = requests.post(auth_url, data=payload) 

# Check if the request was successful 
if response.status_code == 200: 
    token_data = response.json() 
    access_token = token_data['access_token'] 
    token_type = token_data['token_type'] 
    print(f"Your JWT token is: {access_token}") 
else: 
    print("Error obtaining JWT token:", response.text)
    

