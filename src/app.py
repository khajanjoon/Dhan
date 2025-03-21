import base64
import hmac
import os
import struct
import time
from urllib.parse import urlparse, parse_qs
from fastapi import FastAPI, HTTPException
import requests
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
app = FastAPI()

# Fyers API Credentials
client_id = "PQ25J9JSU4-100"
token_file = "fyers_token.txt"
totp_key = "DRU5NOPAFF6XJTMI4AL3DYSM3CP74STA"  # totp_key (ex., "OMKRABCDCDVDFGECLWXK6OVB7T4DTKU5")
username = "XK24776"  # Fyers Client ID (ex., "TK01248")
pin = 5560  # four-digit PIN
client_id = "PQ25J9JSU4-100"  # App ID of the created app (ex., "L9NY305RTW-100")
secret_key = "HAGXAE1PAC"# Secret ID of the created app
redirect_uri = "https://trade.fyers.in/api-login/redirect-uri/index.html"  # Redircet URL you entered while creating the app (ex., "https://trade.fyers.in/api-login/redirect-uri/index.html")

data12 = {
    "symbol":"NSE:IDEA-EQ",
    "qty":1,
    "type":2,
    "side":1,
    "productType":"INTRADAY",
    "limitPrice":0,
    "stopPrice":0,
    "validity":"DAY",
    "disclosedQty":0,
    "offlineOrder":False,
    "orderTag":"tag1"
}



def read_file():
    try:
        with open("fyers_token.txt", "r") as f:
            token = f.read().strip()
        return token
    except FileNotFoundError:
        return None


def write_file(token):
    with open("fyers_token.txt", "w") as f:
        f.write(token)


def totp(key, time_step=30, digits=6, digest="sha1"):
    key = base64.b32decode(key.upper() + "=" * ((8 - len(key)) % 8))
    counter = struct.pack(">Q", int(time.time() / time_step))
    mac = hmac.new(key, counter, digest).digest()
    offset = mac[-1] & 0x0F
    binary = struct.unpack(">L", mac[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(binary)[-digits:].zfill(digits)


def get_token():
    headers = {
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    }

    s = requests.Session()
    s.headers.update(headers)

    data1 = f'{{"fy_id":"{base64.b64encode(f"{username}".encode()).decode()}","app_id":"2"}}'
    r1 = s.post("https://api-t2.fyers.in/vagator/v2/send_login_otp_v2", data=data1)

    request_key = r1.json()["request_key"]
    data2 = f'{{"request_key":"{request_key}","otp":{totp(totp_key)}}}'
    r2 = s.post("https://api-t2.fyers.in/vagator/v2/verify_otp", data=data2)
    assert r2.status_code == 200, f"Error in r2:\n {r2.text}"

    request_key = r2.json()["request_key"]
    data3 = f'{{"request_key":"{request_key}","identity_type":"pin","identifier":"{base64.b64encode(f"{pin}".encode()).decode()}"}}'
    r3 = s.post("https://api-t2.fyers.in/vagator/v2/verify_pin_v2", data=data3)
    assert r3.status_code == 200, f"Error in r3:\n {r3.json()}"

    headers = {"authorization": f"Bearer {r3.json()['data']['access_token']}", "content-type": "application/json; charset=UTF-8"}
    data4 = f'{{"fyers_id":"{username}","app_id":"{client_id[:-4]}","redirect_uri":"{redirect_uri}","appType":"100","code_challenge":"","state":"abcdefg","scope":"","nonce":"","response_type":"code","create_cookie":true}}'
    r4 = s.post("https://api.fyers.in/api/v2/token", headers=headers, data=data4)
    assert r4.status_code == 308, f"Error in r4:\n {r4.json()}"
    print(data4)
    parsed = urlparse(r4.json()["Url"])
    auth_code = parse_qs(parsed.query)["auth_code"][0]

    session = fyersModel.SessionModel(client_id=client_id, secret_key=secret_key, redirect_uri=redirect_uri, response_type="code", grant_type="authorization_code")
    session.set_token(auth_code)
    response = session.generate_token()
    return response["access_token"]


def get_profile(token):
    fyers = fyersModel.FyersModel(client_id=client_id, token=token, log_path=os.getcwd())
    return fyers.get_profile()

def get_fund(token):
    fyers=fyersModel.FyersModel(client_id=client_id, token=token, log_path=os.getcwd())
    return fyers.funds()
    
def get_position(token):
    fyers=fyersModel.FyersModel(client_id=client_id, token=token, log_path=os.getcwd())
    return fyers.positions()

def place_order(token):
    fyers=fyersModel.FyersModel(client_id=client_id, token=token, log_path=os.getcwd())
    return fyers.place_order(data12)

def main():
    token = read_file()
    if token is None:
        token = get_token()
        write_file(token)
        print("Fyers access token is saved in `fyers_token.txt` file.")

    resp = get_profile(token)

    if "error" in resp["s"] or "error" in resp["message"] or "expired" in resp["message"]:
        token = get_token()
        resp = get_profile(token)
    #print(resp)


    resp = get_fund(token)

    if "error" in resp["s"] or "error" in resp["message"] or "expired" in resp["message"]:
        token = get_token()
        resp = get_fund(token)
    #print(resp)

    resp = get_position(token)

    if "error" in resp["s"] or "error" in resp["message"] or "expired" in resp["message"]:
        token = get_token()
        resp = get_position(token)
    print(resp)






def onmessage(message):
    """
    Callback function to handle incoming messages from the FyersDataSocket WebSocket.

    Parameters:
        message (dict): The received message from the WebSocket.

    """
   # Specify the symbol to check
    expected_symbol = 'MCX:CRUDEOILM24SEPFUT'

    # Check if the symbol is as expected
    if message.get('symbol') == expected_symbol:
    # Iterate through all key-value pairs and print them
     for key, value in message.items():
        print(f"{key}: {value}")
    else:
        print(f"Data does not pertain to the expected symbol: {expected_symbol}")


def onerror(message):
    """
    Callback function to handle WebSocket errors.

    Parameters:
        message (dict): The error message received from the WebSocket.


    """
    print("Error:", message)


def onclose(message):
    """
    Callback function to handle WebSocket connection close events.
    """
    print("Connection closed:", message)


def onopen():
    """
    Callback function to subscribe to data type and symbols upon WebSocket connection.

    """
    # Specify the data type and symbols you want to subscribe to
    data_type = "SymbolUpdate"

    # Subscribe to the specified symbols and data type
    symbols = ['NSE:ADANIENT-EQ']
    fyers.subscribe(symbols=symbols, data_type=data_type)

    # Keep the socket running to receive real-time data
    fyers.keep_running()


# Replace the sample access token with your actual access token obtained from Fyers
access_token  = read_file()
#print(access_token)              
# Create a FyersDataSocket instance with the provided parameters
fyers = data_ws.FyersDataSocket(
    access_token=access_token,       # Access token in the format "appid:accesstoken"
    log_path="",                     # Path to save logs. Leave empty to auto-create logs in the current directory.
    litemode=False,                  # Lite mode disabled. Set to True if you want a lite response.
    write_to_file=False,              # Save response in a log file instead of printing it.
    reconnect=True,                  # Enable auto-reconnection to WebSocket on disconnection.
    on_connect=onopen,               # Callback function to subscribe to data upon connection.
    on_close=onclose,                # Callback function to handle WebSocket connection close events.
    on_error=onerror,                # Callback function to handle WebSocket errors.
    on_message=onmessage             # Callback function to handle incoming messages from the WebSocket.
)


def read_token():
    """Reads the Fyers access token from a file."""
    try:
        with open(token_file, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        raise HTTPException(status_code=401, detail="Fyers access token not found. Please authenticate.")


@app.get("/quotes/{exchange}/{symbol}")
def get_stock_quote(exchange: str, symbol: str):
    """
    Fetches live stock/commodity/currency data from Fyers API.

    Supported:
    - NSE, BSE, MCX for Stocks, Futures, Options
    - Currency Pairs (USDINR, GBPINR, etc.)

    Example:
    - /quotes/NSE/SBIN-EQ
    - /quotes/BSE/SBIN-A
    - /quotes/NSE/NIFTY20OCTFUT
    - /quotes/NSE/USDINR20OCTFUT
    - /quotes/MCX/CRUDEOIL20OCTFUT
    """
    token = read_token()
    fyers = fyersModel.FyersModel(client_id=client_id, token=token, log_path=os.getcwd())

    data = {"symbols": f"{exchange}:{symbol}"}
    response = fyers.quotes(data=data)

    if response.get("s") == "error":
        raise HTTPException(status_code=400, detail=response.get("message", "Error fetching stock data"))

    return response

if __name__ == "__main__":
    main()
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
