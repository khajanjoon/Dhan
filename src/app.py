import requests
import json
import pandas as pd
from datetime import datetime
import schedule
import time
import logging
from dhanhq import dhanhq


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Dhan API Credentials
client_id = 1101381542
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzQ0NzkwNzYwLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiaHR0cHM6Ly9hcGkubWFya2V0bWF5YS5jb20vYXBpL29yZGVydXBkYXRlL2RoYW4iLCJkaGFuQ2xpZW50SWQiOiIxMTAxMzgxNTQyIn0.6UubHDZ0MccjB4553qL2aw9Qs5RFDel_Ir8wsqgTLXb8FrnSbyKR0mUIrjj14_gO23MFgHEKScWYi_h9hDwrKA"
dhan = dhanhq(client_id, access_token)

def fetch_near_month_expiry():
    """Fetches the near-month expiry contract for CRUDEOILM."""
    url = "https://smart-search.dhan.co/Search/api/Search/Scrip"
    headers = {"Content-Type": "application/json"}
    payload = {
        "UserId": "1101381542",
        "UserType": "C",
        "Source": "W",
        "Data": {"inst": "", "searchterm": "CRUDEOILM", "exch": "MCX", "optionflag": True},
        "broker_code": "DHN1804"
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

        mcx_data = []
        today = datetime.today()

        if "data" in data:
            for item in data["data"]:
                if item["d_exch"] == "MCX" and item["ExpDate_s"]:
                    try:
                        expiry_date = datetime.strptime(item["ExpDate_s"], "%Y-%m-%dT%H:%M")
                        if expiry_date.month == today.month:
                            mcx_data.append({
                                "Security ID": item["Sid_s"],
                                "Trading Symbol": item["ExchTradingSymbol_s"],
                                "Expiry Date": item["ExpDate_s"],
                                "Upper Circuit": item["Upper_ckt_d"],
                                "Lower Circuit": item["LoweCkt_d"],
                                "Lot Size": item["LOT_UNITS_s"],
                                "Volume": item["volume_i"]
                            })
                    except ValueError as e:
                        logging.warning(f"Skipping invalid date format: {item['ExpDate_s']} - {e}")
        
        if mcx_data:
            df = pd.DataFrame(mcx_data)
            logging.info("\n📌 **Near-Month Futures Contract**")
            logging.info(df.to_string(index=False))  
            return df
        else:
            logging.warning("No Near Month MCX data found.")
            return None

    except requests.exceptions.RequestException as err:
        logging.error(f"Request Error: {err}")
        return None

def fetch_last_price(symbol, security_id):
    """Fetches the last trading price of the given symbol."""
    url = "https://scanx.dhan.co/scanx/rtscrdt"
    payload = {"Data": {"Seg": 5, "Secid": int(security_id)}}

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

        if "data" in data and "Ltp" in data["data"]:
            last_price = data["data"]["Ltp"]
            if last_price is None:
                logging.warning(f"Error: Last price for {symbol} is None. Skipping this iteration.")
                return None

            logging.info(f"📌 Last Trading Price of {symbol}: ₹{last_price}")
            return last_price
        else:
            logging.error(f"Error: 'Ltp' key missing in API response for {symbol}")
            return None

    except requests.exceptions.HTTPError as err:
        logging.error(f"HTTP error occurred: {err}")
        return None

def fetch_atm_option_data(futures_price):
    """Fetches the At-the-Money (ATM) option contracts for near-month expiry."""
    url = "https://smart-search.dhan.co/Search/api/Search/Scrip"
    headers = {"Content-Type": "application/json"}
    payload = {
        "UserId": "1101381542",
        "UserType": "C",
        "Source": "W",
        "Data": {"inst": "", "searchterm": "CRUDEOILM", "exch": "MCX", "optionflag": True},
        "broker_code": "DHN1804"
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

        if "data" in data:
            options = []
            
            for item in data["data"]:
                trading_symbol = item.get("ExchTradingSymbol_s", "")

                if "Strike_d" in item and trading_symbol.endswith(("-CE", "-PE")):
                    try:
                        strike_price = float(item["Strike_d"])
                        options.append((strike_price, trading_symbol, item))
                    except ValueError:
                        logging.warning(f"Skipping invalid strike price: {item['Strike_d']} for {trading_symbol}")

            if not options:
                logging.warning("⚠ No valid options data found.")
                return None, None

            atm_strike, _, atm_option = min(options, key=lambda x: abs(x[0] - futures_price))

            call_option, put_option = None, None
            for strike_price, trading_symbol, option in options:
                if strike_price == atm_strike:
                    if trading_symbol.endswith("-CE"):
                        call_option = option
                    elif trading_symbol.endswith("-PE"):     
                        put_option = option

            logging.info("\n📌 **Near-Month ATM Option Contracts**")
            logging.info(f"ATM Strike Price: ₹{atm_strike}")
            if call_option:
                logging.info(f"Call Option: {call_option['Ticker_t']} (Security ID: {call_option['Sid_s']})")
            if put_option:
                logging.info(f"Put Option: {put_option['Ticker_t']} (Security ID: {put_option['Sid_s']})")
                
            return call_option, put_option

    except requests.exceptions.RequestException as err:
        logging.error(f"Request Error: {err}")
        return None, None

def fetch_data(desired_symbol, seq_id,last_price):
    """Fetch positions and place a sell order only if the desired symbol is not found in positions."""
    res = dhan.get_positions()

    if not res or 'data' not in res:
        logging.error("Error: Invalid or empty response from get_positions().")
        return

    logging.info(f"Desired Symbol: {desired_symbol}")
    logging.info(f"Security ID (seq_id): {seq_id}")

    symbol_found = False

    if res['data']:  # Check if positions data is not empty
        for position in res['data']:
            trading_symbol = position.get('tradingSymbol')
            if trading_symbol is None:
                logging.warning(f"Warning: 'tradingSymbol' key missing in position: {position}")
                continue

            if trading_symbol == desired_symbol:
                symbol_found = True
                logging.info(f"Symbol '{desired_symbol}' found in positions. No order will be placed.")
                 # Place new orders only if they do not exist
                if position['positionType'] == 'SHORT':
                    
                    target_price = last_price - 50  # Fixed calculation
                    logging.info(f"Symbol '{desired_symbol}'target_price price is {target_price}")
                    # Fetch existing forever orders
                    existing_forever_orders = dhan.get_forever().get('data', [])

                    target_order_exists = any(
                      order['tradingSymbol'] == desired_symbol and order['price'] == target_price
                      for order in existing_forever_orders
                    )
                    if not target_order_exists:
                        print("Placing opposite BUY target order...")
                        buy_target_order = dhan.place_forever(
                            security_id=seq_id,
                            exchange_segment=dhan.MCX,
                            transaction_type=dhan.BUY,
                            product_type=dhan.MARGIN,
                            order_type=dhan.LIMIT,
                            quantity=1,
                            price=target_price,
                            trigger_Price=target_price
                        )
                        logging.info(buy_target_order)
                    else:
                        logging.info("BUY target order already exists.")
                    
                break  # Exit the loop if the symbol is found

    # Place the SELL order only if the symbol is not found in positions
    if not symbol_found:
        logging.info(f"Symbol '{desired_symbol}' not found in positions. Placing SELL order.")

        try:
            sell_order = dhan.place_order(
                security_id=seq_id,
                exchange_segment=dhan.MCX,
                transaction_type=dhan.SELL,
                product_type=dhan.MARGIN,
                order_type=dhan.MARKET,
                quantity=1,
                price=0
            )
            logging.info(f"Sell Order Response: {sell_order}")
        except Exception as e:
            logging.error(f"Error placing SELL order: {e}")

# **Schedule Function**
def scheduled_task():
    """Fetches near-expiry futures and options, then gets their last traded prices."""
    df = fetch_near_month_expiry()
    if df is not None and not df.empty:
        symbol = df.iloc[0]["Trading Symbol"]  # **Take the first near expiry symbol**
        security_id = df.iloc[0]["Security ID"]  # **Take the corresponding Security ID**
        futures_price = fetch_last_price(symbol, security_id)

        if futures_price:
            call_option, put_option = fetch_atm_option_data(futures_price)

            if call_option:
                last_price = fetch_last_price(call_option["Ticker_t"], call_option["Sid_s"])
                logging.info(last_price)
                if last_price > 200:
                    fetch_data(call_option['Ticker_t'], call_option['Sid_s'],last_price)
            if put_option:
                last_price = fetch_last_price(put_option["Ticker_t"], put_option["Sid_s"])
                logging.info(last_price)
                if last_price > 200:
                    fetch_data(put_option['Ticker_t'], put_option['Sid_s'],last_price)

# **Run the function every 30 seconds**
schedule.every(100).seconds.do(scheduled_task)

while True:
    schedule.run_pending()
    time.sleep(1)
