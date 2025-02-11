import pandas as pd
import json
from dhanhq import dhanhq
from dhanhq import marketfeed
import requests
from datetime import datetime, timedelta
import schedule
import time

# Dhan API Credentials
client_id = 1101381542
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzQxODc3MTg0LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMTM4MTU0MiJ9.M37LoOQ-NbxzobjoRZ6cfrN0s_vRVfHG7NKc3xv9327TCb0nAKLI2R38mtvvBfvtBMf_deLLAS8iEcf9bO9aDA"
dhan = dhanhq(client_id, access_token)

# Trading Parameters
trade_start_price = 99000.0
desired_symbol = "SILVERMIC-29Aug2025-FUT"

def fetch_data():
    res = dhan.get_positions()
    
    for position in res['data']:
        if position['tradingSymbol'] == desired_symbol:
            if position.get('positionType') is None or position.get('netQty') is None:
                print("Error: positionType or netQty is None. Skipping this position.")
                continue

            seq_id = int(position['securityId'])
            url = "https://scanx.dhan.co/scanx/rtscrdt"
            payload = {"Data": {"Seg": 5, "Secid": seq_id}}

            try:
                response = requests.post(url, json=payload)
                response.raise_for_status()

                if response.status_code == 200:
                    print("Request successful.")

                data = response.json()
                last_price = data['data']['Ltp']

                if last_price is None:
                    print("Error: Last price is None. Skipping this iteration.")
                    return

                
                
              
                # Place new orders only if they do not exist
                if position['positionType'] == 'SHORT':
                    next_short_trade_price = trade_start_price - (position['netQty'] * 2000)
                    target_price = next_short_trade_price + (position['netQty'] * 2000)  # Fixed calculation
                    print("The next target is " + str(target_price))
                    print("The next  short price is " + str(next_short_trade_price))
                    
                    # Fetch existing forever orders
                    existing_forever_orders = dhan.get_forever().get('data', [])

                # Check if the next order and target order already exist
                    next_order_exists = any(
                      order['tradingSymbol'] == desired_symbol and order['price'] == next_short_trade_price
                      for order in existing_forever_orders
                    )

                    target_order_exists = any(
                      order['tradingSymbol'] == desired_symbol and order['price'] == target_price
                      for order in existing_forever_orders
                    )

                    if not next_order_exists:
                        print("Placing forever SELL order...")
                        sell_order = dhan.place_forever(
                            security_id=seq_id,
                            exchange_segment=dhan.MCX,
                            transaction_type=dhan.SELL,
                            product_type=dhan.MARGIN,
                            order_type=dhan.LIMIT,
                            quantity=1,
                            price=next_short_trade_price,
                            trigger_Price=next_short_trade_price
                        )
                        print(sell_order)
                    else:
                        print("Next SELL order already exists.")

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
                        print(buy_target_order)
                    else:
                        print("BUY target order already exists.")

                elif position['positionType'] == 'LONG':
                    next_long_trade_price = trade_start_price - (position['netQty'] * 2000)
                    target_price = next_long_trade_price + (position['netQty'] * 2000)  # Fixed calculation
                    print("The next target is " + str(target_price))
                    print("The next long buy price is " + str(next_long_trade_price))
                    
                    # Fetch existing forever orders
                    existing_forever_orders = dhan.get_forever().get('data', [])

                # Check if the next order and target order already exist
                    next_order_exists = any(
                      order['tradingSymbol'] == desired_symbol and order['price'] == next_long_trade_price
                      for order in existing_forever_orders
                    )

                    target_order_exists = any(
                      order['tradingSymbol'] == desired_symbol and order['price'] == target_price
                      for order in existing_forever_orders
                    )

                    if not next_order_exists:
                        print("Placing forever BUY order...")
                        buy_order = dhan.place_forever(
                            security_id=seq_id,
                            exchange_segment=dhan.MCX,
                            transaction_type=dhan.BUY,
                            product_type=dhan.MARGIN,
                            order_type=dhan.LIMIT,
                            quantity=1,
                            price=next_short_trade_price,
                            trigger_Price=next_short_trade_price
                        )
                        print(buy_order)
                    else:
                        print("Next BUY order already exists.")

                    if not target_order_exists:
                        print("Placing opposite SELL target order...")
                        sell_target_order = dhan.place_forever(
                            security_id=seq_id,
                            exchange_segment=dhan.MCX,
                            transaction_type=dhan.SELL,
                            product_type=dhan.MARGIN,
                            order_type=dhan.LIMIT,
                            quantity=1,
                            price=target_price,
                            trigger_Price=target_price
                        )
                        print(sell_target_order)
                    else:
                        print("SELL target order already exists.")

            except requests.exceptions.HTTPError as err:
                print("HTTP error occurred:", err)
            except json.JSONDecodeError as err:
                print("JSON decoding error occurred:", err)
            except KeyError as err:
                print("Key error occurred:", err)
            except Exception as err:
                print("An unexpected error occurred:", err)

            break
    else:
        print(f"No data found for symbol '{desired_symbol}'.")

# Schedule the fetch_data function to run every 30 seconds
schedule.every(30).seconds.do(fetch_data)

while True:
    schedule.run_pending()
    time.sleep(1)
