import pandas as pd
import json
from dhanhq import dhanhq
from dhanhq import marketfeed
import requests
from datetime import datetime, timedelta
import schedule
import time

client_id = 1101381542
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzMwNjMyMzU0LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMTM4MTU0MiJ9.3yrWmHUM5nVVV2mm8PcazNzVUO8EcQ_Cy66FY7S3wFDwUm65WErFTt6ci4aar21AIHLUu3dEokz7PFPIKxruJQ"
dhan = dhanhq(client_id, access_token)
trade_start_price = 93024.0
desired_symbol = "SILVERMIC-29Nov2024-FUT"

def fetch_data():
    res = dhan.get_positions()
    #print(res)
    for position in res['data']:
        if position['tradingSymbol'] == desired_symbol:

            # Check if essential data is available
            if position.get('positionType') is None or position.get('netQty') is None:
                print("Error: positionType or netQty is None. Skipping this position.")
                continue

            position_details = (
                f"Exchange: {position['exchangeSegment']}, "
                f"Trading Symbol: {position['tradingSymbol']}, "
                f"Security ID: {position['securityId']}, "
                f"Position Type: {position['positionType']}, "
                f"Total Quantity: {position['netQty']}, "
                f"Unrealized Profit: {position['unrealizedProfit']}, "
                f"Realized Profit: {position['realizedProfit']}, "
                f"Buy Avg: {position['buyAvg']}, "
                f"Sell Avg: {position['sellAvg']}, "
                f"Cost Price: {position['costPrice']}"
            )
            print(position_details)

            seq_id = int(position['securityId'])
            url = "https://scanx.dhan.co/scanx/rtscrdt"
            payload = {
                "Data": {
                    "Seg": 5,
                    "Secid": seq_id
                }
            }
            
            try:
                response = requests.post(url, json=payload)
                response.raise_for_status()
                
                if response.status_code == 200:
                    print("Request successful.")

                data = response.json()
                sid = data['data']['sid']
                exch = data['data']['exch']
                isin = data['data']['isin']
                symbol = data['data']['sym']
                name = data['data']['d_sym']
                segment = data['data']['seg']
                last_price = data['data']['Ltp']
                change = data['data']['ch']
                percent_change = data['data']['p_ch']
                volume = data['data']['vol']
                high = data['data']['hg']
                low = data['data']['lo']
                open_price = data['data']['op']
                close_price = data['data']['cl']
                
                # Ensure last_price is not None
                if last_price is None:
                    print("Error: Last price is None. Skipping this iteration.")
                    return



                retrieved_data = (
                    f"SID: {sid}, Exchange: {exch}, ISIN: {isin}, Symbol: {symbol}, Name: {name}, "
                    f"Segment: {segment}, Last Price: {last_price}, Change: {change}, "
                    f"Percent Change: {percent_change}, Volume: {volume}, High: {high}, Low: {low}, "
                    f"Open Price: {open_price}, Close Price: {close_price}"
                )
                print(retrieved_data)

                if position['positionType'] == 'SHORT':
                    next_short_trade_price = trade_start_price - (position['netQty'] * 2000)
                    target_price=next_short_trade_price + (-2 * 2000)
                    print("The next target is " + str(target_price))
                    print("The next  short price is " + str(next_short_trade_price))
                    if float(last_price) > next_short_trade_price:
                        print('short')
                        # Uncomment and update the following lines to place an order if needed
                        res = dhan.place_order(
                           tag=sid,
                           transaction_type=dhan.SELL,
                           exchange_segment=dhan.MCX,
                           product_type=dhan.MARGIN,
                           order_type=dhan.MARKET,
                           validity='DAY',
                           security_id=sid,
                           quantity=1,
                           disclosed_quantity=0,
                           price=0,
                           trigger_price=0,      
                           )
                        print(res)
                    if float(last_price) < target_price :
                         # Uncomment and update the following lines to place an order if needed
                        res = dhan.place_order(
                           tag=sid,
                           transaction_type=dhan.BUY,
                           exchange_segment=dhan.MCX,
                           product_type=dhan.MARGIN,
                           order_type=dhan.MARKET,
                           validity='DAY',
                           security_id=sid,
                           quantity=1,
                           disclosed_quantity=0,
                           price=0,
                           trigger_price=0,      
                           )

                if position['positionType'] == 'LONG':
                    print("Long Position find")
                    next_long_trade_price = trade_start_price - (position['netQty'] * 2000)
                    target_price=next_long_trade_price + (2 * 2000)
                    print("The next target is " + str(target_price))
                    print("The next long buy price is " + str(next_long_trade_price))
                    if float(last_price) < next_long_trade_price:
                        print('buy')
                        res = dhan.place_order(
                           tag=sid,
                           transaction_type=dhan.BUY,
                           exchange_segment=dhan.MCX,
                           product_type=dhan.MARGIN,
                           order_type=dhan.MARKET,
                           validity='DAY',
                           security_id=sid,
                           quantity=1,
                           disclosed_quantity=0,
                           price=0,
                           trigger_price=0,      
                           )
                        print(res)
                    
                    if float(last_price) > target_price :
                         # Uncomment and update the following lines to place an order if needed
                        res = dhan.place_order(
                           tag=sid,
                           transaction_type=dhan.SELL,
                           exchange_segment=dhan.MCX,
                           product_type=dhan.MARGIN,
                           order_type=dhan.MARKET,
                           validity='DAY',
                           security_id=sid,
                           quantity=1,
                           disclosed_quantity=0,
                           price=0,
                           trigger_price=0,      
                          )

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
