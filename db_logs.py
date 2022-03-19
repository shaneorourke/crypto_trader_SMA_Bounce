import sqlite3 as sql
from datetime import datetime
from binance import Client
import binance_keys as bk


conn = sql.connect('crypto_trading.db')
c = conn.cursor()

client = Client(api_key=bk.API_KEY,api_secret=bk.SECRET_KEY)

replace = ['(',')',',','./data/','csv','.','[',']']
replace_number = ['(',')',',','[',']']

def clean_up_sql_out(text,isnumber):
    if isnumber == 1:
        for s in replace_number:
            text = str(text).replace(s,'')      
    else:
        for s in replace:
            text = str(text).replace(s,'')
    return text


## Last Update
c.execute('SELECT timestamp FROM last_update ORDER BY timestamp DESC LIMIT 1')
result = c.fetchone()
result = clean_up_sql_out(result,0)
time_now = datetime.now()
print(f'Last Update:{result}')
print(f'Time Now:{str(time_now)}')

print()


c.execute('SELECT Currency FROM position')
currencies = c.fetchall()
for curr in currencies:
    curr=clean_up_sql_out(curr,0)
    curr=curr.replace("'","")

    print(f'##### CURRENCY:{curr}')

    ## Orders
    print()
    c.execute(f'SELECT * FROM logs WHERE Currency="{curr}" ORDER BY log_datetime DESC limit 1')
    result = c.fetchall()
    for row in result:
        curr = row[0]
        position = row[1]
        Upping_Quantity = row[2]
        minimum_wallet = row[3]
        binance_buy = row[4]
        usdt_wallet = row[5]
        Quantity = row[6]
        close = row[7]
        buy_sell = row[8]
        ordered = row[9]
        FastSMA = row[10]
        SlowSMA = row[11]
        Fast_over_Slow = row[12]
        side = row[13]
        trigger = row[15]
        log_datetime = row[16]
        print(f'Currency:{curr}') 
        print(f'Position:{position}') 
        print(f'Upping_Quantity:{Upping_Quantity}') 
        print(f'minimum_wallet:{round(minimum_wallet,2)}') 
        print(f'binance_buy:{binance_buy}') 
        print(f'usdt_wallet:{round(usdt_wallet,2)}') 
        print(f'Quantity:{Quantity}') 
        print(f'buy_sell:{buy_sell}') 
        print(f'ordered:{ordered}') 
        print(f'Fast_over_Slow:{Fast_over_Slow}') 
        print(f'trigger:{trigger}') 
        print(f'log_datetime:{log_datetime}')
        print(f'close:{round(close,2)}') 
        print(f'buy_sell:{buy_sell}') 
        print(f'ordered:{ordered}') 
        print(f'FastSMA:{round(FastSMA,2)}') 
        print(f'SlowSMA:{round(SlowSMA,2)}')
        print(f'Fast_over_Slow:{Fast_over_Slow}') 
        print(f'side:{side}')
        print()
