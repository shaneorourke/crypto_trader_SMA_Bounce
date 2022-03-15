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
    c.execute(f'SELECT * FROM orders WHERE Currency="{curr}" ORDER BY market_date ASC')
    result = c.fetchall()
    for row in result:
        print(f'Orders:{clean_up_sql_out(row,1)}')

    print()
