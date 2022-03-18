import sqlite3 as sql
from datetime import datetime
from binance import Client
import binance_keys as bk
from rich.console import Console
from rich.theme import Theme

conn = sql.connect('crypto_trading.db')
c = conn.cursor()

customer_theme = Theme({'info':"bold green italic",'integer':'blue bold','pos_warning':'yellow bold italic','neg_warning':'red bold'})
console = Console(color_system='auto',theme=customer_theme)

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

c.execute('DROP table hourly')
conn.commit()
## Last Update
c.execute('SELECT * FROM hourly')
result = c.fetchall()
result = clean_up_sql_out(result,1)
print(result)

console.print()
