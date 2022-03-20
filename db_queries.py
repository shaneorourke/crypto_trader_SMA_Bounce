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


## Last Update
c.execute('SELECT timestamp FROM last_update ORDER BY timestamp DESC LIMIT 1')
result = c.fetchone()
result = clean_up_sql_out(result,0)
time_now = datetime.now()
console.print(f'[info]Last Update[/info][integer]:{result}[/integer]')
console.print(f'[info]Datetime Now[/info][integer]:{str(time_now)}[/integer]')

console.print()


c.execute('SELECT Currency FROM position')
currencies = c.fetchall()
for curr in currencies:
    curr=clean_up_sql_out(curr,0)
    curr=curr.replace("'","")

    console.print(f'[info]##### CURRENCY[/info][integer]:{curr}[/integer]')

    ## Last Buy Price
    c.execute(f'SELECT price FROM orders WHERE Currency="{curr}" and market = "BUY" ORDER BY market_date DESC limit 1')
    buy_price = c.fetchall()
    buy_price = clean_up_sql_out(buy_price,1)

    ## Made First Sale
    c.execute(f'SELECT count(*) FROM orders WHERE Currency="{curr}" and market = "SELL"')
    sale_made = c.fetchall()
    sale_made = clean_up_sql_out(sale_made,1)

    ## Position Open
    c.execute(f'SELECT position FROM position WHERE Currency="{curr}"')
    result = c.fetchall()
    pos = clean_up_sql_out(result,0)
    if pos == '0':
        position = 'BUYING'
    else:
        position = 'SELLING'
    console.print(f'[info]Position[/info][integer]:{position}[/integer]')
    if position == 'SELLING':
        console.print(f'[info]Buy Price[/info][integer]:{buy_price}[/integer]')

    ## Current Price
    price=client.get_symbol_ticker(symbol=curr)
    console.print(f'[info]Current Price[/info][integer]:{float(price["price"])}[/integer]')

    ## Profitability
    c.execute(f"""with last_order as (select market, market_date from orders WHERE Currency="{curr}" ORDER BY market_date DESC LIMIT 1)
                , order_check as(select case when market = 'BUY' then (SELECT round(sum(case when market = "SELL" then price else price*-1 end),2) as profit FROM orders WHERE Currency="{curr}" and market_date != (SELECT market_date FROM last_order)) else (SELECT round(sum(case when market = "SELL" then price else price*-1 end),2) as profit FROM orders WHERE Currency="{curr}") end FROM last_order)
                select * from order_check""")
    result = c.fetchall()
    curr_profit = clean_up_sql_out(result,1)
    if sale_made !='0':
        if curr_profit != 'None':
            profit = round((float(curr_profit)/float(price['price']))*100,2)
            console.print(f'[info]Profit Percentage[/info][integer]:{profit}%[/integer]')
            qty = 0.001
            usdt_value = float(price['price']) * qty
            usdt_profit = usdt_value*(profit/100)
            console.print(f'[info]USDT Profit:$[/info][integer]{round(usdt_profit,2)}[/integer]')


    ## Take Profit Details Est
    c.execute('SELECT round(price+(price * 0.01),2) FROM orders WHERE market = "BUY" ORDER BY market_date DESC LIMIT 1')
    result = c.fetchall()
    if position == 'SELLING':
        tp = clean_up_sql_out(result,1)
        console.print(f'[info]Take Profit[/info][integer]:{tp}[/integer]')

    ## Stop Details Est
    c.execute('SELECT round(price-(price * 0.015),2) FROM orders WHERE market = "BUY" ORDER BY market_date DESC LIMIT 1')
    result = c.fetchall()
    if position == 'SELLING':
        stop = clean_up_sql_out(result,1)
        console.print(f'[info]Stop Limit[/info][integer]:{stop}[/integer]')

    ## P and L
    PL = round(float(buy_price) - float(price['price']),2)
    console.print(f'[info]Profit & Loss[/info][integer]:{PL}[/integer]')


    console.print()
## Profitability
c.execute(f"""with last_order as (select market, market_date from orders ORDER BY market_date DESC LIMIT 1)
            , order_check as(select case when market = 'BUY' then (SELECT round(sum(case when market = "SELL" then price else price*-1 end),2) as profit FROM orders WHERE market_date != (SELECT market_date FROM last_order)) else (SELECT round(sum(case when market = "SELL" then price else price*-1 end),2) as profit FROM orders) end FROM last_order)
            select * from order_check""")
result = c.fetchall()
tot_profit = clean_up_sql_out(result,1)
if sale_made !='0':
    if tot_profit != 'None':
        total_profit = round((float(curr_profit)/float(price['price']))*100,2)
        console.print(f'[info]##### Total Profit Percentage[/info][integer]:{total_profit}%[/integer]')
        qty = 0.001
        usdt_value = float(price['price']) * qty
        usdt_profit = usdt_value*(total_profit/100)
        console.print(f'[info]Total USDT Profit:$[/info][integer]{round(usdt_profit,2)}[/integer]')