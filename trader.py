from binance import Client
from numpy import right_shift
import pandas as pd
import binance_keys as bk
import sqlite3 as sql
from rich.console import Console
from rich.theme import Theme
from time import sleep
from datetime import datetime

conn = sql.connect('crypto_trading.db')
c = conn.cursor()

customer_theme = Theme({'info':"bold green italic",'integer':'blue bold','pos_warning':'yellow bold italic','neg_warning':'red bold'})
console = Console(color_system='auto',theme=customer_theme)

client = Client(api_key=bk.API_KEY,api_secret=bk.SECRET_KEY)

postframe = pd.read_sql('SELECT * FROM position',conn)

stop_loss_percentage = 0.03


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

def round_float(value):
    value = round(float(value),2)
    return value

def last_update():
    c.execute(f'DELETE FROM last_update')
    c.execute(f'INSERT INTO last_update VALUES("{datetime.now()}")')
    conn.commit()

def changepos(curr, buy=True):
    if buy:
        c.execute(f'UPDATE position SET position = True WHERE Currency="{curr}"')
    else:
        c.execute(f'UPDATE position SET position = False WHERE Currency="{curr}"')
    conn.commit()
        
def check_position(curr):
    c.execute(f'SELECT position FROM position WHERE Currency="{curr}"')
    result = c.fetchone()
    result = clean_up_sql_out(result,1)
    return result

def gethourlydata(symbol):
    frame = pd.DataFrame(client.get_historical_klines(symbol,'1h','25 hours ago UTC'))
    frame = frame.iloc[:,:5]
    frame.columns = ['Time','Open','High','Low','Close']
    frame[['Open','High','Low','Close']] = frame[['Open','High','Low','Close']].astype(float)
    frame.Time = pd.to_datetime(frame.Time, unit='ms')
    return frame

def applytechnicals(df):
    df['FastSMA'] = df.Close.rolling(7).mean()
    df['SlowSMA'] = df.Close.rolling(25).mean()


def market_order(curr,qty,buy=True,binance_buy=False,price=float,trigger=str):
    log_datetime = datetime.now()
    if buy:
        side='BUY'
    else:
        side='SELL'
    if binance_buy:
        try:
            order = client.create_order(symbol=curr,side=side,type='MARKET',quantity=qty)
            console.print(f'[info]Binance Order:[/info]{order}')
        except Exception as e:
            console.print(f'[neg_warning]Binance Error:[/neg_warning]{e}')
        db_order = f'INSERT INTO orders VALUES("{curr}",{qty},"{side}",{price},"{trigger}","{log_datetime}")'
    else:
        db_order = f'INSERT INTO orders VALUES("{curr}",{qty},"{side}",{price},"{trigger}","{log_datetime}")'
    c.execute(db_order)
    conn.commit()
    console.print(f'[info]DB Order:[/info]{db_order}')
    

def get_buy_value(curr):
    c.execute(f'SELECT price FROM orders WHERE Currency = "{curr}" order by market_date desc LIMIT 1')
    result = c.fetchone()
    result = clean_up_sql_out(result,1)
    return result

def get_buy_qty(curr):
    c.execute(f'SELECT quantity FROM orders WHERE Currency = "{curr}" order by market_date desc LIMIT 1')
    result = c.fetchone()
    result = clean_up_sql_out(result,1)
    return result

def get_wallet(curr):
    right_curr = curr[3:]
    left_curr = curr[:-4]
    left_curr_bal = client.get_asset_balance(left_curr)
    right_curr_bal = client.get_asset_balance(right_curr)
    return left_curr_bal['free'],right_curr_bal['free']

def qty_decimals(curr,close=float,qty=float):
    base_qty = postframe[postframe.Currency == curr].quantity.values[0]
    if len(str(round(close,2))) - str(close).find('.') == 2:
        close = float(str(round(close,2))+'1')
    if str(close).find('.') == -1:
        close = float(str(round(close,2))+'.11')
    if qty < base_qty:
        qty=base_qty
    else:
        decimal_limit=len(str(round(close,2)).replace('.',''))-1
        qty=str(qty)[:decimal_limit]
    return qty

def check_sale_sold(curr):
    wallet = get_wallet(curr)
    if wallet[0] > wallet[1]:
        return False
    else:
        return True

def log_update(Currency,Position,Upping_Quantity,minimum_wallet,binance_buy,usdt_wallet,Quantity,close,buy_sell,ordered,FastSMA,SlowSMA,Fast_over_Slow,side,trend,trigger,log_datetime):
    c.execute(f"""INSERT INTO logs (Currency,Position,Upping_Quantity,minimum_wallet,binance_buy,usdt_wallet,Quantity,close,buy_sell,ordered,FastSMA,SlowSMA,Fast_over_Slow,side,trend,trigger,log_datetime)
                VALUES ("{Currency}",{Position},{Upping_Quantity},{minimum_wallet},{binance_buy},{usdt_wallet},{Quantity},{close},{buy_sell},{ordered},{FastSMA},{SlowSMA},{Fast_over_Slow},"{side}","{trend}","{trigger}","{log_datetime}")""")
    conn.commit()

def uptrend(curr):
    ## if SlowSMA 5 hours ago is lower than SLowSMA now then TRUE (Uptrend) Else (Downtrend)
    c.execute(f"""""")
    uptrend = c.fetchone()
    uptrend = clean_up_sql_out(uptrend,1)
    return uptrend

def trader(curr):
    log_datetime = datetime.now()
    qty = postframe[postframe.Currency == curr].quantity.values[0]
    df = gethourlydata(curr)
    applytechnicals(df)
    lastrow = df.iloc[-1]
    position = check_position(curr)
    console.print(f'[info]Currency:[/info]{curr}')
    console.print(f'[info]Position:[/info]{position}')
    close = lastrow.Close
    wallet = get_wallet(curr)
    usdt = float(wallet[1])
    qty2 = float(usdt) / float(lastrow.Close)
    binance_buy = False ## True to use REAL binance - Must have over more than in spot wallet
    minimum_wallet = close*qty
    Upping_Quantity = False
    buy_sell = False
    ordered = False
    Fast_over_Slow = False
    trigger = ''
    trend = 'Up'
    if usdt >= minimum_wallet:
        console.print(f'[info]Upping Quantity:[/info][integer]{float(qty_decimals(curr,close,qty2))}[/integer]')
        qty=qty2
        Upping_Quantity = True
    else:
        binance_buy = False
        console.print(f'[info]More USDT Needed Min is:[/info][integer]{minimum_wallet}[/integer]')
    qty = qty_decimals(curr,close,qty)
    console.print(f'[info]USDT Wallet:[/info][integer]{float(usdt)}[/integer]')
    console.print(f'[info]Current Price:[/info][integer]{float(close)}[/integer]')
    if int(position) == 0:
        side = 'BUY'
        console.print(f'[info]FastSMA Price:[/info][integer]{round(float(lastrow.FastSMA),2)}[/integer]')
        console.print(f'[info]SlowSMA Price:[/info][integer]{round(float(lastrow.SlowSMA),2)}[/integer]')
        if lastrow.FastSMA > lastrow.SlowSMA:
            Fast_over_Slow = True
            console.print('[info]Looking for BUY Fast over Slow[/info]')
            console.print(f'[info]Binance Buy:[/info][integer]{binance_buy}[/integer]')
            if lastrow.Close < lastrow.SlowSMA:
                # Long Position
                console.print(f'Fast over Slow SMA Bounce Long Position Trigger')
                market_order(curr,qty,True,binance_buy,lastrow.Close,'buy_fast_over_slow')
                changepos(curr, buy=True)
                buy_sell = True
                ordered = True
            else:
                distane_from_trigger = close - lastrow.SlowSMA
                console.print(f'[info]Close needs to drop:[/info][integer]{round(float(distane_from_trigger),2)}[/integer]')
        if lastrow.FastSMA < lastrow.SlowSMA:
            console.print('[info]Looking for BUY Slow over Fast[/info]')
            if lastrow.Close < lastrow.SlowSMA:
                # Short Position -- Currently in long - change to short / sell for futures
                console.print(f'Slow over Fast SMA Bounce Long Position Trigger')
                market_order(curr,qty,True,binance_buy,lastrow.Close,'buy_slow_over_fast')
                changepos(curr, buy=True)
                buy_sell = True
                ordered = True  
            else:
                distane_from_trigger = close - lastrow.SlowSMA
                console.print(f'[info]Close needs to rise:[/info][integer]{round(float(distane_from_trigger),2)}[/integer]')
    if int(position) != 0:
        side = 'SELL'
        console.print('[info]Looking for SELL[/info]')
        buy_price = get_buy_value(curr)
        take_profit = float(buy_price) * 0.01
        take_profit_price = float(buy_price) + take_profit
        stop = float(buy_price) - (take_profit * 1.5)
        binance_buy = False ## True to use REAL binance - Must have over more than in spot wallet
        console.print(f'[info]Buy Price:[/info][integer]{round(float(buy_price),2)}[/integer]')
        console.print(f'[info]Take Profit:[/info][integer]{round(float(take_profit_price),2)}[/integer]')
        console.print(f'[info]Stop Price:[/info][integer]{round(float(stop),2)}[/integer]')
        qty = get_buy_qty(curr)
        console.print(f'[info]Buy Qty:[/info][integer]{round(float(qty),2)}[/integer]')
        console.print(f'[info]Binance Buy:[/info][integer]{binance_buy}[/integer]')
        if lastrow.Close >= take_profit_price:
            console.print('[pos_warning]Take Profit Triggered Sale[/pos_warning]')
            market_order(curr,qty,False,False,lastrow.Close,'TP')
            buy_sell = True
            trigger = 'TP'
            if check_sale_sold(curr):
                changepos(curr,buy=False)
                ordered = True
            else:
                console.print('[info]SELL ERROR[/info]')
        if lastrow.Close < stop:
            trigger = 'SL'
            console.print('[neg_warning]STOP LOSS TRIGGERED SALE[/neg_warning]')
            market_order(curr,qty,False,False,lastrow.Close,'SL')
            if check_sale_sold(curr):
                changepos(curr,buy=False)
                ordered = True
            else:
                console.print('[info]SELL ERROR[/info]')
    log_update(curr,position,Upping_Quantity,minimum_wallet,binance_buy,usdt,qty,close,buy_sell,ordered,lastrow.FastSMA,lastrow.SlowSMA,Fast_over_Slow,side,trend,trigger,log_datetime)



running=True
while running:
    for coin in postframe.Currency:
        trader(coin)
        last_update()
    console.print()
    sleep(1)