from binance import Client
import pandas as pd
import binance_keys as bk
import sqlite3 as sql
from datetime import datetime
import csv
import os

conn = sql.connect('crypto_trading.db')
c = conn.cursor()

client = Client(api_key=bk.API_KEY,api_secret=bk.SECRET_KEY)

postframe = pd.read_sql('SELECT * FROM position',conn)

stop_loss_percentage = 0.03

today = datetime.now().date()
today = str(today).replace('-','')

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


def last_update():
    c.execute(f'DELETE FROM last_update')
    c.execute(f'INSERT INTO last_update VALUES("{datetime.now()}")')
    conn.commit()

def write_to_file(log_file_name,text):
    if not os.path.exists('logs'):
        os.mkdir('logs')
    file_path = os.path.join('logs',today)
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    file_name = os.path.join(file_path,log_file_name)
    text = str(datetime.now()) + '||' + str(text)
    with open(f'{file_name}', 'a', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow([text])
        f.close()

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
            write_to_file(f'{curr}',f'Binance Order:{order}')
        except Exception as e:
            write_to_file(f'{curr}',f'{log_datetime}:Binance Error:{e}')
        db_order = f'INSERT INTO orders VALUES("{curr}",{qty},"{side}",{price},"{trigger}","{log_datetime}")'
    else:
        db_order = f'INSERT INTO orders VALUES("{curr}",{qty},"{side}",{price},"{trigger}","{log_datetime}")'
    c.execute(db_order)
    conn.commit()
    write_to_file(f'{curr}',f'DB Order:{db_order}')

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

def log_update(Currency,Position,Upping_Quantity,minimum_wallet,binance_buy,usdt_wallet,Quantity,close,buy_sell,ordered,FastSMA,SlowSMA,Fast_over_Slow,side,trigger,log_datetime):
    c.execute(f"""INSERT INTO logs (Currency,Position,Upping_Quantity,minimum_wallet,binance_buy,usdt_wallet,Quantity,close,buy_sell,ordered,FastSMA,SlowSMA,Fast_over_Slow,side,trigger,log_datetime)
                VALUES ("{Currency}",{Position},{Upping_Quantity},{minimum_wallet},{binance_buy},{usdt_wallet},{Quantity},{close},{buy_sell},{ordered},{FastSMA},{SlowSMA},{Fast_over_Slow},"{side}","{trigger}","{log_datetime}")""")
    conn.commit()

def trader(curr):
    log_datetime = datetime.now()
    qty = postframe[postframe.Currency == curr].quantity.values[0]
    df = gethourlydata(curr)
    applytechnicals(df)
    lastrow = df.iloc[-1]
    position = check_position(curr)
    close = lastrow.Close
    wallet = get_wallet(curr)
    usdt = float(wallet[1])
    qty2 = float(usdt) / float(lastrow.Close)
    binance_buy = True ## True to use REAL binance - Must have over more than in spot wallet
    minimum_wallet = close*qty
    Upping_Quantity = False
    buy_sell = False
    ordered = False
    Fast_over_Slow = False
    trigger = ''
    if usdt >= minimum_wallet:
        qty=qty2
        Upping_Quantity = True
    else:
        binance_buy = True
    qty = qty_decimals(curr,close,qty)
    if int(position) == 0:
        side = 'BUY'
        if lastrow.FastSMA > lastrow.SlowSMA:
            Fast_over_Slow = True
            if lastrow.Close < lastrow.SlowSMA:
                market_order(curr,qty,True,binance_buy,lastrow.Close,'buy_fast_over_slow')
                changepos(curr, buy=True)
                buy_sell = True
                ordered = True
        #if lastrow.FastSMA < lastrow.SlowSMA:
        #    if lastrow.Close < lastrow.SlowSMA:
        #        market_order(curr,qty,True,binance_buy,lastrow.Close,'buy_slow_over_fast')
        #        changepos(curr, buy=True)
        #        buy_sell = True
        #        ordered = True
    if int(position) != 0:
        side = 'SELL'
        buy_price = get_buy_value(curr)
        take_profit = float(buy_price) * 0.01
        take_profit_price = float(buy_price) + take_profit
        stop = float(buy_price) - (take_profit * 1.5)
        binance_buy = True ## True to use REAL binance - Must have over more than in spot wallet
        qty = get_buy_qty(curr)
        if lastrow.Close >= take_profit_price:
            market_order(curr,qty,False,binance_buy,lastrow.Close,'TP')
            buy_sell = True
            trigger = 'TP'
            if check_sale_sold(curr):
                changepos(curr,buy=False)
                ordered = True
            else:
                write_to_file(f'{curr}',f'{log_datetime}:SELL ERROR')
        #if lastrow.Close < stop:
        #    trigger = 'SL'
        #    market_order(curr,qty,False,binance_buy,lastrow.Close,'SL')
        #    if check_sale_sold(curr):
        #        changepos(curr,buy=False)
        #        ordered = True
        #    else:
        #        write_to_file(f'{curr}',f'{log_datetime}:SELL ERROR')
    log_update(curr,position,Upping_Quantity,minimum_wallet,binance_buy,usdt,qty,close,buy_sell,ordered,lastrow.FastSMA,lastrow.SlowSMA,Fast_over_Slow,side,trigger,log_datetime)


for coin in postframe.Currency:
    print(datetime.now())
    trader(coin)
    last_update()