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

def round_float(value):
    value = round(float(value),2)
    return value

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
    #df.to_sql(name='hourlydata',con=conn,if_exists='append')


def market_order(curr,qty,buy=True,binance_buy=False,price=float,trigger=str):
    if buy:
        side='BUY'
    else:
        side='SELL'
    if binance_buy:    
        order = client.create_order(symbol=curr,side=side,type='MARKET',quantity=qty)
        order = f'INSERT INTO orders VALUES("{curr}",{qty},"{side}",{price},"{trigger}","{datetime.now()}")'
    else:
        order = f'INSERT INTO orders VALUES("{curr}",{qty},"{side}",{price},"{trigger}","{datetime.now()}")'
        write_to_file(f'{curr}',order)
        c.execute(order)
        conn.commit()
    write_to_file(f'{curr}',order)

def get_buy_value(curr):
    c.execute(f'SELECT price FROM orders WHERE Currency = "{curr}" order by market_date desc LIMIT 1')
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

def trader(curr):
    qty = postframe[postframe.Currency == curr].quantity.values[0]
    df = gethourlydata(curr)
    applytechnicals(df)
    lastrow = df.iloc[-1]
    position = check_position(curr)
    write_to_file(f'{curr}',f'Currency:{curr}')
    write_to_file(f'{curr}',f'Position:{position}')
    close = lastrow.Close
    wallet = get_wallet(curr)
    usdt = float(wallet[1])
    qty2 = float(usdt) / float(lastrow.Close)
    binance_buy = True ## True to use REAL binance - Must have over more than in spot wallet
    minimum_wallet = close*qty
    if usdt >= minimum_wallet:
        write_to_file(f'{curr}',f'Upping Quantity:{float(qty_decimals(curr,close,qty2))}')
        qty=qty2
    else:
        binance_buy = False
        write_to_file(f'{curr}',f'More USDT Needed Min is:{minimum_wallet}')
    write_to_file(f'{curr}',f'Binance Buy:{binance_buy}')
    qty = qty_decimals(curr,close,qty)
    write_to_file(f'{curr}',f'USDT Wallet:{float(usdt)}')
    write_to_file(f'{curr}',f'Current Price:{float(close)}')
    write_to_file(f'{curr}',f'FastSMA Price:{round(float(lastrow.FastSMA),2)}')
    write_to_file(f'{curr}',f'SlowSMA Price:{round(float(lastrow.SlowSMA),2)}')
    if int(position) == 0:
        if lastrow.FastSMA > lastrow.SlowSMA:
            write_to_file(f'{curr}','Looking for BUY Fast over Slow')
            if lastrow.Close < lastrow.SlowSMA:
                write_to_file(f'{curr}',f'Slow SMA Bounce Long Position Trigger')
                market_order(curr,qty,True,False,lastrow.Close,'buy_fast_over_slow')
                changepos(curr, buy=True)
            else:
                distane_from_trigger = close - lastrow.SlowSMA
                write_to_file(f'{curr}',f'Close needs to drop:{round(float(distane_from_trigger),2)}')
        ## Uncomment for futures - should be short / sell here
        if lastrow.FastSMA < lastrow.SlowSMA:
            write_to_file(f'{curr}','Looking for BUY Slow over Fast')
            if lastrow.Close > lastrow.SlowSMA:
                write_to_file(f'{curr}',f'Slow over Fast SMA Bounce Long Position Trigger')
                market_order(curr,qty,True,False,lastrow.Close,'buy_slow_over_fast')
                changepos(curr, buy=True)               
            else:
                distane_from_trigger = close - lastrow.SlowSMA
                write_to_file(f'{curr}',f'Close needs to rise:{round(float(distane_from_trigger),2)}')
    if int(position) != 0:
        write_to_file(f'{curr}','Looking for SELL')
        buy_price = get_buy_value(curr)
        take_profit = float(buy_price) * 0.01
        take_profit_price = float(buy_price) + take_profit
        stop = float(buy_price) - (take_profit * 1.5)
        write_to_file(f'{curr}',f'Buy Price:{round(float(buy_price),2)}')
        write_to_file(f'{curr}',f'Take Profit:{round(float(take_profit_price),2)}')
        write_to_file(f'{curr}',f'Stop Price:{round(float(stop),2)}')
        if lastrow.Close >= take_profit_price:
            write_to_file(f'{curr}','Take Profit Triggered Sale')
            market_order(curr,qty,False,False,lastrow.Close,'TP')
            changepos(curr,buy=False)
        if lastrow.Close < stop:
            write_to_file(f'{curr}','STOP LOSS TRIGGERED SALE')
            market_order(curr,qty,False,False,lastrow.Close,'SL')
            changepos(curr,buy=False)

for coin in postframe.Currency:
    trader(coin)
    last_update()
    write_to_file(f'{coin}','')