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


def trader(curr):
    qty = postframe[postframe.Currency == curr].quantity.values[0]
    df = gethourlydata(curr)
    applytechnicals(df)
    lastrow = df.iloc[-1]
    position = check_position(curr)
    write_to_file(f'{curr}',f'[info]Currency:[/info]{curr}')
    write_to_file(f'{curr}',f'[info]Position:[/info]{position}')
    close = lastrow.Close
    write_to_file(f'{curr}',f'[info]Current Price:[/info][integer]{float(close)}[/integer]')
    write_to_file(f'{curr}',f'[info]FastSMA Price:[/info][integer]{round(float(lastrow.FastSMA),2)}[/integer]')
    write_to_file(f'{curr}',f'[info]SlowSMA Price:[/info][integer]{round(float(lastrow.SlowSMA),2)}[/integer]')
    if int(position) == 0:
        if lastrow.FastSMA > lastrow.SlowSMA:
            write_to_file(f'{curr}','[info]Looking for BUY[/info]')
            if lastrow.Close < lastrow.SlowSMA:
                write_to_file(f'{curr}',f'Slow SMA Bounce Long Position Trigger')
                market_order(curr,qty,True,False,lastrow.Close,'buy')
                changepos(curr, buy=True)
        else:
            write_to_file(f'{curr}','[neg_warning]Slow SMA Greater Than Fast SMA[/neg_warning]')                
    if int(position) != 0:
        write_to_file(f'{curr}','[info]Looking for SELL[/info]')
        buy_price = get_buy_value(curr)
        take_profit = buy_price * 0.01
        take_profit_price = buy_price + take_profit
        stop = buy_price - (take_profit * 1.5)
        write_to_file(f'{curr}',f'[info]Take Profit:[/info][integer]{float(take_profit_price)}[/integer]')
        write_to_file(f'{curr}',f'[info]Stop Price:[/info][integer]{float(stop)}[/integer]')
        if lastrow.Close >= take_profit_price:
            write_to_file(f'{curr}','[pos_warning]Take Profit Triggered Sale[/pos_warning]')
            market_order(curr,qty,False,False,lastrow.Close,'TP')
            changepos(curr,buy=False)
        if lastrow.Close < stop:
            write_to_file(f'{curr}','[neg_warning]STOP LOSS TRIGGERED SALE[/neg_warning]')
            market_order(curr,qty,False,False,lastrow.Close,'SL')
            changepos(curr,buy=False)

for coin in postframe.Currency:
    trader(coin)
    last_update()
    write_to_file(f'{coin}','')