import sqlite3 as sql
conn = sql.connect('crypto_trading.db')
c = conn.cursor()

c.execute('CREATE TABLE IF NOT EXISTS position (Currency text, position boolean, quantity int)')
conn.commit()

c.execute('SELECT position FROM position WHERE Currency = "BTCUSDT"')
result = c.fetchone()
if result == None:
    c.execute('INSERT OR REPLACE INTO position VALUES ("BTCUSDT",0,0.001)')
conn.commit()

c.execute('CREATE TABLE IF NOT EXISTS hourlydata ("Index" text, Time datetime, Open float, High float, Low float, Close float, FastSMA float, SlowSMA float)')
conn.commit()

c.execute('CREATE TABLE IF NOT EXISTS orders (Currency text, quantity float, market text, price float, trigger text, market_date timestamp DEFAULT current_date)')
conn.commit()

c.execute('CREATE TABLE IF NOT EXISTS futures_orders (Currency text, quantity float, market text, positionSide text, type text, price float, trigger text, market_date timestamp DEFAULT current_date)')
conn.commit()

c.execute('CREATE TABLE IF NOT EXISTS trailing_stop_loss (Currency text, stop_price float, market_date timestamp DEFAULT current_date)')
conn.commit()

c.execute('CREATE TABLE IF NOT EXISTS last_update (timestamp DEFAULT current_date)')
conn.commit()

c.execute('CREATE TABLE IF NOT EXISTS waiting_for_entry (Currency text, waiting boolean)')
conn.commit()

c.execute('Drop Table logs')
c.execute('CREATE TABLE logs (Currency text, Position integer, Upping_Quantity boolean, minimum_wallet float, binance_buy boolean, usdt_wallet, Quantity float, close float, buy_sell boolean, ordered boolean, FastSMA float, SlowSMA float, Fast_over_Slow boolean, side text, trigger text, log_datetime timestamp)')
conn.commit()

c.close()