import sqlite3 as sql
conn = sql.connect('crypto_trading.db')
c = conn.cursor()

#c.execute('INSERT INTO orders VALUES("BTCUSDT",0.0014,"BUY",39909.51,"buy_fast_over_slow","2022-03-16 18:49:06.179667")')

c.execute('DELETE FROM orders WHERE market_date="2022-03-17 12:17:06.124808"')
conn.commit()