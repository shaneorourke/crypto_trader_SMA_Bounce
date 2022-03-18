import sqlite3 as sql

conn = sql.connect('crypto_trading.db')
c = conn.cursor()

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

