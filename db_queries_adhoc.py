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

## Last Update
c.execute('DELETE FROM orders WHERE market_date BETWEEN "2022-03-19 22:14:06.284954" and "2022-03-20 03:15:06.235669"')
result = c.fetchall()
result = clean_up_sql_out(result,1)
conn.commit()
print(result)


#2022-03-19 22:14:06.284954
#2022-03-20 03:15:06.235669
