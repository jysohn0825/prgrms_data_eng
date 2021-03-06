# -*- coding: utf-8 -*-
"""Redshift Testing

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Q_joX3as2JSGmCcF5rSQl7KCoYD_oG_w
"""

#redshift 조작을 위한 모듈
import psycopg2 

#redshift connect cursor 정보
def get_Redshift_connection(): 
    host = ""
    redshift_user = ""
    redshift_pass = ""
    port = 
    dbname = ""
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(readonly=True, autocommit=True)
    return conn.cursor()

#cursor object를 만들고 아래의 sql을 날림
cur = get_Redshift_connection() 

sql = "SELECT * FROM raw_data.user_session_channel LIMIT 10;"
cur.execute(sql)

#sql의 결과를 rows에 담음
rows = cur.fetchall() 

for r in rows:
  print(r)


#위의 방법을 pandas로 부르는 법
import pandas.io.sql as sqlio 

df = sqlio.read_sql_query(sql,conn)

df.head() 
