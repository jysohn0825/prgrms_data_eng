'''
# 웹상(https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv
)에 존재하는 이름 성별 CSV 파일을 Redshift에 있는 테이블로 복사
# 헤더의 값이 들어가는 이슈 제거
# 현재 Job을 수행할 때마다 테이블에 레코드들이 들어가는데 이를 idempotent하게 수정
# extract, transform, load를 각각의 task로 만들지말고 하나의 task안에서 이 세가지를 다 호출(Airflow의 경우 태스크들간의 데이터 이동이 복잡)
'''
  
from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import requests
import logging
import psycopg2 

# Redshift connection 함수
# ID,PW는 redshit web UI의 connection에서 적어주고 사용하고 변수로 사용
def get_Redshift_connection():
    host = ""
    redshift_user = ""
    redshift_pass = ""
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

"""# ETL 함수를 하나씩 정의"""

import requests

def extract(url): # 아래의 주어진 csv를 받아와서 requests.get을 통해 읽어온다
    f = requests.get(link)
    return (f.text)

def transform(text):
    lines = text.split("\n")[1:] # 큰 text를 줄 단위로 변형하여 리턴 & CSV 파일의 헤더 제거 (Idempotency 보장!)
    return lines

def load(lines): 
    # 데이터가 크지 않기 때문에 하나씩 실행
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음 -> 데이터의 중복을 방지하고 매번 크게 부를 수 없기 때문에 만들고, 실패시에 좋음
    # 사실 redshift에서는 하나씩 parsing 해서 insert 하는 것이 아니라 copy(표준 SQL X)를 사용
    cur = get_Redshift_connection()
    
    # 아래에서 end를 하면 transcation의 이미가 사라짐 -> 문제가 생길시 원래 내용으로 돌아가지 못 함, TRUNCATE는 rollback이 안 되기 때문에 DELETE 를 씀
    sql = "BEGIN;DELETE FROM TABLE raw_data.name_gender;"
    for l in lines:
        if l != '':
            (name, gender) = l.split(",")
            sql += "INSERT INTO raw_data.name_gender VALUES ('{name}', '{gender}');"
    # END 대신 COMMIT을 써도 되고 BEGIN과 END 사이에서 실패가 있으면 아예 다 실패로 처리되고 처음으로 돌아가고 실패에 있는 것들은 원래 상태로 돌아간다
    sql += "END;"
    # 에러가 생겨도 airflow에 넘어가고 fail로 처리한다 -> 에러 처리를 해서 JOB을 원하는데로 할 수 있지만 fail을 통해 조치를 취하게 할 수 있다 
    cur.execute(sql)

"""# 이제 Extract부터 함수를 하나씩 실행"""
def etl():

    # Airflow 를 사용하면 아래의 링크가 업데이트 될 때마다 실행 한다던지 스케줄링이 가능!!!
    # link가 하드코딩 되어있는데 Variable에 정해놓고 쓸 수 있다 -> DAG web UI에서 amin->variable에 넣을 수 있다
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)
    lines = transform(data)
    load(lines)

# DAG의 속성을 하나씩 주거나 dictionary 처럼 줄 수 있다
dag_second_assignment = DAG(
	dag_id = 'second_assignment',
	start_date = datetime(2020,8,10), # 날짜가 미래인 경우 실행이 안됨
	schedule_interval = '0 2 * * *')  # 적당히 조절

# airflow를 쓴다면 90% 정도는 PythonOperator 를 사용한다
task = PythonOperator(
	task_id = 'perform_etl',
	python_callable = etl,
	dag = dag_second_assignment)
  