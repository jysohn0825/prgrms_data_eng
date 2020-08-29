from airflow import DAG
from airflow.macros import *
from airflow.models import Variable

# plugin 폴더에 있는 만든 모듈들 ppt에서 설명한 2개의 task
from plugins.postgres_to_s3_operator import PostgresToS3Operator
from plugins.s3_to_redshift_operator import S3ToRedshiftOperator


DAG_ID = "Postgres_to_Redshift"
dag = DAG(
    DAG_ID,
    schedule_interval="@once", # 주기적으로 실행은 안되지만 webUI or terminal에서 사용함(개발할때사용 -> 배포 시 interval로 변경)
    max_active_runs=1, 	# 한 인터벌만 실행
    concurrency=2, 		# 인터벌 중 테스크는 2개
    start_date=datetime(2020, 8, 10),
    catchup=False
)

# Postgres 와 RedShift(비어있음 -> Copy 할 예정)에 테이블이 있어야 한다
tables = [
    "customer_features",
    "customer_variants"
]


# s3_bucket, local_dir
s3_bucket = 'grepp-data-engineering' 
local_dir = './'           # 실제 프로덕션에서는 공간이 충분한 폴더 (volume)로 맞춰준다
s3_key_prefix = ''  # 본인의 ID에 맞게 수정
schema = ''        # 본인이 사용하는 스키마에 맞게 수정
prev_task = None

for table in tables:
    s3_key=s3_key_prefix+'/'+table+'.tsv'

    postgrestos3 = PostgresToS3Operator(
        table="public."+table, # Postgres 의 테이블 이름
        s3_bucket=s3_bucket, # S3 어디에 링크할지 정하기 위함
        s3_key=s3_key,
        data_dir=local_dir, # 어떤 로컬 디스크에 할건지! - default는 current dir에 쓰임
        dag=dag,
        task_id="Postgres_to_S3"+"_"+table
    )

    s3toredshift = S3ToRedshiftOperator( # 위에 작업을 이어 받아서 RedShift에 bulk update, iam_role_을 통해 permission도 얻음
        schema=schema,
        table=table,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        copy_options="delimiter '\\t' COMPUPDATE ON", # compudate on -> 압축 옵션
        aws_conn_id='aws_s3_default', # 모든 task에서 각각의 접근 permission을 위함
        task_id='S3_to_Redshift'+"_"+table,
        dag=dag
    )
    if prev_task is not None:
        prev_task >> postgrestos3 >> s3toredshift
    else:
        postgrestos3 >> s3toredshift
    prev_task = s3toredshift
