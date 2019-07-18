#!/usr/bin/python3
import boto3
import pymysql
import datetime
import os
import re
import csv
from zipfile import ZipFile



session = boto3.session.Session(region_name='cn-north-1')
sqs = session.client('sqs')
s3 = session.client('s3')
BUCKET_NAME = "bill-test"# 账单s3桶名
MYSQL_USER = 'liyonghui'
MYSQL_PASSWD = 'shiguang'
MYSQL_HOST = 'bill-test.crqqtvsjmosc.rds.cn-north-1.amazonaws.com.cn'
SQS_URL = 'https://sqs.cn-north-1.amazonaws.com.cn/718707510307/billtest'

def create_database_table(user,passwd,host):
    try:
        db = pymysql.connect(host, user, passwd, port=3306,local_infile=True)
        cursor = db.cursor()
        cursor.execute("create database if not exists billing DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
        cursor.execute('use billing')
        sql = '''create table IF NOT EXISTS  bill_t (
        id bigint(20) unsigned NOT NULL auto_increment,
        InvoiceID VARCHAR(255) NOT NULL,
        PayerAccountId VARCHAR(255) NOT NULL,
        LinkedAccountId VARCHAR(255) NOT NULL,
        RecordType VARCHAR(255) NOT NULL,
        RecordID VARCHAR(255) NOT NULL,
        ProductName VARCHAR(255),
        RateId VARCHAR(255),
        SubscriptionId VARCHAR(255),
        PricingPlanId VARCHAR(255),
        UsageType VARCHAR(255),
        Operation VARCHAR(255),
        vailabilityZone VARCHAR(255),
        ReservedInstance VARCHAR(255),
        ItemDescription VARCHAR(255),
        UsageStartDate timestamp,
        UsageEndDate timestamp,
        UsageQuantity VARCHAR(255),
        BlendedRate VARCHAR(255),
        BlendedCost VARCHAR(255),
        UnBlendedRate VARCHAR(255),
        UnBlendedCost VARCHAR(255),
        ResourceId VARCHAR(255),
        user_owner VARCHAR(255),
        created_at timestamp not null,
        PRIMARY KEY  (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'''
        cursor.execute(sql)
        db.close()
    except:
        print ("Could not connect this database!")
def insert_data(user,passwd,host):
    try:
        db = pymysql.connect(host,user, passwd, port=3306,local_infile=True)
    except:
        print ("Could not connect this database!")
    cursor = db.cursor()
    cursor.execute('use billing')
    sql = '''load data local infile '/tmp/output.csv' into table bill_t FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' (InvoiceID, PayerAccountId, LinkedAccountId, RecordType, RecordID, ProductName, RateId, SubscriptionId, PricingPlanId, UsageType, Operation, vailabilityZone, ReservedInstance, ItemDescription, UsageStartDate, UsageEndDate, UsageQuantity, BlendedRate, BlendedCost, UnBlendedRate, UnBlendedCost, ResourceId, user_owner)'''
    cursor.execute(sql)
    db.commit()
    print ("Insert Sucess!")
    db.close()
def un_zip(file_name):
    zip_file = ZipFile(file_name)
    print ('############################Unzip file############################')
    for names in zip_file.namelist():
        print (names)
        zip_file.extract(names,'/tmp/')
    print ('#################Rename Mian CSV file to output.csv###############')
    if re.search('[1-9]\d*-aws-billing-detailed-line-items-with-resources-and-tags-ACTS-\d{4}-\d{2}.csv',names):
        os.rename('/tmp/'+names,'/tmp/output_temp.csv')
        print ('######################list file in /tmp/############################')
        print (os.listdir('/tmp/'))

    zip_file.close()

def remove_table_header():
    df=open('/tmp/output_temp.csv').readlines()
    df[0]=''
    with open('/tmp/output.csv','w') as f:
        f.writelines(df)

while True:
    mess = response = sqs.receive_message(
        QueueUrl=SQS_URL,
        AttributeNames=[
            'SenderId',
        ],
        MessageAttributeNames=[
            'ALL',
        ],
    )
#    print (mess)
    if 'Messages' in mess:
        for message in mess['Messages']:
            start = datetime.datetime.now()
            print ('Start time:%s' % start)
            bill_name = message['Body']
            file_path = '/tmp/' + bill_name
            sqs.delete_message(QueueUrl= SQS_URL, ReceiptHandle=message['ReceiptHandle'])
            try:
                s3.download_file(BUCKET_NAME,bill_name,file_path)
                print ("Download file %s Sucess！" % bill_name)
            except:
                print ("Can not download this file from S3!")
            un_zip(file_path)
            remove_table_header()
            create_database_table(MYSQL_USER,MYSQL_PASSWD,MYSQL_HOST)
            insert_data(MYSQL_USER,MYSQL_PASSWD,MYSQL_HOST)
            print ('remove file %s /tmp/output.csv and /tmp/output_temp.csv' % file_path)
            os.remove(file_path)
            os.remove('/tmp/output_temp.csv')
            os.remove('/tmp/output.csv')
            end = datetime.datetime.now()
            print ('End time:%s' % end)
            print ('Spend time:%s'%str(end-start))
