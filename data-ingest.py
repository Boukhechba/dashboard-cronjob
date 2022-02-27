#!/usr/bin/env python3.7
#from __future__ import print_function
#import sys
import os
import boto3
import time
#import logging
#import io
#import os

from datetime import datetime,timedelta
from botocore.client import Config
#import dateutil
import pandas as pd
import json 
import pytz
import gzip
from collections import defaultdict
import yaml
from smart_open import open
#import s3fs
import inspect
from database import Database




def read_yaml(file_path):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

config=read_yaml(os.path.join(os.path.dirname(__file__), "config.yml"))

# construct the argument parse and parse the arguments
db_host     = config['DATABASE']['db_host']
db_port     = config['DATABASE']['db_port']
db_user     = config['DATABASE']['db_user']
db_password = config['DATABASE']['db_password']
db_name = config['DATABASE']['db_name']
marker = config['AWS']['marker']
complete_data = defaultdict(list)



#################################################################
# Supporting methods                                            #
#################################################################


def json_processor(f,study):
    lines = f.readlines()
#    print(lines)
    j=1
    for l in lines:    
        obj=l
#        print(obj)
        if j < len(lines)-1:
            if ("}"in str(l[-2:])):
                obj=str(l[:-1], "utf-8")
            else:
                obj=str(l[:-2], "utf-8")
        j=j+1
        try:
            line=json.loads(obj)
            if ( 'SWear' in line["$type"]):
                data_type = line.pop("$type").split('.')[-1]
                datum = data_type
            else:
                line["Sensus OS"] = line["$type"].split(',')[1]
                line["Data Type"] = line.pop("$type").split(',')[0]
                data_type_split = line["Data Type"].split('.')
                data_type = data_type_split[len(data_type_split)-1]
                if data_type[-5:] == "Datum":
                    datum = data_type[:-5]
                else:
                    datum = data_type
            if "PID" in line:
                line.pop("PID")
                
            
            if datum == "Activity":
                line["Activity Mode"] = line.pop("Activity")
            complete_data[datum,study].append(line)
        except: pass
#        except Exception as e:
#            print(study+" : "+str(e))

def push_to_db(frame,study):
    #Send data to DB     
    database = Database(db_name, db_user, db_password, db_host, db_port)
    if not database.table_exists('summary_'+study):
        database.create_summary_table(study)
    summary_columns=[]
    database.cursor.execute("SELECT column_name FROM information_schema.columns where table_schema='boukhech_sensus' and table_name='summary_"+study.replace("-","_")+"'") 
    for row in database.cursor:
        summary_columns.append(row[0])
    
    if not frame.empty:
        frame=frame.groupby(['DeviceId','ParticipantId','Timestamp','OperatingSystem','datum'])['Id'].sum().unstack('datum').reset_index().fillna(0)
        #        df=df.pivot_table( index=['ParticipantId','Timestamp','OperatingSystem'],columns='Type', values='Id',fill_value=0,aggfunc=np.sum).reset_index()
        frame['ParticipantId']=frame['ParticipantId'].astype(str)+":"+frame['DeviceId'].astype(str)+":"+frame['OperatingSystem'].astype(str)
        del frame['OperatingSystem']
        del frame['DeviceId']
        frame=frame.rename(columns={'ParticipantId': 'deviceid'})
        frame=frame.rename(columns={'Timestamp': 'timestamp'})
        summary_columns=database.insert_from_pandas(study.replace("-","_"),frame,summary_columns)
        frame=pd.DataFrame()
    database.close_connection()        

def count_probes(key,data):
    frame=pd.DataFrame()
    if not data.empty:                    
        try:     
            data['datum']=key
#            print(key)
            #data=pd.concat([data,d], ignore_index=True)
            grouped1 = pd.DataFrame(data.Id.groupby([data.datum,(data.Timestamp.dropna().str[:13].replace("T", " ")),data.ParticipantId,data.DeviceId,data.OperatingSystem]).count()).reset_index()
            if 'ScriptState'==key :
                subdata=data.loc[data['datum']== "ScriptState"]
                grouped=pd.DataFrame(subdata.Id.groupby([subdata.State,(subdata.Timestamp.dropna().str[:13].replace("T", " ")),subdata.ParticipantId,subdata.DeviceId,subdata.OperatingSystem]).count()).reset_index()      
                grouped=grouped.rename(columns={'State': 'datum'})
                grouped.datum="ScriptDatum_"+grouped.datum
                frame=pd.concat([frame, grouped, grouped1], ignore_index=True)
            else:
                frame=pd.concat([frame, grouped1], ignore_index=True)
        except Exception as e: 
            print(e) 
            
    return frame
    
#################################################################
# Handler                                                       #
#################################################################
#timeTH = datetime.utcnow()- timedelta(minutes=5)
#timeTH = datetime.utcnow()- timedelta(hours=24)
timeTH = datetime.utcnow()- timedelta(days=90)

absolute_start_time = time.time()

AWS_S3_BUCKET = "sensus-7bd075fa-2b31-448a-a048-cbc505e2ab65"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

s3 = boto3.client('s3', region_name="us-east-1", aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY, config=Config(signature_version="s3", s3={'addressing_style': 'path'}))
errorList= []
listdate =[]
paginator = s3.get_paginator( "list_objects" )


#marker="data/2019-06-17/RPN6942_ScreenDatum_1560763733034-1560763733034.csv.gz"

allowedProbesList=["ScriptStateDatum"]
#allowedProbesList=["HeartbeatDatum"]

if(str(marker)=="None"):
    page_iterator = paginator.paginate( Bucket = AWS_S3_BUCKET, Prefix = "data/")
else:
    page_iterator = paginator.paginate( Bucket = AWS_S3_BUCKET, Prefix = "data/",PaginationConfig={'StartingToken': marker})
#




i=0
y=0
frame=pd.DataFrame()
time = timeTH.replace(tzinfo=pytz.utc)


print("---  Fetching data from S3..")
print("---  Starting from the following marker: "+ str(marker))

for page in page_iterator:
    if "Contents" in page:
        marker=page['Marker']
        for obj in page["Contents"]:
            #Check if the object was uploaded within our targeted time window
#            if(obj['LastModified']>time and obj['Size']>0 and datum in allowedProbesList):         
            if(obj['LastModified']>time and obj['Size']>0):
                name=obj['Key'].split("/")[1]
                try:
                    response = s3.get_object(Bucket=AWS_S3_BUCKET, Key=obj['Key'])
                    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                    if status == 200:
                        f = gzip.open(response.get("Body"), 'rb')
                        json_processor(f,name)
                    else:
                        print("Unsuccessful S3 get_object response. Status - {status}")
                except Exception as e: 
                    print(e) 
                i=i+1

                
print("---  Data processing finished. Inserting data in the database...  ---")

              
for probe,study in complete_data:
    print("---  Inserting data into "+study+ "->"+ probe)
    data=pd.DataFrame(complete_data[probe,study])
    push_to_db(count_probes(probe,data),study)
    
print("---  Storing AWs marker")

config['AWS']['marker']=marker

try:
    with open(os.path.join(os.path.dirname(__file__), "config.yml"), 'w') as file:
        documents = yaml.dump(config, file)
except Exception as e: 
    print(e) 
       
print("--- Done ! "+str(i)+" files have been processed.")


