"""
The script reads relevance score from data/relevance_score.csv and stores the score in deepend_abt table in nucleus database
"""
#!/usr/bin/python3

import psycopg2
import csv
import configparser
import json

#read config file
Config = configparser.ConfigParser()
Config.read("config.ini")

#get database info from config file
hostname = Config.get("nucleus","hostname")
username = Config.get("nucleus","username")
password = Config.get("nucleus","password")
database = Config.get("nucleus","database")

def putRelevance(conn,activity_id,rel_obj):
            cur=conn.cursor()
            #check if activity_id already in ABT table
            cur.execute("""select * from deepend_abt where activity_id = '{}'""".format(activity_id))
            #if activity_id not found in table
            if cur.rowcount == 0:
                #replace ' with " in relevance score json object
                cur.execute("""insert into deepend_abt(activity_id, relevance) values('{}','{}')""".format((activity_id),(rel_obj)))
                conn.commit()
            #if activity_id already exists in table then update relevance score
            else:
                cur.execute("""update deepend_abt set relevance = '{}' where activity_id = '{}'""".format((rel_obj),activity_id ))
                conn.commit()

"""
Load Relevance score from csv file into deepend_abt table in nucleus database
@params: connection object
"""
def loadData(conn):
    #read data from csv file
    with open('/goorulabs/relevance/relevance_score.csv', newline='') as (csvfile) :
        next(csvfile) #skipping headers from csv file
        reader=csv.reader(csvfile)
        cur = conn.cursor()
        for row in reader:
             #replace ' with " in relevance score json object
             rel_obj = row[1].replace("'", "\"")
             putRelevance(conn,row[0],rel_obj)
        print("data inserted into abt")

#Establish connection with the nucleus database
myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
print("connection success")
loadData(myConnection)
myConnection.close()
