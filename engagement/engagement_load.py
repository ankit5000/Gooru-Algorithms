"""
The script reads engagement score from engagement_score.csv and stores the score in deepend_abt table in nucleus database
"""
#!/usr/bin/python3

import psycopg2
import csv
import configparser
import json

#read config file
Config = configparser.ConfigParser()
Config.read("../Relevance/config.ini")

#get database info from config file
hostname = Config.get("nucleus","hostname")
username = Config.get("nucleus","username")
password = Config.get("nucleus","password")
database = Config.get("nucleus","database")

def putEngagement(conn,activity_id,eng_obj):
            #check if activity_id already in ABT table
            cur=conn.cursor()
            cur.execute("""select * from deepend_abt where activity_id = '{}'""".format(activity_id))
            #if activity_id not found in table
            if cur.rowcount == 0:
                cur.execute("""insert into deepend_abt(activity_id, engagement) values('{}','{}')""".format((activity_id),(eng_obj)))
                conn.commit()
            #if activity_id already in table then update engagement score
            else:
                cur.execute("""update deepend_abt set engagement = '{}' where activity_id = '{}' """.format((eng_obj),(activity_id)))
                conn.commit()

"""
Load Engagement score from csv file into deepend_abt table in nucleus database
@params: connection object
"""
def loadData(conn):
    #read from csv file
    with open('/goorulabs/relevance/engagement_score.csv', newline='') as (csvfile) :
        next(csvfile) #skipping headers from csv file
        reader=csv.reader(csvfile)
        cur = conn.cursor()
        for row in reader:
            #replace ' with " in engagement score json object
            eng_obj = row[1].replace("'", "\"")
            putEngagement(conn,row[0],eng_obj)               
        print("data inserted into abt")

#Establish connection with the nucleus database
myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
print("connection success")
loadData(myConnection)
myConnection.close()

