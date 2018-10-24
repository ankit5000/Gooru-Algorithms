"""
The script takes resource information from deepend database, calculates the relevance score for each resource and stoes the score in relevance_score.csv file
"""
#!/usr/bin/python

import psycopg2
import csv
import configparser
from math import exp
import json

#read config file
Config = configparser.ConfigParser()
Config.read("config.ini")

#get database info from config file
hostname = Config.get("deepend","hostname") 
username = Config.get("deepend","username")
password = Config.get("deepend","password")
database = Config.get("deepend","database")

#get parameters from config file
rel_alpha = float(Config.get("relevance_parameters","alpha"))
rel_lambda = float(Config.get("relevance_parameters", "lambda"))

def getRelevance(conn,activity_id):
            cur=conn.cursor() 
            #get competency of the resource
           
            cur.execute("select distinct(competency_code) from competency_collection_map where collection_id in (select collection_id from ds_master where resource_id = '{}')".format(activity_id))
            #store competency in a list
            competency = [items[0] for items in cur.fetchall()]

            #list to store relevance corresponding to each competency
            rel_list = []
 
            #iterate over all the competencies
            for m in competency:
                #store count of all lessons in competency m
                s = 0
                
                if(len(m)>0):
                    #get number of lessons in competency m and using resource p
                    cur.execute("select count(lesson_id) from ds_master where resource_id ='{}' and collection_id in (select collection_id from competency_collection_map where competency_code = '{}')".format(activity_id, m))
                    lesson = cur.fetchall()
                    #store count of all lessons in competency m and using resource p
                    sp = lesson[0][0]

                    #get number of lessons in competency m
                    cur.execute("select count(lesson_id) from ds_master where resource_id in (select resource_id from ds_master where collection_id in (select collection_id from competency_collection_map where competency_code='{}'))".format(m))
                    lesson = cur.fetchall()
                    s = lesson[0][0]
                #check if denominator(s) is zero
                if s != 0:
                    rel_temp = (sp/s)
                else:
                    rel_temp = 0
 
                #Calculate relevance 
                relevance = rel_alpha +( (1 - rel_alpha) * rel_temp * exp((-1) * rel_lambda * len(competency)) )
                #append relevance to rel_list
                rel_list.append(relevance)

            #Create json object for relevance score
            json_array = [{competency[i]: rel_list[i]} for i in range(len(competency))]
            json_obj = {}
            json_obj[activity_id] = json_array
            return json_obj


"""
Calculate relevance score of the resource
@params: database connection object
Stores relevance score in a csv file
"""
def calculateRelevance(conn):
    cur = conn.cursor()
    
    #get all the distinct resources from the database
    cur.execute("select distinct(resource_id) from ds_master")
    resource = cur.fetchall()

    print("resources fetched")

    #Open csv file to store relevance score
    csvfile = "/goorulabs/relevance/relevance_score.csv"
    with open(csvfile, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        writer.writerow(['Resource ID', 'Relevance'])
 
        #iterate over all the resources
        for p in resource:
            if p[0] == None:
                continue
            json_obj=getRelevance(conn,p[0])
            #Write resource and relevance to csv file
            writer.writerow([p[0], json.dumps(json_obj)])

    print("output written to relevance_score.csv")        

#Connect to database
myConnection = psycopg2.connect(host=hostname, user=username, password = password,  dbname=database)
calculateRelevance(myConnection)
myConnection.close()

