"""
The script takes resource information from deepend database, calculates the relevance score for each resource and stoes the score in relevance_score.csv file
"""
#!/usr/bin/python

import psycopg2
import csv
import configparser
from math import exp
import json
import numpy
import random
from scipy.stats import truncnorm
#read config file
Config = configparser.ConfigParser()
Config.read("config.ini")

#get database info from config file
hostname = Config.get("deepend","hostname")
username = Config.get("deepend","username")
password = Config.get("deepend","password")
database = Config.get("deepend","database")


hostname2 = Config.get("nucleus","hostname")
username2 = Config.get("nucleus","username")
password2 = Config.get("nucleus","password")
database2 = Config.get("nucleus","database")

#get parameters from config file
rel_alpha = float(Config.get("relevance_parameters","alpha"))
rel_lambda = float(Config.get("relevance_parameters", "lambda"))
def getRelevance(conn,conn2,original_id,alpha_list):
            cur=conn.cursor() 
            cur2 = conn2.cursor()
            cur2.execute("select id from content where original_content_id =" + "'{}'".format(original_id) )
            copied_ids = [items[0] for items in cur2.fetchall()]
            # print copied_ids
            competencies = {}
            for activity_id in copied_ids:
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
                    rel_alpha=random.choice(alpha_list)
                    #print("Rel alpha",rel_alpha)
                    #Calculate relevance 
                    relevance = rel_alpha +( (1 - rel_alpha) * rel_temp * exp((-1) * rel_lambda * len(competency)) )
                    #append relevance to rel_list
                    rel_list.append(relevance)

                #Create json object for relevance score
                for i in range(len(competency)):
                    if competency[i] in competencies:
                        competencies[competency[i]][0] += res_list[i]
                        competencies[competency[i]][1] += 1
                    else:
                        competencies[competency[i]] = [res_list[i], 1]

            fin_ans = {}
            for key, value in competencies.items():
                fin_ans[key] = value[0]/value[1]
            if fin_ans:
                print("original_id: ",  fin_ans)
            return fin_ans

"""
Calculate relevance score of the resource
@params: database connection object
Stores relevance score in a csv file
"""
def get_truncated_normal(mean=0, sd=1, low=0, upp=10):
    return truncnorm(
        (low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)

#Calculate random alpha values for relevance scores
def generateRandomValues():
#    numpy_arr=numpy.random.normal(0.5,1,1000)
    numpy_arr=get_truncated_normal(mean=0.5, sd=0.5, low=0, upp=1)
    numpy_arr=numpy_arr.rvs(5000)
    alpha_list=numpy_arr.tolist()
    return alpha_list

def calculateRelevance(conn, conn2):
    cur = conn2.cursor()
    alpha_list=generateRandomValues()
    #get all the distinct resources from the database
    cur.execute("select distinct(activity_id) from deepend_abt")
    resource = cur.fetchall()

    print("resources fetched")

    #Open csv file to store relevance score
    csvfile = "/goorulabs/relevance/relevance_score_for_classified.csv"
    with open(csvfile, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        writer.writerow(['Resource ID', 'Relevance'])
 
        #iterate over all the resources
        for p in resource:
            if p[0] == None:
                continue
            json_obj=getRelevance(conn,conn2,p[0],alpha_list)
            #Write resource and relevance to csv file
            # check if json.loads(json.dumps(json_obj) this is required
            writer.writerow([p[0], json.dumps(json_obj)])

    print("output written to relevance_score_for_classified.csv")        

#Connect to database
myConnection = psycopg2.connect( host=hostname, user=username, password = password,  dbname=database)
myConnection2 = psycopg2.connect( host=hostname2, user=username2, password = password2,  dbname=database2)
calculateRelevance( myConnection, myConnection2)
myConnection.close()
myConnection2.close()

