
import psycopg2
import csv
import configparser
import json

Config = configparser.ConfigParser()
Config.read("../Relevance/config.ini")

#get database info from config file
hostname = Config.get("datascopedb","hostname")
username = Config.get("datascopedb","username")
password = Config.get("datascopedb","password")
database = Config.get("datascopedb","database")


hostname2 = Config.get("nucleus","hostname")
username2 = Config.get("nucleus","username")
password2 = Config.get("nucleus","password")
database2 = Config.get("nucleus","database")


w = 0.3

def getEngagement(conn,conn2,activity_id):
            cur=conn.cursor()
            cur2 = conn2.cursor()
            cur2.execute("select taxonomy from deepend_abt where activity_id = '{}'".format(activity_id))
            #print  cur2.fetchall()[0][0].keys()
	    competency = []
            competency = cur2.fetchall()[0][0].keys()
            reslist = []
            res = []
            eng_list = []
            for m in competency:
		#print activity_id, m
                #mq = "select distinct(resource_id) from ds_master where collection_id in ( select collection_id from competency_collection_map where competency_code = '{}')".format(m);
		mq = "select distinct(activity_id) from deepend_abt where taxonomy->'{}' is NOT NULL".format(m);
                cur2.execute(mq)
                reslist = [items[0] for items in cur2.fetchall()]
                cur.execute("select sum(reaction) from ds_master where resource_id = '{}' group by resource_id".format(activity_id))
                reac = [items[0] for items in cur.fetchall()]
                maxreacq = 'select sum(reaction) from ds_master where resource_id in (\'' + '\',\''.join(map(str, reslist)) + '\') group by resource_id';

                cur.execute(maxreacq)
                reaclist = [items[0] for items in cur.fetchall()]
                maxreac=0
                reaction_L=0
                if reaclist:
                    maxreac = max(reaclist)
                if reac:
                    reaction_L=int(reac[0])
                if maxreac:
                    reaction_L=float(reaction_L)/maxreac
                query="select sum(views) from ds_master where resource_id = '{}' group by resource_id".format(activity_id)
                cur.execute(query)
                views=[items[0] for items in cur.fetchall()]
                query = 'select sum(views) from ds_master where resource_id in (\'' + '\',\''.join(map(str, reslist)) + '\') group by resource_id'
                cur.execute(query)

                viewlist=[items[0] for items in cur.fetchall()]
                maxview=0;
                view_L=0
                if viewlist:
                    maxview=max(viewlist)
                if views:
                    view_L=int(views[0])
                if maxview:
                    view_L=float(view_L)/maxview

                engagement = w*view_L + (1-w) * reaction_L
                eng_list.append(engagement)

            json_obj = {}
            if len(competency) > 0:
                json_array = [{ ( (competency[i].split('.',1))[1] ): eng_list[i]} for i in range(len(competency))]
                json_obj[activity_id] = json_array
                #print("obj: ", json_obj)
            return json_obj
            
def calculateEngagement( conn,conn2) :
    cur = conn2.cursor()
    cur.execute("select distinct(activity_id) from deepend_abt limit 5")
    resource = cur.fetchall()
	
    csv_file="/goorulabs/relevance/testing_engagement_score.csv"
    with open(csv_file, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        writer.writerow(['Resource ID', 'Engagement'])
    
        for p in resource:
            if p[0] == None:
                continue
            json_obj=getEngagement(conn,conn2,p[0])
	    print json_obj
            #writer.writerow([p[0], json.loads(json.dumps(json_obj))])
        print("You're done! Output written to engagement_score.csv")


myConnection = psycopg2.connect( host=hostname, user=username, password = password,  dbname=database)
myConnection2 = psycopg2.connect( host=hostname2, user=username2, password = password2,  dbname=database2)
calculateEngagement( myConnection, myConnection2)
myConnection.close()
myConnection2.close()
