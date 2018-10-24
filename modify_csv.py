"""
delete irrelevant columns from csv file and make a new file
"""

import csv

#copy from ds_master.csv to new_ds_master.csv
with open("/goorulabs/relevance/ds_master.csv", "r") as source:
    rdr = csv.reader(source)
    with open("/goorulabs/relevance/new_ds_master.csv","w") as result:
        wtr = csv.writer(result)
        for r in rdr:
            wtr.writerow( (r[0], r[12], r[14], r[15], r[23], r[24], r[26], r[27]) )

#copy from competency_collection_map.csv to new_competency_collection_map.csv
with open("/goorulabs/relevance/competency_collection_map.csv", "r") as source:
    rdr = csv.reader(source)
    with open("/goorulabs/relevance/new_competency_collection_map.csv","w") as result:
        wtr = csv.writer(result)
        for r in rdr:
            wtr.writerow( (r[0], r[1], r[3], r[6]) )
 
