#!/bin/bash

#Copy ds_master table data from datascopedb to ds_master.csv
psql -h postgres-datascopedb.internal.gooru.org -U goorulabs datascopedb -c "\copy (select * from ds_master limit 1000000) to '/goorulabs/relevance/ds_master.csv' delimiter ',' csv header;"
echo "ds_master data copied from database to csv"

#Copy competency_collection_map table data from datascopedb to competency_collection_map.csv
psql -h postgres-datascopedb.internal.gooru.org -U goorulabs datascopedb -c "\copy (select * from competency_collection_map) to '/goorulabs/relevance/competency_collection_map.csv' delimiter ',' csv header;"
echo "competency_collection_map data copied from database to csv"

#Creating tables required on local database
psql -U gooru-labs deepend < create_ds_master.sql
psql -U gooru-labs deepend < create_competency_collection_map.sql
echo "create table done"

#Python script to remove unnecessary columns from csv files and create new csv files
python modify_csv.py
echo "new csv created with relevant columns"

#Copy data from csv files to local database tables
psql -a -d deepend -c "\copy ds_master from '/goorulabs/relevance/new_ds_master.csv' delimiter ',' csv header;"
psql -a -d deepend -c "\copy competency_collection_map from '/goorulabs/relevance/new_competency_collection_map.csv' delimiter ',' csv header;"
echo "csv data added to deepend tables"

#run the relevance algorithm
python3 relevance.py
echo "relevance algorithm completed"

#run load script to load relevance score in ABT table
python3 relevance_load.py
echo "relevance score inserted in deepend_abt table"
