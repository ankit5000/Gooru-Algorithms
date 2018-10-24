#!/bin/bash

#run the engagement algorithm
python3 engagement.py
echo "engagement algorithm completed"

#run load script to load relevance score in ABT table
python3 engagement_load.py
echo "engagement score inserted in deepend_abt table"

