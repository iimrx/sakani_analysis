#!/bin/bash

python3 code/sakani_etl.py #extraction, transformation and loading data
echo "Finished ETL Proccess.!"
sleep 5

#after we load the data to sqlite3 db, we converting it to (.sql) file path to make it eaiser,
#for us to load it to multiple DWH.
sqlite3 db/sakani.db .dump > db/sakani.sql
echo "Finished Converting to SQL File.!"