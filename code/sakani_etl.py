#import needed packages
import os
import pandas as pd
import zipfile as zp
import sqlite3 as sql3
from dotenv import dotenv_values
from IPython.display import display

#importing our secrets from environment varibales
config = dotenv_values(".env")
os.environ['KAGGLE_USERNAME'] = config.get('KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = config.get('KAGGLE_KEY')

#connecting to kaggle via api
from kaggle.api.kaggle_api_extended import KaggleApi
kgl_api = KaggleApi()
kgl_api.authenticate()

#here we declare where our paths is for (main data folder, kaggle dataset, extraction path)
data_path = '../data' #main data path
sqlite_path = '../db' #main db path
kaggle_dataset = 'majedalhulayel/sakani-projects-saudi-arabia' #kaggle dataset path

#lets get the data
kgl_api.dataset_download_files(kaggle_dataset, data_path)

#unzip the dataset file and save to new dir
try:
  if os.path.exists(data_path): #if the data folder do exists enter here
    with zp.ZipFile(data_path+'/sakani-projects-saudi-arabia.zip') as data: #take from original path
      data.extractall(data_path) #uzip into the path if exists
      print(f"Done extracting all files to: {data_path}") #message
      
  else: #if the data folder doesn't exists enter here
    print(f'Creating new data folder: {data_path}\n') #message
    os.mkdir(data_path) #create new folder if not exists
    with zp.ZipFile(data_path+'/sakani-projects-saudi-arabia.zip') as data: #take from original path
      data.extractall(data_path) #unzip into the new path
    print(f"Done extracting all files to: {data_path}") #message
except:
  print("Invalid file")

#lets play with the dataset
df = pd.read_csv("../data/Sakani Projects.csv")

#rename some columns to more clean naming
df.rename(columns = {'under_construction_status':'construction_status','unit_types_0':'unit_type',\
                     'available_units_for_auctions_count':'available_auctions_units','available_units_count':'available_units'}, inplace=True)

#clean row-level data
df['developer_name'].fillna('لا يوجد مدخل', inplace=True)
df['publish_date'].ffill(inplace=True) #filling nan values with prev value
df['construction_status'].fillna('no entry', inplace=True)
df['location'] = df['location_lat'].astype(str) +','+ df['location_lon'].astype(str) #create new column to handle the lat,lot location

#un_wanted columns to delete
df.drop(['city_id','region_id','region_key','region_order_sequence','city_order_sequence','group_unit_id','promoted','unit_types_1', \
          'unit_types_2','type','resource_id','resource_type','subsidizable','max_street_width','max_unit_age','max_bathroom','driver_room', \
          'elevator','basement','delegated_by_broker','maid_room','min_bathroom','min_street_width','min_unit_age','pool','publish','use_register_interest_flag', \
          'location_lat', 'location_lon'], axis=1, inplace=True)

#lets see the dataset after cleaning and before loading it to the DWH
display(df.head()) #here we see the dataset in style of dataframe
print(f"Dataset rows/columns: {df.shape}") #here we see the dataset shape after cleaning

#lets save the new data to another file
print(f"Saving the new data to another file: {data_path}/cleaned_data.csv")
df.to_csv(data_path+'/cleaned_data.csv', index=False)

#lets load our cleand data into sqlite table
try:
  if os.path.exists(sqlite_path): #if the data folder do exists enter here
    engine = sql3.connect(config.get('SQLITE_DB'))
    df.to_sql(config.get('SQLITE_TABLE'), engine, index=False)
    print(f"Loading data into sqlite3 database ...!")

  else: #if the data folder doesn't exists enter here
    print(f'Creating new db folder: {sqlite_path}\n') #message
    os.mkdir(sqlite_path) #create new folder if not exists
    engine = sql3.connect(config.get('SQLITE_DB'))
    df.to_sql(config.get('SQLITE_TABLE'), engine, index=False)
    print(f"Loading data into sqlite3 database ...!")
except Exception as e:
  print(f"Invalid db ... \n{e}")
