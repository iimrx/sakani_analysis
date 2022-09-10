#import needed packages
import os
import pandas as pd
import zipfile as zp
from dotenv import dotenv_values

#importing our secrets from environment varibales
config = dotenv_values(".env")
os.environ['KAGGLE_USERNAME'] = config.get('KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = config.get('KAGGLE_KEY')

#lets connect to kaggle via api
from kaggle.api.kaggle_api_extended import KaggleApi
kgl_api = KaggleApi()
kgl_api.authenticate()

#lets unzip the file containing the dataset
data_folder_path = '../data'
kaggle_dataset = 'majedalhulayel/sakani-projects-saudi-arabia'
extracted_dataset_path = '../data/sakani-projects-saudi-arabia.zip'

#lets get the data
kgl_api.dataset_download_files(kaggle_dataset, data_folder_path)

#lets unzip the dataset file
try:
  if os.path.exists(data_folder_path):
    with zp.ZipFile(extracted_dataset_path) as data: #original file path
      data.extractall(data_folder_path) #saving path
      print(f"Done extracting all files to: {data_folder_path}")
  else:
    print(f'Creating new folder: {data_folder_path}\n')
    os.mkdir(data_folder_path)
    with zp.ZipFile(extracted_dataset_path) as data: #original file path
      data.extractall(data_folder_path) #saving path
    print(f"Done extracting all files to: {data_folder_path}")
except:
  print("Invalid file")

#lets play with the dataset
df = pd.read_csv("../data/Sakani Projects.csv")

#len of the old columns
print(f'Length of Data (Before Column Filtered): {len(df.columns)}')

#un_wanted columns to delete
df.drop(['city_id','region_id','region_key','region_order_sequence','city_order_sequence','group_unit_id','promoted','unit_types_1', \
          'unit_types_2','type','resource_id','resource_type','subsidizable','max_street_width','max_unit_age','max_bathroom','driver_room', \
          'elevator','basement','delegated_by_broker','maid_room','min_bathroom','min_street_width','min_unit_age','pool','publish','use_register_interest_flag'], axis=1, inplace=True)

#len of the new columns
print(f'Length of Data (After Column Filtered): {len(df.columns)}')

#lets clean row-level data
df['developer_name'].fillna('لا يوجد مدخل', inplace=True)
df['publish_date'].ffill(inplace=True) #filling nan values with prev value
df['under_construction_status'].fillna('no entry', inplace=True)

#lets see the dataset after cleaning and before loading it to the DWH
df.style #here we see the dataset in style of dataframe
print(f"Dataset rows/columns: {df.shape}") #here we see the dataset shape after cleaning

#lets save the new data to diff location
df.to_csv(data_folder_path+'/cleaned_data.csv', index=False)