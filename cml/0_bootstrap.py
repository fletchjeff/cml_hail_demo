## Part 0: Bootstrap File
# You need to at the start of the project. It will install the requirements, creates the 
# STORAGE and HAIL_TMP environment variables 

# The STORAGE environment variable is the Cloud Storage location used by the DataLake 
# to store hive data. On AWS it will s3a://[something], on Azure it will be 
# abfs://[something] and on CDSW cluster, it will be hdfs://[something]

# Install the requirements
!pip3 install -r requirements.txt --progress-bar off

# Create the directories and upload data

from cmlbootstrap import CMLBootstrap
from IPython.display import Javascript, HTML
import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime

run_time_suffix = datetime.datetime.now()
run_time_suffix = run_time_suffix.strftime("%d%m%Y%H%M%S")

# Set the setup variables needed by CMLBootstrap
HOST = os.getenv("CDSW_API_URL").split(
    ":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split(
    "/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY") 
PROJECT_NAME = os.getenv("CDSW_PROJECT")  

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

# Set the STORAGE environment variable
try : 
  storage=os.environ["STORAGE"]
except:
  if os.path.exists("/etc/hadoop/conf/hive-site.xml"):
    tree = ET.parse('/etc/hadoop/conf/hive-site.xml')
    root = tree.getroot()
    for prop in root.findall('property'):
      if prop.find('name').text == "hive.metastore.warehouse.dir":
        storage = prop.find('value').text.split("/")[0] + "//" + prop.find('value').text.split("/")[2]
  else:
    storage = "/user/" + os.getenv("HADOOP_USER_NAME")
  storage_environment_params = {"STORAGE":storage}
  storage_environment = cml.create_environment_variable(storage_environment_params)
  os.environ["STORAGE"] = storage
  
# Set the STORAGE environment variable
try: 
  hail_data=os.environ["HAIL_DATA"]
except:
  if "user" in storage:
    hail_data = storage + "/data/hail"
  else:
    hail_data = storage + "/user/" + os.getenv("HADOOP_USER_NAME") + "/data/hail"
    hail_data_params = {"HAIL_DATA":hail_data}
    hail_data_environment = cml.create_environment_variable(hail_data_params)
    os.environ["HAIL_DATA"] = hail_data
  

# Create the hail/tmp directory and copy the data
!hdfs dfs -mkdir -p $STORAGE/user/$HADOOP_USER_NAME/data/hail/tmp
!hdfs dfs -copyFromLocal data/* $STORAGE/user/$HADOOP_USER_NAME/data/hail/

