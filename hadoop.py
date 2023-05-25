import pandas as pd
import logging
from flask import Flask, render_template, request, redirect, \
url_for, flash, make_response, session, jsonify
import os
os.chdir('/home/cdsw/Clerical_Resolution_Online_Widget/flask_poc')
import difflib
from dlh_utils import sessions
from dlh_utils import utilities
from markupsafe import Markup
import ast
import numpy as np
import configparser
import getpass
import pwd
import subprocess
import helper_functions as hf
app=Flask(__name__)
logging.getLogger('werkzeug').disabled=True

spark=sessions.getOrCreateSparkSession(appName='crow_test', size='medium')
config = configparser.ConfigParser()
config.read('config_flow.ini')
rec_id=config['id_variables']['record_id']
clust_id=config['id_variables']['cluster_id']
#ile=spark.sql(f"SELECT * FROM {config['filepath']['file']}")
user = os.environ['HADOOP_USER_NAME']

hive_folder= '/ons/crow/hive/testfile_9'

!hadoop dfs -get /ons/crow/hive/testfile_9

#loading in 
process = subprocess.Popen(["hadoop", "fs","-get",hive_folder,'/home/cdsw/tmp' ])

process.communicate()

#sending out 

process = subprocess.Popen(["hadoop", "fs","-put",local_folder,hive_folder ])

process.communicate()
#
process = subprocess.Popen(["hadoop", "fs","-mv",hive_folder, '/ons/crow/hive/testfile_NEW'])

process.communicate()