
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
import pydoop

app=Flask(__name__)
logging.getLogger('werkzeug').disabled=True

spark=sessions.getOrCreateSparkSession(appName='crow_test', size='medium')
config = configparser.ConfigParser()
config.read('config_flow.ini')
rec_id=config['id_variables']['record_id']
clust_id=config['id_variables']['cluster_id']
#ile=spark.sql(f"SELECT * FROM {config['filepath']['file']}")
user = os.environ['HADOOP_USER_NAME']




def advance_cluster(df):
  #note this function is very clunky and could likely be improved. 
  """
  1)A Function to: determine the number of matches made in a given cluster
  2)If 1 or 0 unmatcher records in cluster remain; progress to the next cluster. 
  
  Parameters: None
  Returns: None
  
  """
  num_in_cluster=len(df.loc[df['Sequential_Cluster_Id']==session['index']])
  print(num_in_cluster)
  list_decided=[set(ast.literal_eval(i)) for i in df.loc[df['Sequential_Cluster_Id']==session['index']]['Match']]
  print(list_decided)
  uni_set_decided={x for l in list_decided for x in l}
  print(uni_set_decided)
  num_decided=len(uni_set_decided)
  if (num_in_cluster-num_decided)<=1:
      for r_id in [x for x in df.loc[df['Sequential_Cluster_Id']==session['index']][rec_id] if x not in uni_set_decided]: 
            df.loc[df[rec_id]==r_id,'Match']=f"['No Match In Cluster For {r_id}']"
      session['index'] = int(session['index'])+ 1
      
def check_matching_done(df):
  """
  A function to check if all the records have a match/non-match status
  
  Parameters: dataframe
  Returns: Boolean
  
  """
  if len(df[df.Match == '[]'])>0 : 
    return 0
  if len(df[df.Match == '[]'])==0:
    print('matching_done')
    return 1
  
def save_rename_hive(dataframe, old_path,new_path):
    """
    A function that takes in a pandas dataframe, and saves it to a hive location, while deleting an old path location
     
    Parameters: Dataframe (Pd.dataframe), old_path (Sring); hive file path to be deleted, new_path (String); new file path to be saved to. 
    Returns: None 
    
    """
    sparkDF=spark.createDataFrame(dataframe)
    sparkDF.registerTempTable("temp_table")
    spark.sql(f"""DROP TABLE IF EXISTS {old_path}""")
    #unhash to delete after new table created 
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {new_path} AS SELECT * FROM temp_table""")
    
def save_rename_parquet(dataframe, old_local_location, old_hdfs_location, new_hdfs_location):
    """
    
    
    """
    dataframe.to_parquet('/home/cdsw/Clerical_Resolution_Online_Widget/tmp/lost_my_mind_6/')
    dataframe.to_parquet('/home/cdsw/Clerical_Resolution_Online_Widget/tmp/tempfile')
    
    process = subprocess.Popen(["hadoop", "fs","-put",'/home/cdsw/Clerical_Resolution_Online_Widget/tmp/tempfile','/ons/crow/crow_test_parquet'])
    process.communicate()
    process = subprocess.Popen(["hadoop", "fs","-mv","/ons/crow/crow_test_parquet/tempfile", new_hdfs_location])
    process.communicate()
   # os.remove('/home/cdsw/Clerical_Resolution_Online_Widget/tmp/tempfile')
    print('hdfs file saved at {new_hdfs_location}')


def get_save_paths(origin_file_path,origin_file_path_fl):
    """
    Takes the input filepath and creates a filepaths for the inprogress and done status of that file. 
    
    Parameters: origin_file_path (string); path of hive location, origin_file_path_fl (List); dot separated list. 
    Returns: in_proh_path (String), filepath_done (String)
    
    
    """
    if 'inprogress' in origin_file_path_fl[-1]:

            # If it is the same user
            if (user in origin_file_path_fl[-1]):
                # Dont rename the file
                in_prog_path= origin_file_path
                end_file_name=origin_file_path_fl[-1][:-11]+'_done'
                
                # create the filepath name for when the file is finished
                filepath_done = "/".join(origin_file_path_fl[:-1]+[end_file_name])

            else:

                print('USER_NOT_IN_NAME')

                # Rename the file to contain the additional user
                
                in_prog_name = origin_file_path_fl[-1][:-11]+f'_{user}'+'_inprogress'
                in_prog_path="/".join(origin_file_path_fl[:-1]+[in_prog_name])

                end_file_name=origin_file_path_fl[-1][:-11]+f'_{user}'+'_done'
                filepath_done="/".join(origin_file_path_fl[:-1]+[end_file_name])

                  



        # If a user is picking this file again and its done
    elif 'done' in origin_file_path_fl[-1]:

            # If it is the same user
            if (user in origin_file_path_fl[-1]):
                # dont change filepath done - keep it as it is
                filepath_done = origin_file_path

                # Rename the file 
                in_prog_name=origin_file_path_fl[-1][:-5]+'_inprogress'
                in_prog_path="/".join(origin_file_path_fl[:-1]+ [in_prog_name])


            else:
                # If it is a different user
                # Rename the file to include the additional user
                in_prog_name=origin_file_path_fl[-1][:-5]+f'_{user}'+'_inprogress'
                in_prog_path="/".join(origin_file_path_fl[:-1] +[in_prog_name])
                
                # create the filepath done
                end_file_name=in_prog_path[:-11]+'_DONE' 
                filepath_done="/".join(origin_file_path_fl[:-1] +[end_file_name ])

                
    else:
              in_prog_name=origin_file_path_fl[-1]+f'_{user}'+'_inprogress'
              in_prog_path="/".join(origin_file_path_fl[:-1]+[in_prog_name])
              end_file_name=origin_file_path_fl[-1]+f'_{user}'+'_done' 
              filepath_done="/".join(origin_file_path_fl[:-1]+ [end_file_name])
              
    return in_prog_path, filepath_done
  
  
  
  
def rename_hadoop(old, new):
  
    process = subprocess.Popen(["hadoop", "fs","-mv",old, new])

    process.communicate()
  
  
def get_hadoop(hdfs_path,local_path ):
  
    process = subprocess.Popen(["hadoop", "fs","-get",hdfs_path,local_path])

    process.communicate()
    
def save_hadoop(local_path,hdfs_path):
  
    process = subprocess.Popen(["hadoop", "fs","-put",local_path,hdfs_path ])

    process.communicate()
    

    
    
    
def remove_hadoop(hdfs_path):

    file_test = subprocess.run(f"hdfs dfs -test -f {hdfs_path}", shell=True, stdout=subprocess.PIPE)
    dir_test = subprocess.run(f"hdfs dfs -test -d {hdfs_path}", shell=True, stdout=subprocess.PIPE)
    if file_test.returncode==0: 
        
        command='-rm'
        print('file')
        process = subprocess.Popen(["hadoop", "fs",command,hdfs_path ])
    
    elif dir_test.returncode==0: 
        command='-rmr'
        print('directory')
        process = subprocess.Popen(["hadoop", "fs",command,hdfs_path ])
        
    else:
        print('some_error')
        #build in some error handling 
    

        


    process.communicate()
    
def validate_input_data(filepath):
    if os.path.getsize(filepath) > 536871:
        raise ('Filesize error; file is bigger than 0.5GB')
        
        

    

    
        
