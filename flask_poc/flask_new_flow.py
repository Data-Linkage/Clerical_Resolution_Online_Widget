
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
import shutil



spark=sessions.getOrCreateSparkSession(appName='crow_test', size='medium')
config = configparser.ConfigParser()
config.read('config_flow.ini')
rec_id=config['id_variables']['record_id']
clust_id=config['id_variables']['cluster_id']
#ile=spark.sql(f"SELECT * FROM {config['filepath']['file']}")
user = os.environ['HADOOP_USER_NAME']



#this line not working
#working_file = working_file.sort_values(by = clust_id).reset_index().astype(str)

#fill nulls in match column with '[]'.This is just to enable the advance cluster function, as is to work
#could probably be improved so we don't have to do this.
#renaming. 

 
      


#######################
app= Flask(__name__)
#may need to be something more secretive/encryptable! 
app.config['SECRET_KEY']='abcd'

def show_diff(seqm):
    """Unify operations between two compared strings
seqm is a difflib.SequenceMatcher instance whose a & b are strings"""
    output= []
    for opcode, a0, a1, b0, b1 in seqm.get_opcodes():
        if opcode == 'equal':
            output.append(seqm.a[a0:a1])
        elif opcode == 'insert':
            output.append(seqm.a[a0:a1])
            #output.append("<mark>" + f"{seqm.b[b0:b1]}" + "</mark>")
        elif opcode == 'delete':
            output.append("<mark>" + f"{seqm.a[a0:a1]}" + "</mark>")
        elif opcode == 'replace':
            output.append("<mark>" + f"{seqm.a[a0:a1]}" + "</mark>")
        else:
            raise RuntimeError("unexpected opcode")
    return ''.join(output)
  
  

@app.route('/', methods=['GET','POST'])
def welcome_page():
    session.clear()
    return render_template("ira_html.html")

@app.route('/new_session', methods=['GET','POST'])
def new_session():
    session.clear()
    
   # session['input_df']=data_pd
    print(config['hdfs_file_space']['hdfs_folder'])
    process = subprocess.Popen(["hadoop", "fs","-ls","-C", config['hdfs_file_space']['hdfs_folder'] ],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    std_out, std_error = process.communicate() 
    std_out2=list(str(std_out).split('\\n'))
    button = request.form.get("hdfs")
    config_status = request.form.get("config")
    version = request.form.get("version")
    

    return render_template("new_session.html", button=button,
                                              version=version,
                                              config_status=config_status, std_out=std_out2)

@app.route('/load_session', methods=['GET','POST'])
def load_session():
    directory = os.listdir('saved_sessions')
     
    #this lists the files in a given location
    process = subprocess.Popen(["hadoop", "fs","-ls","-C",config['hdfs_file_space']['hdfs_folder']],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    std_out, std_error = process.communicate() 
    std_out2=list(str(std_out).split('\\n'))
    return render_template('load_session.html', directory=directory, std_out=std_out2)

@app.route('/cluster_version', methods=['GET','POST'])
def index(): 
  
      if 'full_path' not in session:
      #all the actions that need to happen if a path not in session 
          print('filepath_no_in_session')
          
          session['full_path']=str(request.form.get("file_path"))
          #this is the full original filepath
          session['filename']=session['full_path'].split('/')[-1]
          #this is the current file name
          temp_local_path=f"/home/cdsw/Clerical_Resolution_Online_Widget/tmp/{session['filename']}"

          hf.get_hadoop(session['full_path'],temp_local_path)
          local_file=pd.read_parquet(temp_local_path)
          if os.path.isdir(temp_local_path):
              shutil.rmtree(temp_local_path)
          local_file.to_parquet(temp_local_path)
          session['working_file']=local_file.to_json()
          if 'Match' not in local_file.columns: 
              local_file['Match']='[]'
  
          if 'Sequential_Cluster_Id' not in local_file.columns: 
              local_file['Sequential_Cluster_Id'] = pd.factorize(local_file[clust_id])[0]
              local_file=local_file.sort_values('Sequential_Cluster_Id')

          #get the local filepath in_prog and done paths 
          local_in_prog_path, local_filepath_done=hf.get_save_paths(temp_local_path,temp_local_path.split('/'))
          
          
          os.rename(temp_local_path, local_in_prog_path)
          
          #get the hdfs filepath in_prog and done paths 
          hdfs_in_prog_path, hdfs_filepath_done=hf.get_save_paths(session['full_path'],session['full_path'].split('/'))
          hf.remove_hadoop(session['full_path'])
          hf.save_hadoop(local_in_prog_path, hdfs_in_prog_path)
          
          
      else: 
          local_file=pd.read_json(session['working_file']).sort_values('Sequential_Cluster_Id')
          temp_local_path=f"/home/cdsw/Clerical_Resolution_Online_Widget/tmp/{session['filename']}"
          #get the local filepath in_prog and done paths 
          local_in_prog_path, local_filepath_done=hf.get_save_paths(temp_local_path,temp_local_path.split('/'))
          print(local_in_prog_path, local_filepath_done)
          #get the hdfs filepath in_prog and done paths 
          hdfs_in_prog_path, hdfs_filepath_done=hf.get_save_paths(session['full_path'],session['full_path'].split('/'))
          print(hdfs_in_prog_path,hdfs_filepath_done)

    
      if 'index' not in session:
              session['index']=int(local_file['Sequential_Cluster_Id'][(local_file.Match.values == '[]').argmax()])
              
      
      if request.form.get('Match')=="Match":
              cluster = request.form.getlist("cluster")
              for i in cluster:
                  #note resident ID will need to change from to be read from a config as any reccord id 
                  local_file.loc[local_file[rec_id]==i,'Match']=str(cluster)
              if local_file.Sequential_Cluster_Id.nunique()>int(session['index'])+1:
                  hf.advance_cluster(local_file)
              
            

      elif request.form.get('Non-Match')=="Non-Match":
              #note this section needs building out. 
              #local_file.loc[local_file['Sequential_Cluster_Id']==session['index'],'Match']=0
              cluster = request.form.getlist("cluster")
              for i in cluster:
                  #note resident ID will need to change from to be read from a config as any reccord id 
                  local_file.loc[local_file[rec_id]==i,'Match']=f"['No Match In Cluster For {i}']"
              if local_file.Sequential_Cluster_Id.nunique()>int(session['index'])+1:
                  hf.advance_cluster(local_file)
              
      if request.form.get('Clear-Cluster')=="Clear-Cluster":
            cluster_ids=list(local_file.loc[local_file['Sequential_Cluster_Id']==session['index']][rec_id].values)
            print(f'cluster ids= {cluster_ids}, type= {type(cluster_ids)}')
            for  i in cluster_ids:
                local_file.loc[local_file[rec_id]==i,'Match']='[]'
         
              

      if request.form.get('back')=="back":
              if int(session['index'])>0:
                  session['index'] = session['index']-1

      if request.form.get('save')=="save":
              hf.remove_hadoop(hdfs_in_prog_path)
              os.remove(local_in_prog_path)
              if hf.check_matching_done(local_file):
                  local_file.to_parquet(local_filepath_done)
                  hf.save_hadoop(local_filepath_done,hdfs_filepath_done)
                  
              else:
                  local_file.to_parquet(local_in_prog_path)
                  hf.save_hadoop(local_in_prog_path,hdfs_in_prog_path)
             

      
      if 'index' not in local_file.columns:
          index = (list(range(max(local_file.count()))))
          local_file.insert(0,'index',index)
      else:
          pass
      #not WORKIng 
      df=local_file.loc[local_file['Sequential_Cluster_Id']==session['index']]
      
      df_display=df[[config['display_columns'][i] for i in config['display_columns']]+["Match"]]
      columns = df_display.columns
      data = df_display.values
      num_clusters=str(local_file.Sequential_Cluster_Id.nunique())
      display_message=config['message_for_matchers']['message_to_display']
      id_col_index=df_display.columns.get_loc(rec_id)
      
      session['working_file']=local_file.to_json()
      
      print(local_file.Sequential_Cluster_Id.nunique())
      if local_file.Sequential_Cluster_Id.nunique()>int(session['index']):
          done_message='Keep Matching'
      elif local_file.Sequential_Cluster_Id.nunique()==int(session['index']):
          done_message='Matching Finished. Press Save'
          

      return  render_template("cluster_version.html",
                              data = data,
                              columns=columns, cluster_number=str(int(session['index']+1)),\
                              num_clusters=num_clusters, display_message=display_message, \
                              done_message=done_message, id_col_index=id_col_index)
    
    
    
    
  

@app.route('/about_page', methods=['GET','POST'])
def about():
    return render_template("about_page.html")
  



#########################
#########################


app.config["TEMPLATES_AUTO_RELOAD"] = True
app.run(host=os.getenv('CDSW_IP_ADDRESS'),port= int(os.getenv('CDSW_PUBLIC_PORT')))

#note to self; some of the pages are still kids bugy. Fix the numbers and counts
#troubeshoot real wierd issue where lots of records displaying