#note file relates to templates 2 folder in git 

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



#orking_file=file.toPandas()

#this line not working
#working_file = working_file.sort_values(by = clust_id).reset_index().astype(str)

#fill nulls in match column with '[]'.This is just to enable the advance cluster function, as is to work
#could probably be improved so we don't have to do this.
#renaming. 
"""
if 'Match' not in working_file.columns: 
  working_file['Match']='[]'
  print('hi')
  
if 'Sequential_Cluster_Id' not in working_file.columns: 
    working_file['Sequential_Cluster_Id'] = pd.factorize(working_file[clust_id])[0]
working_file=working_file.sort_values('Sequential_Cluster_Id')


origin_file_path=config['filepath']['file']
origin_file_path_fl=config['filepath']['file'].split('.')
"""

 
      


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
    return render_template("ira_html.html")

@app.route('/new_session', methods=['GET','POST'])
def new_session():
   # session['input_df']=data_pd
    process = subprocess.Popen(["hadoop", "fs","-ls","-C",'/ons/crow/hive' ],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
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
    process = subprocess.Popen(["hadoop", "fs","-ls","-C",'/ons/crow/hive' ],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    std_out, std_error = process.communicate() 
    std_out2=list(str(std_out).split('\\n'))
    #set session index here so that it always sets to first unmatched cluster when you first launch
    #but does not reset when you reload /cluster_version
    #session['index']=int(session['working_file']['Sequential_Cluster_Id'][(working_file.Match.values == '[]').argmax()], std_out=std_out2)
    
    return render_template('load_session.html', directory=directory, std_out=std_out2)

@app.route('/cluster_version', methods=['GET','POST'])
def index(): 
      #set highlighter toggle to 0
      if 'file_path' not in session:
          session['file_path']=str(request.form.get("file_path")).split('/')[-1]
          file=spark.sql(f"SELECT * FROM {config['filepath']['hive_directory']}.{session['file_path']}")
          session['working_file']=file.toPandas().to_json()
          local_file=file.toPandas()
          if 'Match' not in local_file.columns: 
              local_file['Match']='[]'
              print('hi')
  
          if 'Sequential_Cluster_Id' not in local_file.columns: 
              local_file['Sequential_Cluster_Id'] = pd.factorize(local_file[clust_id])[0]
              local_file=local_file.sort_values('Sequential_Cluster_Id')

          origin_file_path=f"{config['filepath']['hive_directory']}.{session['file_path']}"
          origin_file_path_fl=f"{config['filepath']['hive_directory']}.{session['file_path']}".split('.') 
          in_prog_path, filepath_done=hf.get_save_paths(origin_file_path,origin_file_path_fl)
          hf.save_rename_hive(local_file, origin_file_path, in_prog_path)
          
      else: 
          local_file=pd.read_json(session['working_file'])
          origin_file_path=f"{config['filepath']['hive_directory']}.{session['file_path']}"
          origin_file_path_fl=f"{config['filepath']['hive_directory']}.{session['file_path']}".split('.') 
          in_prog_path, filepath_done=hf.get_save_paths(origin_file_path,origin_file_path_fl)
    
      if 'index' not in session:
              session['index']=int(local_file['Sequential_Cluster_Id'][(local_file.Match.values == '[]').argmax()])


      else:
              print(f"{session['index']} newsesh")
      
      if {'Match'}.issubset(local_file.columns):
            pass
            # variable indicates whether user has returned to this file (1) or not (0)
        
      else:
            print("that ran3")
            # create a match column and fill with blanks
            #local_file['Match'] = ''
           # pass
      if request.form.get('Match')=="Match":
              cluster = request.form.getlist("cluster")
              for i in cluster:
                  #note resident ID will need to change from to be read from a config as any reccord id 
                  local_file.loc[local_file[rec_id]==i,'Match']=str(cluster)
              hf.advance_cluster(local_file)
            

      elif request.form.get('Non-Match')=="Non-Match":
              #note this section needs building out. 
              #local_file.loc[local_file['Sequential_Cluster_Id']==session['index'],'Match']=0
              cluster = request.form.getlist("cluster")
              for i in cluster:
                  #note resident ID will need to change from to be read from a config as any reccord id 
                  local_file.loc[local_file[rec_id]==i,'Match']=f"['No Match In Cluster For {i}']"
              hf.advance_cluster(local_file)
              
              
              

      if request.form.get('back')=="back":
              session['index'] = session['index']-1

      if request.form.get('save')=="save":
              if hf.check_matching_done(local_file): 
                  hf.save_rename_hive(local_file, in_prog_path, filepath_done)
              else: 
                  hf.save_rename_hive(local_file, in_prog_path, in_prog_path)
              print('save activated ')

      
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
      
      
      if hf.check_matching_done(local_file)==0:
          done_message='Keep Matching'
      else: 
          done_message='Matching Finished. Press Save'

      return  render_template("cluster_version.html",
                              data = data,
                              columns=columns, cluster_number=session['index'],\
                              num_clusters=num_clusters, display_message=display_message, \
                              done_message=done_message, id_col_index=id_col_index)
    
    
    
    
    
    
    
@app.route('/pairwise_version', methods=['GET','POST'])
def index_pairwise(): 
      #set highlighter toggle to 0
      session['highlighter']=False
      if 'index' not in session:
              session['index']=int(local_file['Sequential_Cluster_Id'][0])

      else: 
        pass
        #logic to display firdt unlinked record first. 

      cluster = request.form.getlist("cluster")
      
      # df=local_file.loc[local_file['Sequential_Cluster_Id']==session['index']]
      # session['index'] = int(session['index'])+ 1

      if request.form.get('Match')=="Match":
              local_file.iloc[cluster]['Match']=1
              session['index'] = int(session['index'])+ 1
              hf.check_matching_done(local_file)

      elif request.form.get('Non-Match')=="Non-Match":
              local_file.loc[local_file['Sequential_Cluster_Id']==session['index'],'Match']=0
              session['index'] = int(session['index'])+ 1

      if request.form.get('back')=="back":
              print('back')
              session['index'] = int(session['index'])-1

      if request.form.get('save')=="save":
              save_output()






      df=local_file.loc[df['Sequential_Cluster_Id']==session['index']]
      print(type(df))
      index = (list(range(max(df.count()))))
      df.insert(0,'index',index)

      
      columns = df.columns
      data = zip(df.values, index)
      session['index'] = int(session['index'])+ 1
      

       
      return  render_template("cluster_version.html",
                              data = data,
                              columns=columns)

@app.route('/about_page', methods=['GET','POST'])
def about():
    return render_template("about_page.html")

#########################
#########################


app.config["TEMPLATES_AUTO_RELOAD"] = True
app.run(host=os.getenv('CDSW_IP_ADDRESS'),port= int(os.getenv('CDSW_PUBLIC_PORT')))