#note file relates to templates 2 folder in git 

import pandas as pd
import logging
from flask import Flask, render_template, request, redirect, \
url_for, flash, make_response, session, jsonify
import os
import ast
import numpy as np
from dlh_utils import sessions
from dlh_utils import utilities

os.chdir('/home/cdsw/Clerical_Resolution_Online_Widget/flask_poc')
app=Flask(__name__)
logging.getLogger('werkzeug').disabled=True

spark=sessions.getOrCreateSparkSession(appName='crow_test', size='medium')

file=spark.sql('SELECT * FROM crow.test3')


#extract list of cluster ids and separate out into dataframes

working_file=file.toPandas()
#working_file = working_file.sort_values(by = 'Cluster_Number').reset_index().astype(str)

#temp thing
#fill nulls in match column with '[]'.This is just to enable the advance cluster function, as is to work
#could probably be improved so we don't have to do this. 
working_file['Match'].replace('nan', '[]',inplace=True)
working_file
#####working poc#######
#######################

##################
##HELPER FUNCTIONS#####
#Note will probably eventually move these into a separate script

def advance_cluster():
  #note this function is very clunky and could likely be improved. 
  """
  1)A Function to: determine the number of matches made in a given cluster
  2)If 1 or 0 unmatcher records in cluster remain; progress to the next cluster. 
  
  Parameters: None
  Returns: None
  
  """
  num_in_cluster=len(working_file.loc[working_file['Cluster_Number']==str(session['index'])])
  print(num_in_cluster)
  list_decided=[set(ast.literal_eval(i)) for i in working_file.loc[working_file['Cluster_Number']==str(session['index'])]['Match']]
  print(list_decided)
  uni_set_decided={x for l in list_decided for x in l}
  print(uni_set_decided)
  num_decided=len(uni_set_decided)
  if (num_in_cluster-num_decided)<=1:
      for rec_id in [x for x in working_file.loc[working_file['Cluster_Number']==str(session['index'])]['Resident_ID'] if x not in uni_set_decided]: 
            working_file.loc[working_file['Resident_ID']==rec_id,'Match']=f"['No Match In Cluster For {rec_id}']"
      session['index'] = int(session['index'])+ 1
    

#######################
app= Flask(__name__)
#may need to be something more secretive/encryptable! 
app.config['SECRET_KEY']='abcd'


@app.route('/', methods=['GET','POST'])
def welcome_page():
    return render_template("welcome_page.html")

@app.route('/new_session', methods=['GET','POST'])
def new_session():
   # session['input_df']=data_pd
    button = request.form.get("hdfs")
    config_status = request.form.get("config")
    version = request.form.get("version")

    return render_template("new_session.html", button=button,
                                              version=version,
                                              config_status=config_status)

@app.route('/load_session', methods=['GET','POST'])
def load_session():
    directory = os.listdir('saved_sessions')
    
    return render_template('load_session.html', directory=directory)

@app.route('/cluster_version', methods=['GET','POST'])
def index(): 
      #set highlighter toggle to 0
      session['highlighter']=False
      if 'index' not in session:
              session['index']=int(working_file['Cluster_Number'][0])

      else: 
        pass
        #logic to display firdt unlinked record first. 
      
      if {'Match'}.issubset(working_file.columns):
        print('Matching_Began')
            # variable indicates whether user has returned to this file (1) or not (0)
        
      else:
            
            # create a match column and fill with blanks
          working_file['Match'] = ''
       
      #cluster = request.form.getlist("cluster")
      #print(cluster)
      
      # df=working_file.loc[working_file['Cluster_Number']==session['index']]
      # session['index'] = int(session['index'])+ 1
      #cur_indexes=list(working_file.loc[working_file['Cluster_Number']==session['index']].index.values)
      if request.form.get('Match')=="Match":
              cluster = request.form.getlist("cluster")
              for i in cluster:
                  #note resident ID will need to change from to be read from a config as any reccord id 
                  working_file.loc[working_file['Resident_ID']==i,'Match']=str(cluster)
              advance_cluster()
            

      elif request.form.get('Non-Match')=="Non-Match":
              #note this section needs building out. 
              #working_file.loc[working_file['Cluster_Number']==session['index'],'Match']=0
              cluster = request.form.getlist("cluster")
              for i in cluster:
                  #note resident ID will need to change from to be read from a config as any reccord id 
                  working_file.loc[working_file['Resident_ID']==i,'Match']=f"['No Match In Cluster For {i}']"
              advance_cluster()
              
              
              

      if request.form.get('back')=="back":
              session['index'] = int(session['index'])-1

      if request.form.get('save')=="save":
            table=utilities.pandas_to_spark(working_file)
            utilities.write_format(table,'hive' ,'crow', 'test3')
      
      if 'index' not in working_file.columns:
          index = (list(range(max(working_file.count()))))
          working_file.insert(0,'index',index)
      else:
          pass

      df=working_file.loc[working_file['Cluster_Number']==str(session['index'])]

      
      columns = df.columns
      data = df.values

      return  render_template("cluster_version.html",
                              data = data,
                              columns=columns)

    
    
    
    
    
    
    
@app.route('/pairwise_version', methods=['GET','POST'])
def index_pairwise(): 
      #set highlighter toggle to 0
      session['highlighter']=False
      if 'index' not in session:
              session['index']=int(working_file['Cluster_Number'][0])

      else: 
        pass
        #logic to display firdt unlinked record first. 

      cluster = request.form.getlist("cluster")
      
      # df=working_file.loc[working_file['Cluster_Number']==session['index']]
      # session['index'] = int(session['index'])+ 1

      if request.form.get('Match')=="Match":
              working_file.iloc[cluster]['Match']=1
              session['index'] = int(session['index'])+ 1

      elif request.form.get('Non-Match')=="Non-Match":
              working_file.loc[working_file['Cluster_Number']==session['index'],'Match']=0
              session['index'] = int(session['index'])+ 1

      if request.form.get('back')=="back":
              
              session['index'] = int(session['index'])-1

      if request.form.get('save')=="save":
            table=utilities.pandas_to_spark(working_file)
            print('save activated ')
            utilities.write_format(table,'hive' ,'crow', 'test3')





      df=working_file.loc[working_file['Cluster_Number']==session['index']]
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