#note file relates to templates 2 folder in git 

import pandas as pd
import logging
from flask import Flask, render_template, request, redirect, \
url_for, flash, make_response, session, jsonify
import os

from dlh_utils import sessions
from dlh_utils import utilities

os.chdir('/home/cdsw/Clerical_Resolution_Online_Widget/flask_poc')
app=Flask(__name__)
logging.getLogger('werkzeug').disabled=True


spark=sessions.getOrCreateSparkSession(appName='crow_test', size='medium')

file=spark.sql('SELECT * FROM crow.test2')
#utilities.write_format(dataframe,'hive' ,'crow', 'test2')

#extract list of cluster ids and separate out into dataframes

working_file=file.toPandas()
#working_file['Match']=''
#####working poc#######
#######################

app= Flask(__name__)
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
#      working_file = request.files['input_file']
#      working_file.save('/tmp/working_file.csv')
#      pd.read_csv('/tmp/working_file.csv')
#      working_file['Match']=''
#      
      
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
              print(cluster)
#              working_file['Match'] = working_file['Match'].iloc[cluster]['Match']=1
#              working_file['Match'] = working_file['Match'].loc[(working_file['Cluster_Number']==session['index']) & (working_file['Match']!= 1)]=0
              working_file.loc[working_file['Cluster_Number']==session['index'],'Match']=1
              
              session['index'] = int(session['index'])+ 1

      elif request.form.get('Non-Match')=="Non-Match":
              working_file.loc[working_file['Cluster_Number']==session['index'],'Match']=0
              session['index'] = int(session['index'])+ 1

      if request.form.get('back')=="back":
              session['index'] = int(session['index'])-1

      if request.form.get('save')=="save":
            table=utilities.pandas_to_spark(working_file)
            utilities.write_format(table,'hive' ,'crow', 'test2')



      
      if 'index' not in working_file.columns:
          index = (list(range(max(working_file.count()))))
          working_file.insert(0,'index',index)
      else:
          pass

      df=working_file.loc[working_file['Cluster_Number']==session['index']]
      df_index = list(df['index'].values)

      
      columns = df.columns
      data = zip(df.values, df_index)
      session['index'] = int(session['index'])+ 1
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
            utilities.write_format(table,'hive' ,'crow', 'test2')





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