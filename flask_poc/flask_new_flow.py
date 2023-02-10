#note file relates to templates 2 folder in git 

import pandas as pd
import logging
from flask import Flask, render_template, request, redirect, \
url_for, flash, make_response, session, jsonify
import os
os.chdir('/home/cdsw/FLOW_real2')

from dlh_utils import sessions
from dlh_utils import utilities

app=Flask(__name__)
logging.getLogger('werkzeug').disabled=True


spark=sessions.getOrCreateSparkSession(appName='crow_test', size='medium')

file=spark.sql('SELECT * FROM crow.test2')
#utilities.write_format(dataframe,'hive' ,'crow', 'test2')

#extract list of cluster ids and separate out into dataframes
working_file=file.toPandas()
working_file['Match']=''
#####working poc#######
#######################

app= Flask(__name__)
app.config['SECRET_KEY']='abcd'
@app.route('/', methods=['GET','POST'])
def intro():
   # session['input_df']=data_pd
    return render_template("intro_page.html")

@app.route('/cluster2', methods=['GET','POST'])
def index(): 
      if 'index' not in session:
              session['index']=int(working_file['Cluster_Number'][0])

      else: 
        pass
        #logic to display firdt unlinked record first. 


      # df=working_file.loc[working_file['Cluster_Number']==session['index']]
      # session['index'] = int(session['index'])+ 1

      if request.form.get('Match')=="Match":
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


      df=working_file.loc[working_file['Cluster_Number']==session['index']]
      # session['index'] = int(session['index'])+ 1

      return  render_template("cluster_version.html",tables=[df.to_html(classes='data')], titles=df.columns.values) 

    
@app.route('/about_page', methods=['GET','POST'])
def about():
    return render_template("about_page.html")

#########################
#########################


app.config["TEMPLATES_AUTO_RELOAD"] = True
app.run(host=os.getenv('CDSW_IP_ADDRESS'),port= int(os.getenv('CDSW_PUBLIC_PORT')))