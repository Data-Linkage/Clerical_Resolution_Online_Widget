import flask
from flask import Flask, render_template, request, session
from pyspark.sql.functions import when, col
import os
import configparser
from configparser import ExtendedInterpolation
from pyspark.sql import SparkSession
import subprocess
import logging
import pandas as pd



app = Flask(__name__, instance_relative_config= True,
            static_folder='path_to_static_folder',template_folder='templates',root_path = 'ida_lu/gui')
app.secret_key = 'ida'
logging.getLogger('werkzeug').disabled=True
spark = (SparkSession.builder.appName('flask_app')                              
         .getOrCreate())   

@app.route('/', methods=['GET','POST'])
def pipeline_button():
    list_of_filenames = []
    datasets_location = ''
    process = subprocess.Popen(["hadoop", "fs","-ls","-C",datasets_location],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    std_out, std_error = process.communicate()  
    std_out = str(std_out).split("\\n")[:-1]
    for i in std_out:
        file_name = str(i).split('/')[-1]
        list_of_filenames.append(file_name)


  
    return render_template('pipeline_page.html',list_of_filenames = list_of_filenames)  
  
@app.route('/adapt_columns', methods=['GET','POST'])
def columns():
    button = request.form.get("form")
    list_of_files = []
    #sort file out
    folder_location = f'path_including_{button}_input'
    process = subprocess.Popen(["hadoop", "fs","-ls","-C",folder_location],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    std_out, std_error = process.communicate()  
    std_out = str(std_out).split("\\n")[:-1]
    for i in std_out:
        file_name = str(i).split('/')[-1]
        list_of_files.append(file_name)
    for i in list_of_files:
        if 'csv' in i:
           file = i
           

    
    datasets_location = f'path_including_{button}_and_{file}_inputs'
       
    version_control = spark.read.format("csv").option("header","true").load(datasets_location)

    version_control_cols = []


    columns =version_control.collect()
    for i in columns:
        f = i[0:][:]
        version_control_cols.append(f)  
        
    session['my_var'] = datasets_location
    session['my_var2'] = version_control_cols
    
    dap = request.form.getlist('dap')
    ida = request.form.getlist('ids')
    
    
    
    
    return render_template('column_choice.html',version_control_cols=version_control_cols)
    
#,version_control,datasets_location    
    
# the below isn't an internal CDSW path, it routes to flask templates
@app.route('/adapt_columns/change_csv', methods=['GET','POST'])
def change_csv():

#    version_control = spark.read.format("csv").load(datasets_location)
#    version_control_cols = version_control.columns
    dap = request.form.getlist('dap')
    ids = request.form.getlist('ids')
    
    datasets_location = session.get('my_var',None)
    version_control_cols = session.get('my_var2',None)
    version_control = spark.read.format("csv").option("header","true").load(datasets_location)
    
    for i in ids:
        version_control = version_control.withColumn('IDS', when(version_control.variable == i,'TRUE').otherwise(version_control['IDS']))
   
    version_control = version_control.withColumn('IDS', when(~version_control.variable.isin(ids) ,'FALSE').otherwise(version_control['IDS']))
       

    for i in dap:
       version_control = version_control.withColumn('DAP', when(col('variable') == i,'TRUE').otherwise(version_control['IDS']))
        
    version_control = version_control.withColumn('DAP', when(~version_control.variable.isin(dap) ,'FALSE').otherwise(version_control['DAP']))    
        
    version_control = version_control.toPandas()     
    version_control.to_csv('/tmp/version_file2.csv', header = True, index = False)
        
        
    commands = ["hadoop", "fs", "-moveFromLocal", "-f", '/tmp/version_file2.csv', datasets_location]
    
    process = subprocess.Popen(commands, 
                               stdout=subprocess.PIPE, 
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()    
    
    
    return render_template('success.html')
  

if __name__=="__main__":
    app.run(host=os.getenv('CDSW_IP_ADDRESS'),port= int(os.getenv('CDSW_PUBLIC_PORT')))
     