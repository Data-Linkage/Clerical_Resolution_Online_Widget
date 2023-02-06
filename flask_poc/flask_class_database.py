import pandas as pd
import logging
from flask import Flask, render_template, request, redirect, \
url_for, flash, make_response, session, jsonify
import os
from flask_sqlalchemy import SQLAlchemy
import sqlite3
from sqlite3 import Error

#
os.chdir('/home/cdsw/Clerical_Resolution_Online_Widget/flask_poc')

#create database

dataframe=pd.read_csv('clusters_example_data.csv')
dataframe['Match']=''
#get column titles as separated
col_list=list(dataframe.columns.values).remove('Resident_ID')
cols=', '.join(['', list(dataframe.columns.values).remove('Resident_ID')]) + ',Resident_ID PRIMARY_KEY, Match'
#string df.to_sql('df', conn, if_exists='append', index = False)
#cols= 'Resident_ID PRIMARY_KEY, Match, First_Name, Last_Name, Day_Of_Birth, Sex, Address'
#data_pd=pd.read_csv('clusters_example_data.csv')
#data_pd
app=Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI']='sqlite:///database.db'
db=SQLAlchemy(app)

class ClericalSample():
  def __init__(self): 
    self.con=sqlite3.connect('/home/cdsw/Clerical_Resolution_Online_Widget/flask_poc/database.db')
    self.cur=self.con.cursor()
    self.create_table()
  
  def create_table(self): 
    self.cur.execute(f"""CREATE TABLE IF NOT EXISTS sample ({cols})""")
    dataframe.to_sql('database',self.con,if_exists='append',index=False)

    #self.con.execute(f"""ALTER TABLE database match VARCHAR""")
  def insert(self, item):
    self.cur.execute(""" INSERT OR IGNORE INTO sample VALUES(?,?)""", item)
    self.con.commit()
    
  def read(self):
    self.cur.execute("""SELECT * FROM sample""")
    rows=self.cur.fetchall()
    return rows
  
  
db=ClericalSample()
#db.insert(('id2','fghg'))

#logging.getLogger('werkzeug').disabled=True

working_file=pd.read_csv('/home/cdsw/clusters_example_data.csv')
app.config['SECRET_KEY']='abcd'
@app.route('/', methods=['GET','POST'])
def intro():
    # session['input_df']=data_pd
    return render_template("intro_page.html")

@app.route('/cluster2', methods=['GET','POST'])
def index(): 
    database=ClericalSample()
    if 'index' not in session:
            session['index'] = 201 # setting session data
    df=working_file.loc[working_file['Cluster_Number']]==session['index']
  
    if request.form.get('Match')=="Match":
            database.insert((session['index'],'1'))
            session['index'] = int(session['index'])+ 1
    elif request.form.get('Non-Match')=="Non-Match":
            database.insert((session['index'],'0'))
            session['index'] = int(session['index'])+ 1
   
    return  render_template("cluster_version.html",tables=[df.to_html(classes='data')], titles=df.columns.values) 
    
        
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.run(host=os.getenv('CDSW_IP_ADDRESS'),port= int(os.getenv('CDSW_PUBLIC_PORT')))

  
  
database2=ClericalSample()
database2.read()

#con=sqlite3.connect('/home/cdsw/FLOW_real/test.db')
#dataframe.to_sql('test',con,if_exists='append',index=False)


  
@app.route('/cluster_version', methods=["GET","POST"])
def index():
  
    if 'index' not in session:
            session['index'] = '201' # setting session data
    df=working_file.loc[working_file['Cluster_Number']==session['index']]
  
    if request.form.get('Match')=="Match":
        df.loc[df['Cluster_Number'] == session['index'], 'Match'] = 1
        session['index'] = str(int(session['index'])+ 1)
    elif request.form.get('Non-Match')=="Non-Match":
        df.loc[df['Cluster_Number'] == session['index'], 'Match'] = 0
        session['index'] = str(int(session['index'])+ 1)
    df=working_file.loc[working_file['Cluster_Number']==session['index']]
    
    if request.form.get('save')=='save':
        working_file2=working_file.astype(str)
        return working_file2.to_json()
    return  render_template("cluster_version.html",tables=[df.to_html(classes='data')], titles=df.columns.values) 


app.config["TEMPLATES_AUTO_RELOAD"] = True
app.run(host=os.getenv('CDSW_IP_ADDRESS'),port= int(os.getenv('CDSW_PUBLIC_PORT')))




  
"""  
# Create DataFrame
app= Flask(__name__)
app.config['SECRET_KEY']='abcd'
@app.route('/', methods=['GET','POST'])
def intro():
    session['input_df']=request.files['file']
    return render_template("intro_page.html")

@app.route('/cluster_version', methods=["GET","POST"])
def index():
    if 'index' not in session:
        session['index'] = 201 # setting session data
    else:
        if request.form.get('NEXT')=="add one":
            session['index'] = session.get('index') + 1
    data_pd=session['input_df']
    df=data_pd.loc[data_pd['Cluster_Number']==session['index']]
    return render_template("cluster_version.html",tables=[df.to_html(classes='data')], titles=df.columns.values)
  
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.run(host=os.getenv('CDSW_IP_ADDRESS'),port= int(os.getenv('CDSW_PUBLIC_PORT')))