
import pandas as pd
import logging
from flask import Flask, render_template, request, redirect, \
url_for, flash, make_response, session
import os
os.chdir('/home/cdsw/flask_poc')

data_pd=pd.read_csv('clusters_example_data.csv')
data_pd
app=Flask(__name__)
logging.getLogger('werkzeug').disabled=True

data=pd.read_csv('/home/cdsw/clusters_example_data.csv')

#extract list of cluster ids and separate out into dataframes

#####working poc#######
#######################
#working proof of concept
poc=1
if poc ==1:
    app= Flask(__name__)
    app.config['SECRET_KEY']='abcd'
    @app.route('/', methods=['GET','POST'])
    def intro():
       # session['input_df']=data_pd
        return render_template("intro_page.html")

    @app.route('/cluster_version', methods=["GET","POST"])
    def index():
        if 'index' not in session:
            session['index'] = 201 # setting session data
        else:
            if request.form.get('NEXT')=="add one":
                session['index'] = session['index']+ 1
        #data_pd=session['input_df']
        df=data_pd.loc[data_pd['Cluster_Number']==session['index']]
        return render_template("cluster_version.html",tables=[df.to_html(classes='data')], titles=df.columns.values)

    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.run(host=os.getenv('CDSW_IP_ADDRESS'),port= int(os.getenv('CDSW_PUBLIC_PORT')))


#########################
#########################




  
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
"""
