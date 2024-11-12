
"""
This is the main script to run the application.

"""
import os
os.chdir('/home/cdsw/Clerical_Resolution_Online_Widget/version2_flask')
from multiprocessing import Process
from datetime import datetime
import shutil
import logging
import subprocess
import re
import configparser
from flask import Flask, render_template, request, session
from flask_session import Session
import pandas as pd
import helper_functions as hf

start_time=datetime.now()

config = configparser.ConfigParser()
config.read('config_flow.ini')
rec_id=config['id_variables']['record_id']
clust_id=config['id_variables']['cluster_id']
user = os.environ['HADOOP_USER_NAME']

#Setting up some files if not already done.
#creating a tmp file 
temp_folder=f"{config['filespaces']['local_space']}"
  
if not os.path.exists(temp_folder):
    os.mkdir(temp_folder)

#creating a user specific temp file within tmp. 
user_temp_folder  = f"{config['filespaces']['local_space']}{user}"

if not os.path.exists(user_temp_folder):
    os.mkdir(user_temp_folder)

#clear temp/user folder
for filename in os.listdir(user_temp_folder):
    file_path = os.path.join(user_temp_folder, filename)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)
    except FileNotFoundError:
        print(f'temp folder does not exist or is emply')
######################

app=Flask(__name__)
#generate a random string for key.
key2=str(os.urandom(24))
app.config['SECRET_KEY']=key2
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_FILE_DIR"] = f"{config['filespaces']['local_space']}{user}"
logging.getLogger('werkzeug').disabled=True

Session(app)

@app.route('/', methods=['GET','POST'])
def welcome_page():
    """
    This page acts as a menu for the user
    """
    session['font_choice'] = f"font-family:{request.form.get('font_choice')}"
    session_keys=list(session)
    for i in session_keys:
        if i!= 'font_choice':
            session.pop(i)
    return render_template("welcome_page.html", font_choice = session['font_choice'])
@app.route('/new_session', methods=['GET','POST'])
def new_session():
    """
    Allows the user to select their data, and launch the session
    """
    #code to remove session variables except for font choice
    #this is to ensure if the page is returned to in the same session- variables are cleared
    #to avoid conflicts/saving over wrong files.
    session_keys=list(session)
    for i in session_keys:
        if i!= 'font_choice':
            session.pop(i)
    
    #using hadoop commands- get list of files in folder from hdfs
    process = subprocess.Popen(["hadoop", "fs","-ls","-C",\
                                config['filespaces']['hdfs_folder'] ],\
                                stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    std_out, std_error = process.communicate()
    std_out2=list(str(std_out).split('\\n'))
    #fix for error where first file in the folder has b' due to hadoop commands
    std_out2=[re.sub(r"^b'","",i) for i in std_out2]
    button = request.form.get("hdfs")
    config_status = request.form.get("config")
    version = request.form.get("version")
    return render_template("new_session.html", button=button,
                                              version=version,
                                              config_status=config_status, std_out=std_out2,
                                              font_choice = session['font_choice'])
@app.route('/cluster_version', methods=['GET','POST'])
def index():
    """
    This is the main page where clerical happens!
    """
    #When cluster version button pressed.
    #Clear session variables except for font choice
    if request.form.get('version')=="Cluster Version":
        hf.clear_session()
    
    #Set filepaths and read in pd dataframe
    
    #if file not opened in session before
    if 'full_path' not in session:
        local_file,local_in_prog_path, local_filepath_done,\
        hdfs_in_prog_path, hdfs_filepath_done=hf.new_file_actions()
        #start the save thread to move in progress file back to hdfs.
        s_thread=Process(target=save_thread, args= (local_in_prog_path,\
                                              hdfs_in_prog_path,\
                                              local_file, local_filepath_done, hdfs_filepath_done))
        s_thread.start()
    
    #if file already opened in session
    else:
        local_file,local_in_prog_path, local_filepath_done, \
        hdfs_in_prog_path, hdfs_filepath_done=hf.reload_page()

    ############ session variables and toggles#############

    hf.set_session_variables(local_file)
    
    ###if matching done. 
    if hf.check_matching_done(local_file):
        local_file.to_parquet(local_filepath_done)
    else:
        local_file.to_parquet(local_in_prog_path)

    
      ##############################Button Code###############################
      ##Code to control the actions on each button press.
      #if match button pressed; add the record Id's of the
      #selected records to the match column as an embedded list
    
    match_error=''
    if request.form.get('Match')=="Match":
        match_error=hf.make_match(local_file,match_error)
        #save if at a backup_save checkpoint.
        hf.backup_save(save_thread,local_in_prog_path,hdfs_in_prog_path,\
                                              local_file, local_filepath_done,\
                                              hdfs_filepath_done)


    elif request.form.get('Non-Match')=="Non-Match":
        hf.make_non_match(local_file)
        #save if at a backup_save checkpoint.
        hf.backup_save(save_thread,local_in_prog_path,hdfs_in_prog_path,\
                                              local_file, local_filepath_done,\
                                              hdfs_filepath_done)

 

    #if Clear-Cluster pressed; replace the match column for cluster with '[]'
    if request.form.get('Clear-Cluster')=="Clear-Cluster":
        hf.clear_cluster(local_file)


    #if back button pressed; set session['index'] back to move to previous cluster (Unless index=0)
    if request.form.get('back')=="back":
        if int(session['index'])>0:
            session['index'] = session['index']-1

    #if save pressed...save file to hdfs    
    if request.form.get('save')=="save":
        s_thread=Process(target=save_thread, args= (local_in_prog_path,hdfs_in_prog_path,\
                                                    local_file, local_filepath_done, hdfs_filepath_done))
        s_thread.start()

    
    #set select select all and highlighter toggles
    hf.reset_toggles()

      
        
      ####################Things to display code#########################
      
    #extract a df dor the current cluster
    data_f=local_file.loc[local_file['Sequential_Cluster_Id']==session['index']]

    #select columns; split into column headers and data
    #possible copy set warning place
    display_cols_list=[config['display_columns'][i] for i in config['display_columns']]+["Match","Comment"]
    df_display=data_f[display_cols_list].copy()
    #extract a list of columns that are highlighted.
    #this is so that match, comment ect columns are not impacted by highlighter.
    highlight_cols=[config['display_columns'][i] for i in config['display_columns']]
    df_display[highlight_cols] = df_display[highlight_cols].astype(str)
    highlight_cols.remove(rec_id)

      
      ################HIGHLIGHTER###############
   
                  
    hf.highlighter_func(highlight_cols, df_display)             
    columns = df_display.columns
    data = df_display.values

      #############OTHER THINGS TO DISPLAY#######
      

    #get number of clusters and message to display.
    num_clusters=str(local_file.Sequential_Cluster_Id.nunique())
    display_message=config['message_for_matchers']['message_to_display']
    id_col_index=df_display.columns.get_loc(rec_id)
    #cast local_file back to json
    session['working_file']=local_file.to_json()
    match_col_index=df_display.columns.get_loc('Match')
        
    #check if cluster done
    cur_cluster_done= hf.check_cluster_done(local_file)
    
    #set continuation message
    done_message=hf.set_continuation_message(local_file, cur_cluster_done)
    
    #some variables for html
    button_left, button_right = hf.set_position_vars(columns)

    return  render_template("cluster_version.html",
                            data = data,
                            columns=columns, cluster_number=str(int(session['index']+1)),\
                            button_left = button_left, button_right = button_right,\
                            num_clusters=num_clusters, display_message=display_message,\
                            done_message=done_message, id_col_index=id_col_index,\
                            select_all=session['select_all'],\
                            highlight_differences=session['highlight_differences'],\
                            font_choice = session['font_choice'],\
                            match_error=match_error, match_col_index=match_col_index)
@app.route('/about_page', methods=['GET','POST'])
def about():
    """
    This page gives info and guidance about the app
    """
    return render_template("about_page.html")
########################

########################

if __name__=='__main__':
  
    def save_thread(local_in_prog_path,hdfs_in_prog_path,\
                  local_file, local_filepath_done, hdfs_filepath_done):
        """
        A fumctiom to save to hdfs
        """
        print('save initiated')
        hf.remove_hadoop(session['full_path'])
        hf.remove_hadoop(hdfs_in_prog_path)
        hf.remove_hadoop(hdfs_filepath_done)
        if os.path.exists(local_in_prog_path):
            os.remove(local_in_prog_path)
        else:
            pass

        if os.path.exists(local_filepath_done): 
            os.remove(local_filepath_done)
        else:
            pass



        if hf.check_matching_done(local_file):
            local_file.to_parquet(local_filepath_done)
            hf.save_hadoop(local_filepath_done,hdfs_filepath_done)

        else:
            local_file.to_parquet(local_in_prog_path)
            hf.save_hadoop(local_in_prog_path,hdfs_in_prog_path)
        print('Saving Complete')    

    
    
             
    def run_app():
        """
        A function to run the main app
        """
        print('App is running, press icon in top corner to launch application.\n\
              It may take a few secounds for the "CROW clerical tool" icon to appear.\n\
              if you cannot see it, look again in a few secounds')

        app.config["TEMPLATES_AUTO_RELOAD"] = True
        app.run(host="127.0.0.1",port=int(os.environ['CDSW_APP_PORT'])) 
        

    ra=Process(target=run_app)
    ra.start()
    
    #run a timer in the main terminal
    #this is run in main occupying the kernel; but timesout after a set time
    
    nowtime=datetime.now()
    n=(nowtime-start_time).total_seconds()
    while n < 3600:
        nowtime=datetime.now()
        n=(nowtime-start_time).total_seconds()
    
    ra.terminate()
    
    #clear the users temp folder.
    for filename in os.listdir(user_temp_folder):
        file_path = os.path.join(user_temp_folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except FileNotFoundError:
            print(f'temp folder does not exist or is emply')
    
    print('Session has timed out. Please re-start your session \n and re-run the script to continue')