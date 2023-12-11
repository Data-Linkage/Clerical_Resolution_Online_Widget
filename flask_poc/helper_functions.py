import ast
import os
#os.chdir('/home/cdsw/Clerical_Resolution_Online_Widget/flask_poc')
import subprocess
import configparser
from flask import  session
from multiprocessing import Process
from markupsafe import Markup
from flask import Flask, render_template, request, session
user = os.environ['HADOOP_USER_NAME']
config = configparser.ConfigParser()
config.read('config_flow.ini')
rec_id=config['id_variables']['record_id']
clust_id=config['id_variables']['cluster_id']
#ile=spark.sql(f"SELECT * FROM {config['filepath']['file']}")
user = os.environ['HADOOP_USER_NAME']
import pandas as pd
import shutil




def advance_cluster(dataframe):
  #note this function is very clunky and could likely be improved. 
    """
    1)A Function to: determine the number of matches made in a given cluster
    2)If 1 or 0 unmatcher records in cluster remain; progress to the next cluster. 

    Parameters: None
    Returns: None

    """
    num_in_cluster=len(dataframe.loc[dataframe['Sequential_Cluster_Id']==session['index']])

    list_decided=[set(ast.literal_eval(i)) for i in dataframe.loc[dataframe['Sequential_Cluster_Id']==session['index']]['Match']]

    uni_set_decided={x for l in list_decided for x in l}

    num_decided=len(uni_set_decided)
    if (num_in_cluster-num_decided)<=1:
        for r_id in [x for x in dataframe.loc[dataframe['Sequential_Cluster_Id']==session['index']][rec_id] if x not in uni_set_decided]: 
            dataframe.loc[dataframe[rec_id]==r_id,'Match']=f"['No Match In Cluster For {r_id}']"
        session['index'] = int(session['index'])+ 1
      
def check_matching_done(dataframe):
    """
    A function to check if all the records have a match/non-match status

    Parameters: dataframe
    Returns: Boolean

    """
    if len(dataframe[dataframe.Match == '[]'])>0 : 
        return 0
    if len(dataframe[dataframe.Match == '[]'])==0:
        print('matching_done')
        return 1
  



def get_save_paths(origin_file_path,origin_file_path_fl):
    """
    Takes the input filepath and creates a filepaths for the inprogress
    and done status of that file. 
    Parameters: origin_file_path (string); path of hive location,
    origin_file_path_fl (List); dot separated list. 
    Returns: in_proh_path (String), filepath_done (String)
    
    
    """
    if 'inprogress' in origin_file_path_fl[-1]:

        # If it is the same user
        if user in origin_file_path_fl[-1]:
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
        if user in origin_file_path_fl[-1]:
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
  
  
  
  
  
  
def get_hadoop(hdfs_path,local_path ):
    """
    A function to take a copy a file from hdfs to the local filespace
    
    Parameters: hdfs filepath(string); location of hdfs file
                local_path(string); location of filepath to store data locally
    Returns: None
    
    """
  
    process = subprocess.Popen(["hadoop", "fs","-get",hdfs_path,local_path])

    process.communicate()
    
def save_hadoop(local_path,hdfs_path):
    """
    A function to take a copy a file from local folder to hdfs
    
    Parameters: hdfs filepath(string); 
                local_path(string); location of filepath to store data locally
    Returns: None
    """

  
    process = subprocess.Popen(["hadoop", "fs","-put",local_path,hdfs_path ])

    process.communicate()
    

    
 ####NOTE remove hadoop function is broken!   
def remove_hadoop(hdfs_path):

    file_test = subprocess.run(f"hdfs dfs -test -f {hdfs_path}", shell=True, stdout=subprocess.PIPE, check=False)
    dir_test = subprocess.run(f"hdfs dfs -test -d {hdfs_path}", shell=True, stdout=subprocess.PIPE,check= False)
    if file_test.returncode==0: 
        
        command='-rm'
        print('file')
        process = subprocess.Popen(["hadoop", "fs",command,hdfs_path ])
        process.communicate()
    elif dir_test.returncode==0: 
        command='-rmr'
        print('directory')
        process = subprocess.Popen(["hadoop", "fs",command,hdfs_path ])
        process.communicate()
    else:
        print(f'{hdfs_path} could not be deleted')
        #build in some error handling 
        

def validate_columns(df):
    """
    Checks for a given dataframe, if the record_id and cluster id columns
    are present and that the record id column contains 
    all unique values. Relevant errors are then raised. 
    
    Parameters: df (Pandas Dataframe )
    
    """
    if rec_id not in df.columns:
        raise Exception('no record ID in data; contact your project leader for guidance')
    if not df[rec_id].is_unique: 
        raise Exception('record ids are not unique; contact your project leader for guidance ')
    if clust_id not in df.columns: 
        raise Exception('no cluster id column in data; contact your project leader for guidance')

    
def validate_input_data(filepath):
    """
    Checks that the size of the file is smaller than 0.5GB and raises an error if not
    """
    if os.path.getsize(filepath) > 536871:
        raise Exception('Filesize error; file is bigger than 0.5GB')
        

def check_cluster_done(dataframe): 
    """
    XXX
    
    """
    num_in_cluster=len(dataframe.loc[dataframe['Sequential_Cluster_Id']==session['index']])

    list_decided=[set(ast.literal_eval(i)) for i in dataframe.loc[dataframe['Sequential_Cluster_Id']==session['index']]['Match']]

    uni_set_decided={x for l in list_decided for x in l}

    num_decided=len(uni_set_decided)
    if (num_in_cluster-num_decided)==0: 
        return 1 
    else: 
        return 0
      
def highlighter_func(highlight_cols, df_display): 
    """X"""
    if not session['highlight_differences']==1: 
        print('highlihter not on')
        return
    for column in highlight_cols: 
        for i in df_display.index.values[1:]:
            output=[]
            element=df_display.loc[i,column]
            for count, letter in enumerate(element):
                #check if element has something to compare to
                if not count<= len(df_display.loc[df_display.index.values[0],column])-1:
                    output.append("<mark>"+ letter + "</mark>")
                    break

                if not letter != df_display.loc[df_display.index.values[0],column][count]:
                    output.append(letter)
                else:
                    output.append("<mark>"+ letter + "</mark>")
                data_point = ''.join(output)

            df_display.loc[i,column] = Markup(data_point)

def new_file_actions():
    """
    Actions when page is loaded for the first time with this file
    
    1) get filepaths from app request
    2) set new column variables.
    2) set hdfs and local paths for done and inprogress files. 
    
    Parameters: None
    
    Returns: local_file(pandas dataframe)
             local_in_prog_path (string)
             local_filepath_done (string)
             hdfs_in_prog_path (string)
             hdfs_filepath_done(string)
             
    """
          
    #actions for if this is the initial launch/path is not a session variable
    #get the hdfs file paths and file name
    session['full_path']=str(request.form.get("file_path"))
    session['filename']=session['full_path'].split('/')[-1]

    #get the temporary file location from config
    temp_local_path=f"{config['filespaces']['local_space']+session['filename']}"
    #get the data from hdfs into local location
    get_hadoop(session['full_path'],temp_local_path)

    #load from local location to a pandas df
    local_file=pd.read_parquet(temp_local_path)

    #validate pd columns/raise errors
    validate_columns(local_file)

    #if temp path is now a directory; remove directory
    if os.path.isdir(temp_local_path):
        shutil.rmtree(temp_local_path)
        #this is a hack to resolve; to_parquet only works with single partition files.
    #save back to temp path as a one-partition parquet
    local_file.to_parquet(temp_local_path)

    #create json version of the local file (a flask hack)
    session['working_file']=local_file.to_json()

    #if there are not already; create the following columns: Match, 
    #Comment, Sequential_Cluster_Id, Sequential_Record_Id 
    
    if 'Match' not in local_file.columns:
        local_file['Match']='[]'
        
    if 'Sequential_Cluster_Id' not in local_file.columns:
        local_file['Sequential_Cluster_Id'] = pd.factorize(local_file[clust_id])[0]
        local_file=local_file.sort_values(by=['Sequential_Cluster_Id'])

    if 'Comment' not in local_file.columns:
        local_file['Comment']=''

    if 'Sequential_Record_Id' not in local_file.columns:
        local_file['Sequential_Record_Id'] = pd.factorize(local_file[rec_id])[0]
        local_file=local_file.sort_values(by=['Sequential_Record_Id'])



    #get the local filepath in_prog and done paths rename locally to in_prog_path
    local_in_prog_path, local_filepath_done=get_save_paths(temp_local_path,temp_local_path\
                                                              .split('/'))
    os.rename(temp_local_path, local_in_prog_path)

    #get the hdfs filepath in_prog and done paths and rename in hdfs to in_prog_path
    hdfs_in_prog_path, hdfs_filepath_done=get_save_paths(session['full_path'],\
                                                            session['full_path'].split('/'))
    #return filepaths
    return local_file, local_in_prog_path, local_filepath_done, hdfs_in_prog_path, hdfs_filepath_done
        
def reload_page():
    """
    Actions when page is reloaded
    
    1) loads local file from (temp) json
    2) gets hdfs and local paths for done and inprogress files. 
    
    Parameters: None
    
    Returns: local_file(pandas dataframe)
             local_in_prog_path (string)
             local_filepath_done (string)
             hdfs_in_prog_path (string)
             hdfs_filepath_done(string)
             
    """
    #load in pd dataframe
    local_file=pd.read_json(session['working_file']).sort_values(by=['Sequential_Record_Id'])

    temp_local_path=f"{config['filespaces']['local_space']+session['filename']}"

    #get the local filepath in_prog and done paths rename locally to in_prog_path
    local_in_prog_path, local_filepath_done=get_save_paths(temp_local_path,\
                                                              temp_local_path.split('/'))

    #get the hdfs filepath in_prog and done paths and rename in hdfs to in_prog_path
    hdfs_in_prog_path, hdfs_filepath_done=get_save_paths(session['full_path'],\
                                                            session['full_path'].split('/'))
    #return filepaths
    return local_file,local_in_prog_path, local_filepath_done, hdfs_in_prog_path, hdfs_filepath_done
  
  
def set_session_variables(local_file):
    """
    A function to set the session variables required. 
    Index (main iterable to control flow of the application, based on cluster id)
    Select all toggle (initially set to off)
    Highlighter toggle (initially set to off)
    
    Parameters: None
    
    Returns: None
    """
    if 'index' not in session:
        session['index']=int(local_file['Sequential_Cluster_Id'][(local_file.Match.values == '[]').argmax()])

    #set select all toggle
    if "select_all" not in session: 
        session['select_all']=0

    #highlight differences toggle
    if 'highlight_differences' not in session: 
        session['highlight_differences']=0

def make_match(local_file,match_error): 
    """
    A function to declare a a group of records as a match. This gets selected records, 
    from checkboxes, and appends, 'No match for record ID' to the match column. 
    A comment is also added.
    
    Parameters: local file (dataframe)
                match_error (String)
                
    Returns:    match_error (String)
    
    """
    #get record(s) selected
    cluster = request.form.getlist("cluster")
    for i in cluster:
      
        #if only 1 selected, display on-screen messgae
        if len(cluster)<=1:
            match_error='you have only selected one record'
            
        #if more than 1 selected; perform match and append comment
        elif len(cluster)>=2:
            local_file.loc[local_file[rec_id]==i,'Match']=str(cluster)
            local_file.loc[local_file[rec_id]==i,'Comment']=str(request.form.get("Comment"))
            match_error=''
            
            #move on to next cluster if not at end of file
            if local_file.Sequential_Cluster_Id.nunique()>int(session['index'])+1:
                advance_cluster(local_file)
    return match_error




def make_non_match(local_file):
    """
    A function to declare a record as non-match. This gets selected records, 
    from checkboxes, and appends, 'No match for record ID' to the match column. 
    A comment is also added. 
    
    Parameters: None
    
    Returns: None
    """
    #get records selected
    cluster = request.form.getlist("cluster")
    
    #add result to match column
    for i in cluster:
        local_file.loc[local_file[rec_id]==i,'Match']=f"['No Match In Cluster For {i}']"
        local_file.loc[local_file[rec_id]==i,'Comment']=str(request.form.get("Comment"))

    #move on to next cluster if at the end of a file
    if local_file.Sequential_Cluster_Id.nunique()>int(session['index'])+1:
        advance_cluster(local_file)




def clear_cluster(local_file):
    """
    A function to clear the comments and matche results in a given cluster
    
    Parameters: local_file (dataframe)
    
    Returns: None
    """
    
    #get lisy of ids's in cluster
    cluster_ids=list(local_file.loc[local_file['Sequential_Cluster_Id']==session['index']][rec_id].values)
    
    #reset Match and Comment columns to their respective default. 
    for i in cluster_ids:
        local_file.loc[local_file[rec_id]==i,'Match']='[]'
        local_file.loc[local_file[rec_id]==i,'Comment']=''

        
def set_continuation_message(local_file, cur_cluster_done):
    """
    A function to change the message displayed on screen, depending if 
    all the matching decisions have been made in a given cluster. 
    
    Parameters: local_file (pandas dataframe)
                cur_cluster_done (Boolean)
                
    Returns:    done_message (String)
    """
    not_last_record=local_file.Sequential_Cluster_Id.nunique()>int(session['index'])+1
    if (not_last_record) or (cur_cluster_done==0):
        done_message='Keep Matching'
    elif (not not_last_record) and (cur_cluster_done==1):
        done_message='Matching Finished- Press save and close the application'
    return done_message
  
def reset_toggles():
    """
    A function to set the highlighter and select all toggles. 

    Parameters: None
    
    Returns: None
    """
    if request.form.get('selectall')=="selectall":
        if session['select_all']==1:
            session['select_all']=0
        elif session['select_all']==0:
            session['select_all']=1
    
    #set highlight differences toggle
    if request.form.get('highlight_differences') == 'highlight_differences':
        if session['highlight_differences']==1:
            session['highlight_differences']=0
        elif session['highlight_differences']==0:
            session['highlight_differences']=1
            
def set_position_vars(columns):
    """
    A function to set some variables, that specific position on the page, 
    relative to the number of columns in the data
    
    Parameters: columns (list)
    
    Returns: button_left (Int), button_right (Int)
    
    """
    column_width = len(columns)+1
    button_left = int(column_width/2)
    button_right = button_left + 2*(column_width / 2 - int(column_width / 2))
    return button_left, button_right
  
def backup_save(save_thread, local_in_prog_path,hdfs_in_prog_path,local_file, local_filepath_done,hdfs_filepath_done):
    """
    A function to save progress, at a given backup point.
    
    Parameters: None
    
    Returns: None
    
    """
    if session['index'] % int(config['custom_setting']['backup_save'])==0:
      
          #set s_thread 
          s_thread=Process(target=save_thread, args= (local_in_prog_path,hdfs_in_prog_path,\
                                              local_file, local_filepath_done,\
                                              hdfs_filepath_done))
          #intiate save process
          s_thread.start()
          
def clear_session():
    """
    A function to remove all the session variables, except for font choice.
    
    Parameters: None
    
    Returns: None
    """
    session_keys=list(session)
    for i in session_keys:
        if i!= 'font_choice':
            session.pop(i)