import ast
import os
#os.chdir('/home/cdsw/Clerical_Resolution_Online_Widget/flask_poc')
import subprocess
import configparser
from flask import  session
user = os.environ['HADOOP_USER_NAME']
config = configparser.ConfigParser()
config.read('config_flow.ini')
rec_id=config['id_variables']['record_id']
clust_id=config['id_variables']['cluster_id']
#ile=spark.sql(f"SELECT * FROM {config['filepath']['file']}")
user = os.environ['HADOOP_USER_NAME']

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

    file_test = subprocess.run(f"hdfs dfs -test -f {hdfs_path}", shell=True, stdout=subprocess.PIPE)
    dir_test = subprocess.run(f"hdfs dfs -test -d {hdfs_path}", shell=True, stdout=subprocess.PIPE)
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
        raise Exception('no record ID in data')
    if not df[rec_id].is_unique: 
        raise Exception('record ids are not unique; contact your project leader for guidance ')
    if clust_id not in df.columns: 
        raise Exception('no cluster id column in data')

    
def validate_input_data(filepath):
    """
    Checks that the size of the file is smaller than 0.5GB and raises an error if not
    """
    if os.path.getsize(filepath) > 536871:
        raise ('Filesize error; file is bigger than 0.5GB')
        




