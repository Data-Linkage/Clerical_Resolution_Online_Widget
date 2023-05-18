def advance_cluster(df):
  #note this function is very clunky and could likely be improved. 
  """
  1)A Function to: determine the number of matches made in a given cluster
  2)If 1 or 0 unmatcher records in cluster remain; progress to the next cluster. 
  
  Parameters: None
  Returns: None
  
  """
  num_in_cluster=len(df.loc[df['Sequential_Cluster_Id']==session['index']])
  print(num_in_cluster)
  list_decided=[set(ast.literal_eval(i)) for i in df.loc[df['Sequential_Cluster_Id']==session['index']]['Match']]
  print(list_decided)
  uni_set_decided={x for l in list_decided for x in l}
  print(uni_set_decided)
  num_decided=len(uni_set_decided)
  if (num_in_cluster-num_decided)<=1:
      for r_id in [x for x in df.loc[df['Sequential_Cluster_Id']==session['index']][rec_id] if x not in uni_set_decided]: 
            df.loc[df[rec_id]==r_id,'Match']=f"['No Match In Cluster For {r_id}']"
      session['index'] = int(session['index'])+ 1
      
def check_matching_done(df): 
  if len(df[df.Match == '[]'])>0: 
    return 0
  if len(df[df.Match == '[]'])==0:
    print('matching_done')
    return 1
  
def save_rename_hive(dataframe, old_path,new_path):
    sparkDF=spark.createDataFrame(dataframe)
    sparkDF.registerTempTable("temp_table")
    spark.sql(f"""DROP TABLE IF EXISTS {old_path}""")
    #unhash to delete after new table created 
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {new_path} AS SELECT * FROM temp_table""")


def get_save_paths(origin_file_path,origin_file_path_fl ):
      if 'inProgress' in origin_file_path_fl[-1]:

            # If it is the same user
            if (user in origin_file_path_fl[-1]):
                # Dont rename the file
                in_prog_path= origin_file_path
                end_file_name=origin_file_path_fl[-1][:-11]+'_DONE'
                # create the filepath name for when the file is finished
                filepath_done = ".".join([origin_file_path_fl[0], end_file_name])
                print(f'filepath done={filepath_done}')
                print(f'in prog path={in_prog_path}')
            else:

                print('USER_NOT_IN_NAME')

                # Rename the file to contain the additional user
                in_prog_path = origin_file_path_fl[-1][:-11]+f'_{user}'+'_inProgress'


                end_file_name=origin_file_path_fl[-1][:-11]+f'_{user}'+'_DONE'
                filepath_done=".".join([origin_file_path_fl[0],end_file_name ])
                print(f'filepath done={filepath_done}')
                print(f'in prog path={in_prog_path}')                        



        # If a user is picking this file again and its done
      elif 'DONE' in origin_file_path_fl[-1]:

            # If it is the same user
            if (user in origin_file_path_fl[-1]):
                # dont change filepath done - keep it as it is
                filepath_done = origin_file_path

                # Rename the file 
                in_prog_path=".".join([origin_file_path_fl[0],origin_file_path_fl[-1][:-5]+'_inProgress'])
                print(f'filepath done={filepath_done}')
                print(f'in prog path={in_prog_path}')

            else:
                # If it is a different user
                # Rename the file to include the additional user
                in_prog_path=".".join([origin_file_path_fl[0],origin_file_path_fl[-1][:-5]+f'_{user}'+'_inProgress'])

                # create the filepath done
                filepath_done=".".join([origin_file_path_fl[0],in_prog_path[:-11]+'_DONE' ])
                print(f'filepath done={filepath_done}')
                print(f'in prog path={in_prog_path}')
                
      else:

              in_prog_path=".".join([origin_file_path_fl[0],origin_file_path_fl[-1]+f'_{user}'+'_inProgress' ])
              filepath_done=".".join([origin_file_path_fl[0],origin_file_path_fl[-1]+f'_{user}'+'_DONE' ])
      
      return in_prog_path, filepath_done