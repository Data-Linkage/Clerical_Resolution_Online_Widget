import pyspark
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions
from pyspark.sql.functions import *

#!pip3 install dlh_utils --upgrade 
#!pip3 install pandas==1.1.5 
#!pip3 install markupsafe  
#!pip3 install pyarrow 
#!pip3 install flask 
#!pip3 install numpy 
#!pip3 install flask_session

import pandas as pd
pd.DataFrame.iteritems = pd.DataFrame.items
import numpy as np
import os
import random
import dlh_utils as dlh

spark = (
    SparkSession.builder.appName("small-session")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", 1)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.maxExecutors", 3)
    .config("spark.sql.shuffle.partitions", 12)
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.repl.eagerEval.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)

###============== Creating Dummy Data========================================###

data_l = [("l1", "ROBIN", "HOOD", "1160", "M", "SHERWOOD FOREST"),
          ("l2", "SHERLOCK", "HOLMES", "01/06/1984", "M", "221B, BAKER STREET"),
          ("l3", "DRACULA", "NONE", "UNKNOWN", "M","CASTLE, PRINCIPALITY OF TRANSYLVANIA"),
          ("l4", "ALICE", "IN WONDERLAND", "MAY 1953", "F", "WONDERLAND"),
          ("l5", "E", "HYDE", "N/A/", "M", "SOHO, LONDON"),
          ("l6", "ROMEO", "MONTAGUE", "1532", "M", "VIENNA")]

schema_l = StructType([
    StructField("id_l", StringType(), True),
    StructField("name", StringType(), True),
    StructField("surname", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("address", StringType(), True)
])

df_l = spark.createDataFrame(data=data_l, schema=schema_l)


data_r = [("ra", "ROBYN", "HOOD", "1160", "M", "SHERWOOD FOREST"),
          ("rb", "SHERLOCK", "HOLMES", "06/01/1984", "M", "221B, BAKER STREET"),
          ("rc", "SHERLOCK", "HOLMES", "01/06/1984", "M", "221B, BAKER ROAD"),
          ("rd", "LOCKY", "HOLMES", "01/06/1984", "M", "221B, BAKER STREET"),
          ("re", "LOCKY", "HOLMES", "06/01/1984", "M", "221B, BAKER STREET"),
          ("rf", "SHIRLEY", "HOLMES", "01/08/1984", "F", "221B, BAKER STREET"),
          ("rg", "SHIRLEY", "HOLMES", "11/08/1984", "F", "221B, BAKER STREET"),
          ("rh", "DRA", "CULA", "N/A", "M", "TRANSYLVANIA"),
          ("ri", "DRACULA", "NONE", "NONE", "M", "PRINCIPALITY OF TRANSYLVANIA"),
          ("rj", "NONE", "NONE", "MAY 1953", "F", "WONDERLAND"),
          ("rk", "ALICE", "IN WONDERLAND", "MAY 1953", "F", "WONDERLAND, LONDON"),
          ("rl", "DR. H", "JACKYLL", "NONE", "M", "THE WEST END, LONDON"),
          ("rm", "DOCTOR", "JACKYLL", "UNKNOWN", "M", "VICTORIAN LONDON"),
          ("rn", "EDWARD", "HYDE", "N/A/", "M", "SOHO, LONDON"),
          ("ro", "HENRY", "JACKYLL", "NONE", "M", "SOHO, LONDON"),
          ("rp", "ROMEO", "MONTAGUE", "1532", "M", "VIENA"),
          ("rq", "ROMEO", "MONTAGUES", "1532", "M", "VIENNA")]

schema_r = StructType([
    StructField("id_r", StringType(), True),
    StructField("name", StringType(), True),
    StructField("surname", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("address", StringType(), True)
])

df_r = spark.createDataFrame(data=data_r, schema=schema_r)

data_spine = [("l1", "ra"),
              ("l2", "rb"),
              ("l2", "rc"),
              ("l2", "rd"),
              ("l2", "re"),
              ("l2", "rf"),
              ("l2", "rg"),
              ("l3", "rh"),
              ("l3", "ri"),
              ("l4", "rj"),
              ("l4", "rk"),
              ("l5", "rl"),
              ("l5", "rm"),
              ("l5", "rn"),
              ("l5", "ro"),
              ("l6", "rp"),
              ("l6", "rq")]

schema_spine = StructType([
    StructField("id_l", StringType(), True),
    StructField("id_r", StringType(), True)
])

df_spine = spark.createDataFrame(data=data_spine, schema=schema_spine)

###============================================================================###

###===================== FUNCTION =============================================###

def crow_samples1(spine, df_l, df_r, id_l, id_r, sample_size,
                  dedupe_df_l, outdir, sample_name):
    """
    Puts your linked data into a format that can be used by CROW2.
    Assigns a cluster and unique record id to each record.
    Standardises null values to an empty string as CROW can't handle nulls.
    Saves your CROW sample as a parquet file with one partition.

    prerequisites
    -------------
    Three datasets are reqiured:
    1. The left dataset with a unique id for each record (id_l)
    2. The right dataset with a unique id for each record (id_r)
    3. A spine for the linked data containing id_l and id_r.
    The spine can include duplicates.

    Please assign the same name to the variables that you wish to
    see in CROW for both the left and right datasets. Extra variables
    that you do not wish to see in CROW but still want them in your
    datasets do not have a name requirement.

    Parameters
    ----------
    spine: (dataset)
      A spine of your data containing the left id and right id.
    df_l: (dataset)
      The left dataset with a unique id for each record (id_l).
    df_r: (dataset)
      The right dataset with a unique id for each record (id_r).
    id_l: (str)
      The name of your unique id variable for your left dataset.
    id_r: (str)
      The name of your unique id variable for your right dataset.
    sample_size: (int)
      The number of pairs or clusters that you want to sample.
    dedupe_df_l: (Boolean)
      = True if you only want to see one left record per cluster.
      = False if you want to see all left records per cluster but
      they will be duplicates if the left record has linked to more
      than one right record.
    outdir: (string)
      The filepath directory where you want to save your sample.
    sample_name: (string)
      The name of the sample you want to save.

    Returns
    -------
    A sample of your linked data ready to be used in CROW.

    Example
    -------

    > df_l.show(5, truncate=0)
    +----+--------+-------------+----------+---+------------------------------------+
    |id_l|name    |surname      |dob       |sex|address                             |
    +----+--------+-------------+----------+---+------------------------------------+
    |l1  |ROBIN   |HOOD         |1160      |M  |SHERWOOD FOREST                     |
    |l2  |SHERLOCK|HOLMES       |01/06/1984|M  |221B, BAKER STREET                  |
    |l3  |DRACULA |NONE         |UNKNOWN   |M  |CASTLE, PRINCIPALITY OF TRANSYLVANIA|
    |l4  |ALICE   |IN WONDERLAND|MAY 1953  |F  |WONDERLAND                          |
    |l5  |E       |HYDE         |N/A/      |M  |SOHO, LONDON                        |
    +----+--------+-------------+----------+---+------------------------------------+

    > df_r.show(10, truncate=0)
    +----+--------+-------+----------+---+----------------------------+
    |id_r|name    |surname|dob       |sex|address                     |
    +----+--------+-------+----------+---+----------------------------+
    |ra  |ROBYN   |HOOD   |1160      |M  |SHERWOOD FOREST             |
    |rb  |SHERLOCK|HOLMES |06/01/1984|M  |221B, BAKER STREET          |
    |rc  |SHERLOCK|HOLMES |01/06/1984|M  |221B, BAKER ROAD            |
    |rd  |LOCKY   |HOLMES |01/06/1984|M  |221B, BAKER STREET          |
    |re  |LOCKY   |HOLMES |06/01/1984|M  |221B, BAKER STREET          |
    |rf  |SHIRLEY |HOLMES |01/08/1984|F  |221B, BAKER STREET          |
    |rg  |SHIRLEY |HOLMES |11/08/1984|F  |221B, BAKER STREET          |
    |rh  |DRA     |CULA   |N/A       |M  |TRANSYLVANIA                |
    |ri  |DRACULA |NONE   |NONE      |M  |PRINCIPALITY OF TRANSYLVANIA|
    |rj  |NONE    |NONE   |MAY 1953  |F  |WONDERLAND                  |
    +----+--------+-------+----------+---+----------------------------+

    > df_spine.show(11, truncate=0)
    +----+----+
    |id_l|id_r|
    +----+----+
    |l1  |ra  |
    |l2  |rb  |
    |l2  |rc  |
    |l2  |rd  |
    |l2  |re  |
    |l2  |rf  |
    |l2  |rg  |
    |l3  |rh  |
    |l3  |ri  |
    |l4  |rj  |
    |l4  |rk  |
    +----+----+
    > crow_samples1(spine=df_spine, df_l=df_l, df_r=df_r, id_l='id_l', id_r='id_r',
                    sample_size=6, dedupe_df_l=True, outdir='/ons/crow/test/',
                    sample_name='crow2test1').sort(col('cluster_id').asc()).show(truncate=0)
    +----+--------+---------+----------+---+------------------------------------+----+----------+-------+---------+
    |id_l|name    |surname  |dob       |sex|address                             |id_r|cluster_id|dataset|record_id|
    +----+--------+---------+----------+---+------------------------------------+----+----------+-------+---------+
    |l1  |ROBYN   |HOOD     |1160      |M  |SHERWOOD FOREST                     |ra  |cluster_1 |df_r   |rid8     |
    |l1  |ROBIN   |HOOD     |1160      |M  |SHERWOOD FOREST                     |ra  |cluster_1 |df_l   |rid12    |
    |l6  |ROMEO   |MONTAGUE |1532      |M  |VIENA                               |rp  |cluster_2 |df_r   |rid5     |
    |l6  |ROMEO   |MONTAGUE |1532      |M  |VIENNA                              |rp  |cluster_2 |df_l   |rid10    |
    |l6  |ROMEO   |MONTAGUES|1532      |M  |VIENNA                              |rq  |cluster_2 |df_r   |rid16    |
    |l3  |DRACULA |NONE     |UNKNOWN   |M  |CASTLE, PRINCIPALITY OF TRANSYLVANIA|ri  |cluster_3 |df_l   |rid11    |
    |l3  |DRACULA |NONE     |NONE      |M  |PRINCIPALITY OF TRANSYLVANIA        |ri  |cluster_3 |df_r   |rid14    |
    |l3  |DRA     |CULA     |N/A       |M  |TRANSYLVANIA                        |rh  |cluster_3 |df_r   |rid21    |
    |l5  |EDWARD  |HYDE     |N/A/      |M  |SOHO, LONDON                        |rn  |cluster_4 |df_r   |rid23    |
    |l5  |DR. H   |JACKYLL  |NONE      |M  |THE WEST END, LONDON                |rl  |cluster_4 |df_r   |rid6     |
    |l5  |E       |HYDE     |N/A/      |M  |SOHO, LONDON                        |rl  |cluster_4 |df_l   |rid13    |
    |l5  |HENRY   |JACKYLL  |NONE      |M  |SOHO, LONDON                        |ro  |cluster_4 |df_r   |rid22    |
    |l5  |DOCTOR  |JACKYLL  |UNKNOWN   |M  |VICTORIAN LONDON                    |rm  |cluster_4 |df_r   |rid15    |
    |l2  |SHERLOCK|HOLMES   |01/06/1984|M  |221B, BAKER STREET                  |rb  |cluster_5 |df_l   |rid2     |
    |l2  |LOCKY   |HOLMES   |06/01/1984|M  |221B, BAKER STREET                  |re  |cluster_5 |df_r   |rid3     |
    |l2  |SHERLOCK|HOLMES   |06/01/1984|M  |221B, BAKER STREET                  |rb  |cluster_5 |df_r   |rid17    |
    |l2  |SHIRLEY |HOLMES   |11/08/1984|F  |221B, BAKER STREET                  |rg  |cluster_5 |df_r   |rid4     |
    |l2  |SHERLOCK|HOLMES   |01/06/1984|M  |221B, BAKER ROAD                    |rc  |cluster_5 |df_r   |rid18    |
    |l2  |LOCKY   |HOLMES   |01/06/1984|M  |221B, BAKER STREET                  |rd  |cluster_5 |df_r   |rid20    |
    |l2  |SHIRLEY |HOLMES   |01/08/1984|F  |221B, BAKER STREET                  |rf  |cluster_5 |df_r   |rid19    |
    +----+--------+---------+----------+---+------------------------------------+----+----------+-------+---------+   
    """

    # Get a sample and add a cluster ID
    cluster_id = (spine
                  .select(id_l)
                  .dropDuplicates()
                  .orderBy(rand())
                  .limit(sample_size))

    w = Window().orderBy(lit('A'))

    cluster_id = (cluster_id
                  .withColumn(
                      'row_number',
                      row_number()
                      .over(w)))

    cluster_id = (cluster_id
                  .withColumn(
                      'cluster_id',
                      concat(lit('cluster_'),
                             col('row_number')))
                  .drop('row_number'))

    sample_spine = spine.join(cluster_id, id_l, 'inner')

    # Add cluster_id to df_l
    df_l = df_l.join(sample_spine, id_l, 'inner')

    # add dataset flag to df_l
    df_l = df_l.withColumn('dataset', lit('df_l'))

    if dedupe_df_l == True:
        df_l = df_l.dropDuplicates([id_l])
    else:
        None

    # Add cluster to df_r
    df_r = df_r.join(sample_spine, id_r, 'inner')

    # add dataset flag to df_r
    df_r = df_r.withColumn('dataset', lit('df_r'))

    # create sample
    sample = dlh.dataframes.union_all(df_l, df_r, fill='')

    # Add unique record_id
    sample = sample.withColumn('row_number',
                               row_number()
                               .over(w))

    sample = (sample
              .withColumn(
                  'row_number',
                  col('row_number')
                  .cast(StringType())))

    sample = (sample
              .withColumn(
                  'record_id',
                  concat(lit('rid'),
                         col('row_number')))
              .drop('row_number'))

    # standardise nulls/missing values as CROW cannot handle missing values
    sample = dlh.standardisation.standardise_null(
        df=sample,
        replace=None,
        subset=None,
        replace_with='',
        regex=False)

    if outdir.endswith('/'):
        outdir = outdir[:-1]
    else:
        None

    return sample.repartition(1).write.parquet(f'{outdir}/{sample_name}')

bucket = 's3a://onscdp-dev-data01-5320d6ca/user/hannah.goode'

for x in ['b','c','d','e']:
  crow_samples1(spine=df_spine, df_l=df_l, df_r=df_r, 
              id_l='id_l', id_r='id_r',
              sample_size=6, dedupe_df_l=True, 
              outdir=bucket,
              sample_name=f'crow_test_{x}') 
  
###-----------------------------------------------------------------###

def crow_analysis1(sample, total_clusters):
  """
    This function analyses the results from your CROW session.
  
    prerequisites
    -------------
    1. Your finished CROW sample that was originally made using
    the crow_samples1() function. The file path for the sample 
    will now take on the form: filepath/{sample_name}_username_done.
    2. Make sure pd.DataFrame.iteritems = pd.DataFrame.items has been run
    
    Parameters
    ----------
    sample: (dataset)
      your finished crow dataset.
    sample_size: (int)
      The number of pairs or clusters that were in your sample.

    Returns
    -------
    This function will tell you how many clusters were a match,
    a partial match or a non-match.
    It will also provide a database of the clusters telling you 
    how many records were in the cluster, the type of match in the cluster,
    how many records from the left and right dataset matched, and how
    many records didnt match in the cluster.
    
    Limitations
    -----------
    This function cannot tell you if a record from the left and right dataset 
    have matched together in CROW samples where there are more than one record 
    from the left dataset per cluster. However, if there is only one record from the left 
    dataset per cluster and there is a '1' in the 'no_df_l_match' column, you
    can assume that the left record has matched to at least one right record.
    
    Example
    -------

    > sample = spark.read.parquet('/ons/crow/DGO_AWAY_DAY/sample2_goodeh_done')
    > sample.show(3, truncate=0)
    |id_l|name |surname  |dob |sex|address|id_r|cluster_id|dataset|record_id|Match                    |Sequential_Cluster_Id|Comment|Sequential_Record_Id|__index_level_0__|
    +----+-----+---------+----+---+-------+----+----------+-------+---------+-------------------------+---------------------+-------+--------------------+-----------------+
    |l6  |ROMEO|MONTAGUE |1532|M  |VIENNA |rp  |cluster_5 |df_l   |rid1     |['rid1', 'rid6', 'rid19']|0                    |       |0                   |0                |
    |l6  |ROMEO|MONTAGUES|1532|M  |VIENNA |rq  |cluster_5 |df_r   |rid6     |['rid1', 'rid6', 'rid19']|0                    |       |1                   |5                |
    |l6  |ROMEO|MONTAGUE |1532|M  |VIENA  |rp  |cluster_5 |df_r   |rid19    |['rid1', 'rid6', 'rid19']|0                    |       |2                   |18               |
    +----+-----+---------+----+---+-------+----+----------+-------+---------+-
    
    > analysis = crow_analysis1(sample=sample, total_clusters=6)
    number of clusters = 6 
    number of full matches = 2 
    number of partial matches = 3 
    number of non-matches = 1
    
    > analysis.show()
    +--------------+-------------+----------+-------------+-------------+------------+
    |cluster_number|   match_type|no_records|no_df_l_match|no_df_r_match|no_non_match|
    +--------------+-------------+----------+-------------+-------------+------------+
    |     cluster_1|partial_match|         3|            1|            1|           1|
    |     cluster_2|partial_match|         5|            0|            3|           2|
    |     cluster_3|   full_match|         2|            1|            1|           0|
    |     cluster_4|    non_match|         3|            0|            0|           3|
    |     cluster_5|   full_match|         3|            1|            2|           0|
    |     cluster_6|partial_match|         7|            1|            2|           4|
    +--------------+-------------+----------+-------------+-------------+------------+
  """
  
  matched_df = pd.DataFrame(columns=['cluster_number',
                                     'match_type',
                                     'no_records',
                                     'no_df_l_match',
                                     'no_df_r_match',
                                     'no_non_match'])
  
  full_match = 0
  partial_match = 0
  non_match = 0
  
  for i in range(1,total_clusters+1):
    df = sample.filter(col('cluster_id')==f'cluster_{i}')
    
    no_records = df.count()
    
    no_non_match = df.filter(col('Match').rlike('No Match In Cluster')).count()
    
    df_match = df.filter(~col('Match').rlike('No Match In Cluster'))
    
    no_df_l_match = df_match.filter(col('dataset')=='df_l').count()
    
    no_df_r_match = df_match.filter(col('dataset')=='df_r').count()
    
    if no_non_match == 0:
      full_match += 1
      match_type = 'full_match'
    elif 0 < (no_df_l_match + no_df_r_match) < no_records:
      partial_match += 1
      match_type = 'partial_match'
    elif no_non_match == no_records:
      non_match += 1
      match_type = 'non_match'
    else:
      match_type = 'error'
      
    matched_df.loc[len(matched_df.index)] = [f'cluster_{i}',
                                             match_type,
                                             no_records,
                                             no_df_l_match,
                                             no_df_r_match,
                                             no_non_match]
  print(f' number of clusters = {total_clusters}',
        f'\n number of full matches = {full_match}',
        f'\n number of partial matches = {partial_match}',
        f'\n number of non-matches = {non_match}')
  
  matched_df = spark.createDataFrame(matched_df)
  
  return matched_df

bucket = 's3a://onscdp-dev-data01-5320d6ca/user/hannah.goode'
sample_c = spark.read.parquet(f'{bucket}/crow_test_c_hannah.goode_done')
sample_c.show()
analysis_c = crow_analysis1(sample = sample_c, 
                            total_clusters = 6)
analysis_c.show()
