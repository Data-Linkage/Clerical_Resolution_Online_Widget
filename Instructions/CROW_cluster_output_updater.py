import pandas as pd
import numpy as np

def CROW_output_updater(output_df, ID_column, Source_column, df1_name, df2_name):
    '''
    Returns the outputs of CROW in a pairwise linked format
    
    Parameters
    ----------
    output_df: dataframe
               pandas dataframe containing CROW matched outputs
    ID_column: string 
               name of record_id column in CROW matched outputs
    Source_column: string 
               name of column in CROW matched outputs identifying which
               data source the record is from
    df1_name: string
              name of the first data source
    df2_name: string
              name of the second data source        
    
    Return
    -------
    df: dataframe
    
    Example
    -------
    
    > CROW_output_updater(output_df = matches, ID_column = 'Record_1D', Source_column = 'Source_Dataset', df1_name = 'CENSUS_HH', df2_name = 'PES_HH')
    
    >   puid_CEN	puid_PES
    0	B01	        A01_NAME
    1	B02	        A02_REL
    2	B03 	    A03_SEX
    3	B04_DOB     A04_DOB
    4	B04_Y	    A04_YYYY
    5	B04_M       A04_MM
    6	B04_D	    A04_DD
    7	B05	        A04_AGE
    8	B14  	    A05_RELIG
    9	B18	        A06_MSTATUS
    10  D01         A07_SCH
    11  D03_L       A08_L
    12  D03_G       A08_G
    '''

    # CROW Output
    df = output_df
    
    # Select required columns and only keep clusters containing matches
    df = df[df['Match'] != 'No match in cluster'][[ID_column, Source_column, 'Match']]
    df = df.rename(columns = {ID_column : 'Record_1', Source_column : 'Source_Dataset_1'})
    
    # Create lookup of ID to Source Dataset to use later
    lookup = df[['Record_1', 'Source_Dataset_1']]
    lookup = lookup.rename(columns = {'Record_1' : 'Record_2', 'Source_Dataset_1' : 'Source_Dataset_2'})
    
    # Remove trailing commas and convert to lists
    df['Match'] = df['Match'].str.rstrip(',')
    df['Record_2'] = df['Match'].str.split(',')
    df.drop(['Match'], axis = 1, inplace = True)
    
    # Explode to get all combinations of matched pairs
    df = df.explode('Record_2')
    
    # Types
    df['Record_1'] = df['Record_1'].astype('str')
    df['Record_2'] = df['Record_2'].astype('str')
    lookup['Record_2'] = lookup['Record_2'].astype('str')
    
    # Join on Source Dataset for Record_2
    df = pd.merge(df, lookup, on = 'Record_2', how = 'left')
    
    # Reorder ID columns to identify all duplicates (including cross duplicates)
    df['Record_1_FINAL'] = np.where(df['Source_Dataset_1'] == df1_name, df['Record_1'], df['Record_2'])
    df['Record_2_FINAL'] = np.where(df['Source_Dataset_2'] == df2_name, df['Record_2'], df['Record_1'])
    
    # Remove duplicate (df1 to df1 and/or df2 to df2) matches
    df = df[~((df.Source_Dataset_1 == df.Source_Dataset_2))]
    df.drop(['Source_Dataset_1', 'Source_Dataset_2', 'Record_1', 'Record_2'], axis = 1, inplace = True)
    
    # Remove all duplicates
    df = df.drop_duplicates(['Record_1_FINAL', 'Record_2_FINAL'])
    
    # Rename columns and save
    df = df[['Record_1_FINAL', 'Record_2_FINAL']]
    df = df.rename(columns = {'Record_1_FINAL' : f'puid_{df1_name}', 'Record_2_FINAL' : f'puid_{df2_name}'})
    df.reset_index(drop = True, inplace = True)
    
    return df

matches = pd.read_csv('')
updated_outputs = CROW_output_updater(output_df = , ID_column = '', Source_column = '', df1_name = '', df2_name = '')
