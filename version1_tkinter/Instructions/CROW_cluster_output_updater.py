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

    > CROW_output_updater(output_df = matches, ID_column = 'Record_1D',
                          Source_column = 'Source_Dataset',
                          df1_name = 'CEN', df2_name = 'PES')

    >   puid_CEN	puid_PES
    0	A2	        ABC1
    1	A3	        ABC1
    2	ABC1	    A2
    3	A3	        A2
    4	ABC1	    A3
    5	A2	        A3
    6	AC12	    A9
    7	C11	        ABC10
    8	ABC10	    C11
    9	A9	        AC12
    '''

    # CROW Output
    df = output_df

    # Select required columns and only keep clusters containing matches
    df = df[df['Match'] != 'No match in cluster'][[ID_column, Source_column, 'Match']]
    df = df.rename(columns = {ID_column : 'Record_1', Source_column : 'Source_Dataset_1'})

    # Create lookup of ID to Source Dataset to use later
    lookup = df[['Record_1', 'Source_Dataset_1']]
    lookup = lookup.rename(columns = {'Record_1' : 'Record_2',
                                      'Source_Dataset_1' : 'Source_Dataset_2'})

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
    df['Record_1_FINAL'] = np.where(df['Source_Dataset_1'] == df1_name,
                                    df['Record_1'], df['Record_2'])
    df['Record_2_FINAL'] = np.where(df['Source_Dataset_2'] == df2_name,
                                    df['Record_2'], df['Record_1'])

    # Remove duplicate (df1 to df1 and/or df2 to df2) matches
    df = df[~((df.Source_Dataset_1 == df.Source_Dataset_2))]
    df.drop(['Source_Dataset_1', 'Source_Dataset_2', 'Record_1', 'Record_2'],
            axis = 1, inplace = True)

    # Remove all duplicates
    df = df.drop_duplicates(['Record_1_FINAL', 'Record_2_FINAL'])

    # Rename columns and save
    df = df[['Record_1_FINAL', 'Record_2_FINAL']]
    df = df.rename(columns = {'Record_1_FINAL' : f'puid_{df1_name}',
                              'Record_2_FINAL' : f'puid_{df2_name}'})
    df.reset_index(drop = True, inplace = True)

    return df
