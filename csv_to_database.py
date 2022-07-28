from email import header
import pandas as pd
import psycopg2
import os
import sys
from io import StringIO

parameters_dict= {
'user':'admin',
'password': 'password',
'host' :'localhost',
'dbname' : 'demo_db',
'port' : 5432}

def clean_table_and_col_names(file_name: str, date_columns : list = []):
    """
    This clean the filename and columns, and change the datetime column type from object to datetime type

    Parameters
    -----------
    file_name:  str
                Name of CSV file to be ingested 
    
    date_columns:   list, optional
                    List of date columns to be convert

    Returns
    ----------
    table_name:  str
                Name of the tabel to be created in the database

    df :        dataframe, table
                the cleaned dataframe to be ingested into the database;
    """
    
    df = pd.read_csv(file_name)

    # change file_name into table_name with lower case and removal of spaces, dashes.
    table_name = file_name.lower().replace(' ', '_').replace('-', '_').replace('\\', '_').replace('/', '_').split('.')[0]

    # processed column names into lower_case, and removal of spaces, dashes
    df.columns = [x.lower().replace(' ', '_').replace('-', '_').replace('\\', '_').replace('/', '_') for x in df.columns]

    df = change_date_column(df, date_columns)
    
    return table_name, df

def change_date_column(df, date_columns : list = []):
    """
    This clean the datetime column from the dataframe from object type to datetime type

    Parameters
    -----------
    df :        dataframe, table
                the cleaned dataframe to be ingested into the database;

    date_columns:   list, optional
                    List of date columns to be convert

    Returns
    ---------
    df :        dataframe, table
                the cleaned dataframe which datetime has been changed;
    """

    if len(date_columns) != 0:
        for i in date_columns:
            column = i.lower().replace(' ', '_').replace('-', '_').replace('\\', '_').replace('/', '_')
            df[column] = pd.to_datetime(df[column])
    
    return df

def change_dtype(df):
    """
    This create the columns and their SQL object by replacement of pandas type to SQL type 

    Parameters
    -----------
    df :        dataframe, table
                the cleaned dataframe to be ingested into the database

    Returns
    ---------
    col_to_str :    str   
                the string that contains the attributes and its corresponding SQL data type
    """
    
    #preprocessing of the df columns
    replacement = {
    "object" : 'varchar',
     "float64": 'float',
     'int64': 'int',
     'datetime64[ns]': 'timestamp'
        }
    
    col_to_str = ', '.join(f"{n} {d}" for (n,d) in zip(df.columns, df.dtypes.replace(replacement)) )


    return col_to_str

def connect(parameter):
    """
    This create conn object after successful connection or exit

    Parameters
    -----------
    parameter :     dict
                    parameters of the database

    Returns
    ---------
    conn :      connection obj  
                connection object if successfully connected
    """
    
    conn = None
    try:
        print('Connecting to the PostgreSQL database............>>>>>>>>>')
        conn = psycopg2.connect(**parameter)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        print('Connection is unsucessful >>>>>>>>>>>>>>')
        print('Program exit the interface ......>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        sys.exit(1)
    print('Connection successful ..........')

    return conn


def copy_from_disk(conn, df, table_name, col_to_str):

    """
    This takes the preprocessed dataframe, save it to the disk in csv format
    and implement copy_from module to copy it to postgres table

    Parameters
    -----------
    conn :      connection obj  
                connection object if successfully connected

    df :        dataframe, table
                the cleaned dataframe to be ingested into the database

    table_name:  str
                Name of the tabel to be created in the database

    col_to_str :    str   
                the string that contains the CSV attributes and its corresponding SQL data type

    Returns
    ----------

    """
    
    with conn.cursor() as cur:

        # DDL TRANSACTION
        cur.execute('DROP TABLE IF EXISTS %s ;' %(table_name))

        create_script = "CREATE TABLE %s (%s);" %(table_name, col_to_str) 

        
        cur.execute(create_script)
        
        os.makedirs('clean_csv', exist_ok=True)


        df.to_csv(f'clean_csv/{table_name}.csv',index=False, header = False)

        f = open(f'clean_csv/{table_name}.csv', 'r')

        try:
            cur.copy_from(f, table_name, sep=',')

            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print('Error: %s' %(error))

            conn.rollback()
    print(f'The table {table_name} has been successful be loaded with data')
    print('The file has been successful copied with copy_from_disk() function')
    conn.close()

    return 


def copy_from_stringio(conn, df, table_name, col_to_str):

    """
    This takes the preprocessed dataframe, save it in memory in csv format
    and use copy_from module to copy it to postgres table

    Parameters
    -----------
    conn :      connection obj  
                connection object if successfully connected

    df :        dataframe, table
                the cleaned dataframe to be ingested into the database

    table_name:  str
                Name of the tabel to be created in the database

    col_to_str :    str   
                the string that contains the CSV attributes and its corresponding SQL data type

    Returns
    ----------

    """

    buffer = StringIO

    df.to_csv(buffer, index = False, heade = False)

    buffer.seek(0)
    
    with conn.cursor() as cur:

        # DDL TRANSACTION
        cur.execute('DROP TABLE IF EXISTS %s ;' %(table_name))

        create_script = "CREATE TABLE %s (%s);" %(table_name, col_to_str) 

        
        cur.execute(create_script)
        
        try:
            cur.copy_from(buffer, table_name, sep=',')

            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print('Error: %s' %(error))

            conn.rollback()
    print(f'The table {table_name} has been successful be loaded with data')
    print('The file has been successful copied using copy_from_stringio() function')
    conn.close()

    return 




def main(file_name : str, date_columns:list = [], copy_from_disk = False, copy_from_stringio = False):
    """
    This runs the functions altogether and print necessary statement

    Parameters
    -----------
    file_name:  str
                Name of CSV file to be ingested 
    
    date_columns:   list, optional
                    List of date columns to be convert

    Returns
    ----------

    """

    table_name, df = clean_table_and_col_names(file_name, date_columns)
    col_to_str = change_dtype(df)
    conn = connect(parameters_dict)
    if copy_from_disk == True:
        copy_from_disk(
            conn = conn,
            df = df,
            table_name = table_name,
            col_to_str = col_to_str
        )
    else:
        copy_from_stringio(
            conn = conn,
            df = df,
            table_name = table_name,
            col_to_str = col_to_str
        )

main('employees.csv', ['HIRE_DATE'])



