import os
from csv_to_database import *
import csv_logger


logger = csv_logger.get_logger(__name__)

parameters_dict= {
'user':'admin',
'password': 'password',
'host' :'localhost',
'dbname' : 'demo_db',
'port' : 5432}

def main(file_name : str, date_columns:list = [], copy_from_disk = True, link = None, file_different_path= False):
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

    table_name, df = clean_table_and_col_names(file_name = file_name,
                                                date_columns= date_columns,
                                                file_different_path=file_different_path,
                                                link = link
                                                )
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


if __name__ == '__main__':
    # csv folder must be in the parent directory where the code is situated
    csv_folder = 'csv_folder'
    datetime = ['inserted_at', 'updated_at']
    csv_link = os.getcwd() + '\\'+ csv_folder
    csv_files = os.listdir(csv_link)
    file_different_path = True

    for file in csv_files:
        main(file, date_columns=datetime, copy_from_disk=False, link = csv_link, file_different_path = file_different_path)
        logger.info(f'{file} has been copied successfuly copied into the PostgreSQL table\n')
    logger.info('The csv files has been successfully loaded into the PostgreSQL table\n')

