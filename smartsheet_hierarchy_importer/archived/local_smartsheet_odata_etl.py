import sys
import json
import yaml
import requests
import pandas as pd
import numpy as np
import datetime
import smartsheet_api as ssa
from pathlib import Path
from datetime import timedelta
from datetime import datetime
from datetime import date
from requests.auth import HTTPBasicAuth
import smartsheet
import time
import re
import argparse


def read_credentials(cred_file):
    project_dir = Path(__file__).parent
    credentials_filepath = project_dir.joinpath(cred_file)
    creds = yaml.load(open(credentials_filepath), Loader=yaml.FullLoader)
    return(creds)

def read_sql_query(query_file,site_code,datasource):
    project_dir = Path(__file__).parent
    sql_filepath = project_dir.joinpath(query_file)
    sql_query = yaml.load(open(sql_filepath), Loader=yaml.FullLoader)

    query_string = {}
    site_tag = site_code+"_"+datasource

    query_string[datasource] = sql_query.get(site_tag).get("sql_query")

    return(query_string)

def connect_odata(creds_db):

    odata_url = creds_db['odata_url']
    username = creds_db['username']
    password = creds_db['password']

    try:
        response = requests.get(odata_url, auth=HTTPBasicAuth(username, password))
    except Exception as e:
        print(e)
    return(response)

def parse_xml_response(response,primary_key):
    if response.status_code == 200:
        xml_data = response.content
    
        ns = {
                'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices',
                'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
                'atom': 'http://www.w3.org/2005/Atom'
            }
        
        df_xml = pd.read_xml(xml_data,xpath='.//atom:entry/atom:content/m:properties', namespaces=ns)
        df_xml.rename(columns=lambda x: x.replace('-', ''), inplace=True)
        df_xml.rename(columns=lambda x: re.sub('_x.{4}_','_',x), inplace=True)
        df_xml.replace({np.nan: "None"}, inplace=True)
        # Deduplicate on all columns except primary key
        dedup_columns = df_xml.drop(columns=primary_key).columns.tolist()
        df_xml = df_xml.drop_duplicates(subset=dedup_columns)

    else:
        print(f'Failed to fetch data. Status code: {response.status_code}')
    
    return(df_xml)

def save_rows_to_df(rows,column_map_idtoname,primary_key):
    data = []
    qr_id = None
    parent_row_id = None

    for row in rows:
        row_dict = {}
        row_dict["Smartsheet_Row_Id"] = row.id
        for cell in row.cells:
            row_dict[column_map_idtoname[cell.column_id]] = cell.value
        data.append(row_dict)

    df = pd.DataFrame()
    if len(data)>0:
        df = pd.DataFrame(data)
        df = df.astype(str)
        df["Smartsheet_Row_Id"] = df["Smartsheet_Row_Id"].astype(int)
        df[primary_key] = df[primary_key].replace('\.0$','',regex=True)

    return(df)

def run_smartsheet_update_data(**kwargs):
    sm = kwargs['smartsheet']
    sheet_id = kwargs['sheet_id']
    old_data_df = kwargs['old_data_df']
    new_data_df = kwargs['new_data_df']
    column_map = kwargs['column_map']
    pk_field = kwargs['pk_field']
    delete_flag = kwargs['delete_flag']
    updated_records = {}
    update_row_cells = []
    delete_row_id = []
    update_sheet = None
    
    column_map = {key: value for key, value in column_map.items() if key in new_data_df.columns}

    for index, sm_row in old_data_df.iterrows():

        pk_id = sm_row[pk_field]    
        row_id = sm_row['Smartsheet_Row_Id']

        ### Delete Records if Flag is True and PK ID from old record is no longer present in new record dataframe
        if delete_flag==True:
            if not pk_id in new_data_df[pk_field].values:
                delete_row_id.append(row_id)
                continue

        for column_name in column_map:
            if column_name == pk_field:
                continue
            
            # print(new_data_df)
            # print(pk_field)
            # print(pk_id)
            # print(column_name)
            # print(new_data_df[new_data_df[pk_field]==1])
            new_col_val = str(new_data_df[new_data_df[pk_field]==int(pk_id)][column_name].values[0])
            sm_col_val = str(sm_row[column_name])

            date_pattern= re.compile(r'^\d{4}-\d{2}-\d{2}$')
            numeric_pattern = re.compile(r'\.0$')

            if date_pattern.search(sm_col_val):
                sm_col_val = datetime.strptime(sm_col_val,'%Y-%m-%d').strftime('%m/%d/%Y')
            elif numeric_pattern.search(sm_col_val):
                sm_col_val = sm_col_val.replace('.0','')
            elif numeric_pattern.search(new_col_val):
                new_col_val = new_col_val.replace('.0','')
                
            if sm_col_val != new_col_val:
                updated_records[pk_id] = 1
                update_row_cells.extend([
                        {   'row_id': row_id,
                            'column_id': column_map[column_name],
                            'value': new_col_val,
                            'strict': False
                        }
                    ])

    ### Update Records
    if len(update_row_cells)>0:
        update_sheet = sm.update_smartsheet_cell(sheet_id=sheet_id,
                                                 update_row_cells=update_row_cells)
                                                 
        print(f'{len(updated_records)} records updated in smartsheet')

    if len(delete_row_id)>0:
        update_sheet = sm.delete_rows_from_sheet(sheet_id=sheet_id,row_ids=delete_row_id)
    
        print(f'{len(delete_row_id)} records deleted in smartsheet')

    return(update_sheet)

def run_smartsheet_add_data(**kwargs):

    sm = kwargs['smartsheet']
    sheet_id = kwargs['sheet_id']
    new_data_df = kwargs['new_data_df']
    column_map = kwargs['column_map']
    update_sheet = None

    column_map = {key: value for key, value in column_map.items() if key in new_data_df.columns}
    
    for index, row in new_data_df.iterrows():
        sm_row_cells = []
        for column_name in column_map:
            sm_cell_model = {
                'column_id': column_map[column_name],
                'value': row[column_name],
                'strict': False
            }
            sm_row_cells.append(sm_cell_model)  

        ## Add parent rows
        update_sheet = sm.add_row_into_sheet(sheet_id=sheet_id,
                                             parent_row_id=None,
                                             add_cells=sm_row_cells,
                                             to_bottom=True,
                                             add_predecessor=False)

    return(update_sheet)



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--site_code", required=True, help="Provide code for the site, example GRE = Greenville, GRO = Groningen, FLO = Florence, CAM = Cambridge")
    parser.add_argument("--credential", required=True, help="File with database and smartsheet credentials")
    parser.add_argument("--odata_connection", required=True,default="ODATA_CONNECTION", help="Name of the ODATA to Connection")
    parser.add_argument("--odata_name", required=True, default="odata_table", help="Name of the odata source")
    parser.add_argument("--odata_link", required=True, help="Url for the odata source")
    parser.add_argument("--primary_key", required=True, help="Name of the primary key field in the sql data")
    parser.add_argument("--delete_closed", action="store_true", help="If true, delete closed records from Smartsheet")
    parser.add_argument("--out_file_name", help="Name of the output file for storing data")
    args = parser.parse_args()
    
    site_code = args.site_code.upper()
    primary_key = args.primary_key

    start_time = datetime.now()
    print(f"Started At {start_time}")

    ### Read SQL Credentials
    print("Read Credentials")
    creds = read_credentials(args.credential)

    retry_count = 3
    retry_delay = 5

    response = ''

    ## Connect to Odata and read data
    while not response and retry_count > 0:
        print(f"Establishing Connection with ODATA Feed")
        time.sleep(retry_delay)
        response = connect_odata(creds[args.odata_connection])
        retry_count -= 1

    ## Parse Odata Response
    df_data = parse_xml_response(response,primary_key)
    
    ## Build Column Template for Smartsheet
    print("Get Smartsheet Template")
    new_smartsheet_column = []
    smartsheet_column_type = {}
    set_primary_column=1

    for column in df_data.columns:
        if df_data[column].dtype.name =='datetime64[ns]':
            new_smartsheet_column.append(smartsheet.models.Column({
                'title': column,
                'type': 'DATE'
            }))
            df_data[column] = df_data[column].dt.strftime('%m/%d/%Y')
            smartsheet_column_type[column] = 'DATE'
        else:
            if set_primary_column==1:
                new_smartsheet_column.append(smartsheet.models.Column({
                    'title': column,
                    'type': 'TEXT_NUMBER',
                    'primary': True
                }))
                set_primary_column=0
            else:
                new_smartsheet_column.append(smartsheet.models.Column({
                    'title': column,
                    'type': 'TEXT_NUMBER'
                })) 
            smartsheet_column_type[column] = 'TEXT_NUMBER'


    ### Smartsheet API
    print("Initialize Smartsheet API")
    sm = ssa.smartsheet_api(creds['SMARTSHEET'])

    ### Get list of value for split field
    ### Data will be split into different files based on this field values
    ### Each smartsheet will be named as per the value in the list
    partition_key_list = list()
    if args.out_file_name:
        partition_key_list = [args.out_file_name]
    else:
        partition_key_list = [args.odata_name]

    ### Get name and smartsheet id for all sheets in the folder
    print("Get all Smartsheet Name as Key and ID and Value in a Folder")
    sheet_dict = sm.get_all_sheets_in_folder()

    for partition_key in partition_key_list:

        sheet_name = re.sub('/',' ',partition_key)

        is_new_sheet = 0 

        ## If sheet is not present in the folder
        if sheet_name not in sheet_dict:
            print(f"Creating new Smartsheet for {sheet_name}")
            response = sm.create_sheet_in_folder(new_sheet_name=sheet_name,new_sheet_template=new_smartsheet_column)
            is_new_sheet = 1

        ## Get smartsheet id for the current sheet
        print("Get Sheet ID for the current Smartsheet")
        sheet_id = sm.get_sheet_by_name_in_folder(sheet_name=sheet_name)

        print("Map Column Names To Column Id in current Smartsheet")
        column_map = sm.get_column_name_id_map(sheet_id=sheet_id)

        ## Get row ids for current data in smartsheet
        if is_new_sheet == 0:
            print("Get Row data From Smartsheet")
            rows = sm.get_rows_from_sheet(sheet_id=sheet_id)
            print("Check for existing data in Smartsheet")
            current_smartsheet_df = pd.DataFrame()
            current_smartsheet_df = save_rows_to_df(rows,column_map['id_to_name'],primary_key)
            current_smartsheet_df = current_smartsheet_df.fillna("")
            
            ### Add new column in existing smartsheet
            
            if all(column in data_df.columns for column in current_smartsheet_df.columns)==False:
                print("Add New Column to the Smartsheet")
                for index, column in enumerate(data_df.columns):
                    if column not in current_smartsheet_df.columns:
                        column_type = smartsheet_column_type[column]
                        response = sm.add_column_to_smartsheet(sheet_id=sheet_id, column_name=column, column_type=column_type, column_index=index)
                        current_smartsheet_df[column] = ''
                        print(f'{column} column added to the Smartsheet')

                print("Re-Map Column Names To Column Id in current Smartsheet")
                column_map = sm.get_column_name_id_map(sheet_id=sheet_id)

            if not current_smartsheet_df.empty:
                print(f"Update Records in {sheet_name} Smartsheet")
                updated_sheet = run_smartsheet_update_data(smartsheet=sm,
                                                           sheet_id=sheet_id,
                                                           old_data_df=current_smartsheet_df,
                                                           new_data_df=data_df,
                                                           column_map=column_map['name_to_id'],
                                                           pk_field=primary_key,
                                                           delete_flag=args.delete_closed
                                                           )

                ## Filter to add only new record
                data_df = data_df[~data_df[primary_key].isin(list(map(int,current_smartsheet_df[primary_key].unique().tolist())))]
                

        ## Add new data to smartsheets
        print(f"Add New Records in {sheet_name} Smartsheet")
        print(f"Adding {len(data_df)} new records")
        updated_sheet = run_smartsheet_add_data(smartsheet=sm,
                                                sheet_id=sheet_id,
                                                new_data_df=data_df,
                                                column_map=column_map['name_to_id'])
        
        #sys.exit(1)
        

    end_time = datetime.now()
    print(f"Finished At {end_time}")
    total_time = end_time - start_time
    total_time_s = total_time.total_seconds()
    total_time_h = divmod(total_time_s, 60)[0]
    print(f"Completed in {total_time_h} minutes")
    sys.exit(1)



    


    
    
    
