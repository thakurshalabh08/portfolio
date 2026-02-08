import sys
import json
import yaml
import sqlalchemy
import oracledb
import pymssql
import pandas as pd
import numpy as np
import datetime
import smartsheet_api as ssa
from pathlib import Path
from datetime import timedelta
from datetime import datetime
from datetime import date
import smartsheet
import time
import re
import argparse


def read_credentials(cred_file):
    project_dir = Path(__file__).parent
    credentials_filepath = project_dir.joinpath(cred_file)
    creds = yaml.load(open(credentials_filepath), Loader=yaml.FullLoader)
    return(creds)

def read_sql_query(query_file,site_code,product_complaint):
    project_dir = Path(__file__).parent
    sql_filepath = project_dir.joinpath(query_file)
    sql_query = yaml.load(open(sql_filepath), Loader=yaml.FullLoader)

    query_string = {}
    site_deviation_tag = site_code+"_GTW_DEVIATION"

    if product_complaint == True :
        query_string["GTW"] = sql_query.get(site_deviation_tag).get("only_product_complaint")
    else:
        query_string["GTW"] = sql_query.get(site_deviation_tag).get("excluding_product_complaint")

    query_string["IMOST"] = sql_query.get("IMOST").get("batch_tafqar")
    query_string["GTW_STATUS_DATE"] = sql_query.get("GTW_AUTO_UPDATE")

    #with open(sql_filepath,'r') as f_in:
    #    query_string = " ".join([line.strip() for line in f_in.readlines()])

    return(query_string)

def read_smartsheet_template(smartsheet_template_file):
    project_dir = Path(__file__).parent
    sm_template_filepath = project_dir.joinpath(smartsheet_template_file)
    smart_template = pd.read_csv(sm_template_filepath)
    return(smart_template)

def create_db_connection(creds_db):

    dbtype = creds_db['dbtype']
    servername = creds_db['host']
    driver = creds_db['driver']
    port = creds_db['port']
    database = creds_db['database']
    username = creds_db['username']
    password = creds_db['password']

    dbcon = None
    engine = None

    try:
        if dbtype == 'Oracle':
            sid = oracledb.makedsn(servername, port, sid=database)
            connection_url = f'oracle+oracledb://{username}:{password}@{sid}'
        elif dbtype == 'Sql Server':
            connection_url = f'mssql+pymssql://{username}:{password}@{servername}/{database}'
        engine = sqlalchemy.create_engine(connection_url)
        dbcon = engine.connect()
        dbcon.autocommit = True
    except Exception as e:
        print(e)
    return(dbcon,engine)

def add_business_days(start_date, num_days):
    end_date = start_date
    business_days_added = 0
    while business_days_added < num_days:
        end_date += timedelta(days=1)
        weekday = end_date.weekday()
        if weekday >= 5:  # Saturday is 5 and Sunday is 6
            continue
        business_days_added += 1
    return end_date

def save_rows_to_df(rows,column_map_idtoname):
    data = []
    qr_id = None
    parent_row_id = None

    for row in rows:
        row_dict = {}
        row_level = 3
        row_dict["Smartsheet_Row_Id"] = row.id
        if row.parent_id == None:
            qr_id = row.cells[0].value
            parent_row_id = row.id
            row_level = 1
        elif row.parent_id == parent_row_id:
            row_level = 2
        
        row_dict['Task_Level'] = row_level
        row_dict['QR_Id'] = qr_id
    
        for cell in row.cells:
            row_dict[column_map_idtoname[cell.column_id]] = cell.value
        data.append(row_dict)

    df = pd.DataFrame(data)

    return(df)

def save_data_to_sql(**kwargs):
    sql_engine = kwargs['sql_engine']
    export_df = kwargs['dataframe']
    ## Drop unwanted columns
    #export_df = export_df.drop(['Started','Finished','Baseline Start','Baseline Finish','Variance'], axis=1)
    ## Reformat columns
    export_df.columns = export_df.columns.str.replace(' ', '_')
    export_df = export_df.astype(str)
    export_df = export_df.rename(columns={'%_Complete': 'Percent_Complete'})
    export_df['QR_Id'] = export_df['QR_Id'].str.replace('.0','',regex=False)
    export_df['Days_Open'] = export_df['Days_Open'].str.replace('.0','',regex=False)
    export_df['Days_Overdue'] = export_df['Days_Overdue'].str.replace('.0','',regex=False)
    export_df['Investigation_Iteration'] = export_df['Investigation_Iteration'].str.replace('.0','',regex=False)
    export_df['Days_Remaining_Until_Due'] = export_df['Days_Remaining_Until_Due'].str.replace('.0','',regex=False)
    export_df['Task_Name'] = export_df['Task_Name'].str.replace('.0','',regex=False)
    export_df['GTW_Born_On_Date'] = export_df['GTW_Born_On_Date'].str.replace('T.+','',regex=True)
    export_df['GTW_Target_Finish'] = export_df['GTW_Target_Finish'].str.replace('T.+','',regex=True)
    export_df = export_df.replace("N/A",'None')
    
    ## Insert data into SQl table
    if export_df['DR_Type'].values[0]=='Product Complaint':
        export_df.to_sql(name='PY_ProductComplaint_Deviation',con=sql_engine,if_exists='append',schema='dbo',index=False,method='multi',chunksize=50)
    else:
        export_df.to_sql(name='PY_Deviation',con=sql_engine,if_exists='append',schema='dbo',index=False,method='multi',chunksize=50)

def run_smartsheet_update_data(**kwargs):
    sm = kwargs['smartsheet']
    sheet_id = kwargs['sheet_id']
    old_data_df = kwargs['old_data_df']
    new_data_df = kwargs['new_data_df']
    column_map = kwargs['column_map']
    subtask_map_all = kwargs['subtask']
    status_sql = kwargs['sql_string']
    oracle_engine = kwargs['oracle_engine']
    today_date = date.today().strftime("%m/%d/%Y")
    delete_row_id = []
    closed_deviation = []
    updated_deviation = []
    update_row_cells = []
    update_sheet = None

    old_data_df = old_data_df[(old_data_df.QR_Id.isin(new_data_df.qr_id.unique().tolist()))]

    date_pattern= re.compile(r'\d{4}-\d{2}-\d{2}')
    date_pattern_gtw= re.compile(r'\d{2}\/\d{2}\/\d{2}')
    
    for index, sm_row in old_data_df.iterrows():

        qr_id = int(sm_row['QR_Id'])

        update_check = 0

        new_qr_record = new_data_df[new_data_df.qr_id==qr_id].copy()

        gtw_responsible_email = new_qr_record['responsible_email'].values[0]
        gtw_responsible_name = new_qr_record['responsible_name'].values[0]
        gtw_status = new_qr_record['status'].values[0]
        gtw_due_date = new_qr_record['due_date'].values[0]
        gtw_batch = new_qr_record['batch'].values[0]
        gtw_tafqar = new_qr_record['tafqar_dt'].values[0]
        gtw_respo_dept = new_qr_record['responsible_dept'].values[0]
        gtw_client = new_qr_record['client'].values[0]
        gtw_report_to_name = new_qr_record['reporting_to'].values[0]
        gtw_report_to_email = new_qr_record['reporting_to_email'].values[0]
        gtw_dr_type = new_qr_record['dr_type'].values[0]
        gtw_desc = new_qr_record['short_description'].values[0]
        gtw_max_iteration = new_qr_record['deviation_iteration_num'].values[0]
        gtw_is_reopened = new_qr_record['deviation_reopened_after_closing'].values[0]
        gtw_reopened_date = new_qr_record['reopen_date'].values[0]
        gtw_closed_date = new_qr_record['date_closed'].values[0]
        gtw_current_state_date = new_qr_record['date_current_state'].values[0]
        gtw_criticality = new_qr_record['criticality'].values[0]

        if sm_row['Task_Level'] == 1:
            ## Get the Status update date based on QR-ID and Iteration Number
            query_sql = status_sql["multi_iteration"]
            query_sql = query_sql.format(QR_ID=qr_id)    
            gtw_status_df = pd.read_sql(query_sql,oracle_engine)
            record_in_smartsheet_date = datetime.strptime(sm_row['Started Date'],'%Y-%m-%d')

            print(record_in_smartsheet_date)
            print(datetime.strptime('2024-03-19','%Y-%m-%d'))

            if record_in_smartsheet_date < datetime.strptime('2024-03-19','%Y-%m-%d'):
                subtask_map = subtask_map_all[subtask_map_all.Version==1]
            else:
                subtask_map = subtask_map_all[subtask_map_all.Version==2]

            if len(gtw_status_df) == 0:
                query_sql = status_sql["first_iteration"]
                query_sql = query_sql.format(QR_ID=qr_id)
                gtw_status_df = pd.read_sql(query_sql,oracle_engine)

            last_closed_date = gtw_closed_date

            if not date_pattern_gtw.search(str(last_closed_date)):
                last_closed_date = 'N/A'

            if not sm_row['Completion Date']==None:
                completion_date = datetime.strptime(sm_row['Completion Date'],'%Y-%m-%d').strftime('%m/%d/%Y')
            else:
                completion_date = None

            if not sm_row['Reopened Date']==None:
                previous_reopen_date = datetime.strptime(sm_row['Reopened Date'],'%Y-%m-%d').strftime('%m/%d/%Y')
            else:
                previous_reopen_date = None

            if not completion_date==None and completion_date==last_closed_date and gtw_status in ['Closed - Done', 'Closed - Cancelled']:
                offload_date = add_business_days(datetime.strptime(completion_date,'%m/%d/%Y'),30).strftime('%m/%d/%Y')
                if today_date >= offload_date:
                    delete_row_id.append(sm_row['Smartsheet_Row_Id'])
                    closed_deviation.append(sm_row['QR_Id'])
           
            if not type(gtw_batch)==str:
                gtw_batch = "N/A"
            if not type(gtw_tafqar)==str:
                gtw_tafqar = "N/A"

            if isinstance(sm_row['Due Date'],datetime):
                sm_due_date = datetime.strptime(sm_row['Due Date'],'%Y-%m-%d').strftime('%m/%d/%Y')
            else:
                sm_due_date = "N/A"

            if gtw_status in ['Closed - Done', 'Closed - Cancelled']:
                gtw_completion_date = last_closed_date
                complete_checkbox = True
            else:
                gtw_completion_date = ''
                complete_checkbox = False

            if sm_row['Status'] != gtw_status:
                update_check = 1
                update_row_cells.extend([
                        {   
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Status"],
                            'value': gtw_status,
                            'strict': True
                        }
                    ])

            if completion_date != gtw_completion_date:
                update_check = 1
                update_row_cells.extend([
                        {   
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Completion Date"],
                            'value': gtw_completion_date,
                            'strict': False
                        },
                        {   
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Finished"],
                            'type': ' CHECKBOX',
                            'value': complete_checkbox,
                            'strict': True
                        },
                        {   
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Finished Date"],
                            'value': gtw_completion_date,
                            'strict': False
                        }
                    ])

            if sm_due_date != gtw_due_date:
                update_check = 1
                update_row_cells.extend([
                        {   
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Due Date"],
                            'value': gtw_due_date,
                            'strict': False
                        }
                    ])
                
                
            if sm_row['Batch'] != gtw_batch:
                update_check = 1
                update_row_cells.extend([
                        {   
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Batch"],
                            'value': gtw_batch,
                            'strict': False
                        },
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Tafqar date"],
                            'value': gtw_tafqar,
                            'strict': False
                        }           
                    ])

            if sm_row['Responsible Department'] != gtw_respo_dept:
                update_check = 1
                update_row_cells.extend([
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Responsible Department"],
                            'value': gtw_respo_dept,
                            'strict': False
                        }
                    ])

            if sm_row['Client'] != gtw_client:
                update_check = 1
                update_row_cells.extend([
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Client"],
                            'value': gtw_client,
                            'strict': False
                        }
                    ])

            if sm_row['Reporting To Name'] != gtw_report_to_name:
                update_check = 1
                update_row_cells.extend([
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Reporting To Name"],
                            'value': gtw_report_to_name,
                            'strict': False
                        }
                    ])

            if sm_row['Reporting To Email'] != gtw_report_to_email:
                update_check = 1
                update_row_cells.extend([
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Reporting To Email"],
                            'value': gtw_report_to_email,
                            'strict': False
                        }
                    ])

            if sm_row['DR Type'] != gtw_dr_type:
                update_check = 1
                update_row_cells.extend([
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["DR Type"],
                            'value': gtw_dr_type,
                            'strict': False
                        }
                    ])

            if sm_row['Short Description'] != gtw_desc:
                update_check = 1
                update_row_cells.extend([
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Short Description"],
                            'value': gtw_desc,
                            'strict': False
                        }
                    ])

            if sm_row['Is Reopened'] != gtw_is_reopened:
                update_check = 1
                update_row_cells.extend([
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Is Reopened"],
                            'value': gtw_is_reopened,
                            'strict': False
                        }
                    ])
                
            if previous_reopen_date!= gtw_reopened_date:
                update_check = 1
                update_row_cells.extend([
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Reopened Date"],
                            'value': gtw_reopened_date,
                            'strict': False
                        }
                    ])

            if sm_row['Current State From Date'] != gtw_current_state_date:
                update_check = 1
                update_row_cells.extend([
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Current State From Date"],
                            'value': gtw_current_state_date,
                            'strict': False
                        }
                    ])

            if sm_row['Criticality'] != gtw_criticality:
                update_check = 1
                update_row_cells.extend([
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Criticality"],
                            'value': gtw_criticality,
                            'strict': False
                        }
                    ])

        elif sm_row['Task_Level'] == 2:
            ### Auto populate task start and finish based on status for Level 2 sub-task
            sub_task_name = sm_row['Task Name']
            print(sub_task_name)
            print(subtask_map)
            completed_on_status = subtask_map[subtask_map.Sub_Task==sub_task_name]['Completed_On_Status'].values[0]
            auto_populate_task = subtask_map[subtask_map.Sub_Task==sub_task_name]['Auto_Populate_Status'].values[0]
            completed_on_status = str(completed_on_status).split(', ')
            subtask_start_date = None
            subtask_end_date = None
            current_subtask_start_date = None
            current_subtask_end_date = None
            checkbox = False

            if gtw_status in completed_on_status:
                first_status = completed_on_status[0]
                first_status_start_date = gtw_status_df[(gtw_status_df.name==first_status)]['date_entry'].min()
                subtask_dates = gtw_status_df[(gtw_status_df.name==auto_populate_task) & (gtw_status_df.date_exit<=first_status_start_date)].copy()
                subtask_dates =  subtask_dates[(subtask_dates.iteration_num==subtask_dates.iteration_num.min())]
               
                #print(qr_id," ",sub_task_name," ",gtw_status," ",first_status," ",auto_populate_task, " ",first_status_start_date)
                #print(gtw_status_df)
                #print(subtask_dates)
                if not subtask_dates.empty:
                    checkbox = True
                    subtask_start_date = datetime.strptime(str(subtask_dates['date_entry'].iat[0]),'%Y-%m-%d %H:%M:%S').strftime('%m/%d/%Y')
                    subtask_end_date = datetime.strptime(str(subtask_dates['date_exit'].iat[-1]),'%Y-%m-%d %H:%M:%S').strftime('%m/%d/%Y')
                    completion_percent = "100%"

            if 'Done or Cancelled' in sub_task_name:
                if gtw_status in ['Closed - Done', 'Closed - Cancelled']:
                    checkbox = True
                    subtask_start_date = gtw_closed_date
                    subtask_end_date = gtw_closed_date
                    completion_percent = "100%"
                else:
                    checkbox = False
                    subtask_start_date = ''
                    subtask_end_date = ''
                    completion_percent = ''

            if date_pattern.search(str(sm_row['Started Date'])):
                current_subtask_start_date = datetime.strptime(str(sm_row['Started Date']),'%Y-%m-%d').strftime('%m/%d/%Y')

            if date_pattern.search(str(sm_row['Finished Date'])):
                current_subtask_end_date = datetime.strptime(str(sm_row['Finished Date']),'%Y-%m-%d').strftime('%m/%d/%Y')

            if subtask_start_date!=None and subtask_end_date!=None:
                if current_subtask_start_date!=subtask_start_date or current_subtask_end_date!=subtask_end_date:
                    update_check = 1
                    update_row_cells.extend([  
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Started Date"],
                            'value': subtask_start_date,
                            'strict': False
                        },
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Finished Date"],
                            'value': subtask_end_date,
                            'strict': False
                        },
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["% Complete"],
                            'value': completion_percent,
                            'strict': False
                        },
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Started"],
                            'type': 'CHECKBOX',
                            'value': checkbox,
                            'strict': True
                        },
                        {
                            'row_id': sm_row['Smartsheet_Row_Id'],
                            'column_id': column_map["Finished"],
                            'type': 'CHECKBOX',
                            'value': checkbox,
                            'strict': True
                        }
                    ])

        ### Update contact for all parent and child rows
        if sm_row['Assigned To'] != gtw_responsible_email and gtw_responsible_email!="N/A":
            update_check = 1
            new_contact = {
                'objectType': 'CONTACT',
                'email': gtw_responsible_email, 
                'name': gtw_responsible_name
            }

            update_row_cells.extend([
                {
                    'row_id': sm_row['Smartsheet_Row_Id'],
                    'column_id': column_map["Assigned To"],
                    'object_value': new_contact,
                    'strict': True
                }
            ])

        if update_check==1:
            updated_deviation.append(qr_id)

    if len(update_row_cells) > 0 :
        update_sheet = sm.update_smartsheet_cell(sheet_id=sheet_id,
                                                update_row_cells=update_row_cells)
    if len(updated_deviation) > 0:
        print(f'{len(updated_deviation)} records updated in smartsheet')

    ## Offload data to SQL database and Delete Record from Smartsheet
    if len(closed_deviation)>0:
        export_df = old_data_df[old_data_df.QR_Id.isin(closed_deviation)]
        save_data_to_sql(dataframe=export_df,sql_engine=kwargs['sql_engine'])
        print(f'Inserted {len(closed_deviation)} closed deviation records into sql database')
    
    if len(delete_row_id)>0: 
        update_sheet = sm.delete_rows_from_sheet(sheet_id=sheet_id,row_ids=delete_row_id)
        print(f'Deleted {len(delete_row_id)} deviation records from smartsheet')

    return(update_sheet)

def run_smartsheet_add_data(**kwargs):

    sm = kwargs['smartsheet']
    sheet_id = kwargs['sheet_id']
    new_data_df = kwargs['new_data_df']
    column_map = kwargs['column_map']
    subtask_map = kwargs['subtask']
    status_sql = kwargs['sql_string']
    oracle_engine = kwargs['oracle_engine']
    predecessor_value = None
    predecessor_type = "FS"
    update_sheet = None

    for parent_index, parent_row in new_data_df.iterrows():

        open_date = parent_row["date_opened"]
        today_date = date.today().strftime("%m/%d/%Y")

        iteration_num = int(parent_row['deviation_iteration_num'])
        ## Get the Status update date based on QR-ID and Iteration Number
        query_sql = status_sql["multi_iteration"]
        query_sql = query_sql.format(QR_ID=parent_row['qr_id'])
        gtw_status_df = pd.read_sql(query_sql,oracle_engine)

        if len(gtw_status_df) == 0:
            query_sql = status_sql["first_iteration"]
            query_sql = query_sql.format(QR_ID=parent_row['qr_id'])
            gtw_status_df = pd.read_sql(query_sql,oracle_engine)

        contact = {'objectType': 'CONTACT',
                   'email': parent_row['responsible_email'], 
                   'name': parent_row['responsible_name']
                  }

        parent_row_cells = [
            {
                'column_id': column_map["Task Name"],
                'value': parent_row['qr_id']
            },
            {
                'column_id': column_map["Duration"],
                'value': "30d"
            },
            {
                'column_id': column_map["GTW Born On Date"],
                'value': open_date,
                'strict': False
            },
            {
                'column_id': column_map["Assigned To"],
                'object_value': contact,
                'strict': True
            },
            {
                'column_id': column_map["Status"],
                'value': parent_row['status'],
                'strict': True
            },
            {
                'column_id': column_map["Started"],
                'type': 'CHECKBOX',
                'value': True,
                'strict': True
            },
            {
                'column_id': column_map["Started Date"],
                'value': today_date,
                'strict': False
            },
            {
                'column_id': column_map["Finished"],
                'type': 'CHECKBOX',
                'value': False,
                'strict': True
            },
            {
                'column_id': column_map["Due Date"],
                'value': parent_row['due_date'],
                'strict': False
            },
            {
                'column_id': column_map["Responsible Department"],
                'value': parent_row['responsible_dept'],
                'strict': False
            },
            {
                'column_id': column_map["Client"],
                'value': parent_row['client'],
                'strict': False
            },
            {
                'column_id': column_map["Reporting To Name"],
                'value': parent_row['reporting_to'],
                'strict': False
            },
            {
                'column_id': column_map["Reporting To Email"],
                'value': parent_row['reporting_to_email'],
                'strict': False
            },
            {
                'column_id': column_map["DR Type"],
                'value': parent_row['dr_type'],
                'strict': False
            },
            {
                'column_id': column_map["Short Description"],
                'value': parent_row['short_description']
            },
            {
                'column_id': column_map["Batch"],
                'value': parent_row['batch']
            },
            {
                'column_id': column_map["Tafqar date"],
                'value': parent_row['tafqar_dt'],
                'strict': False
            },
            {
                'column_id': column_map["Is Reopened"],
                'value': parent_row['deviation_reopened_after_closing']
            },
            {
                'column_id': column_map["Reopened Date"],
                'value': parent_row['reopen_date'],
                'strict': False
            },
            {
                'column_id': column_map["Current State From Date"],
                'value': parent_row['date_current_state'],
                'strict': False
            },
           {
                'column_id': column_map["Criticality"],
                'value': parent_row['criticality'],
                'strict': False
            }

        ]

        ## Add parent rows
        update_sheet = sm.add_row_into_sheet(sheet_id=sheet_id,
                                             parent_row_id=None,
                                             add_cells=parent_row_cells,
                                             to_bottom=True,
                                             add_predecessor=False)

        parent_row_id = update_sheet.rows[-1].id
        subtask_open_date = open_date
        predecessor_value = None
        checkbox = False
        completion_percent = None
        subtask_start_date = None
        subtask_end_date = None

        ## Add child rows
        for child_index, child_row in subtask_map.iterrows():

            status = parent_row['status']
            subtask = child_row['Sub_Task']
            duration = child_row['Duration']
            completed_on_status = str(child_row['Completed_On_Status']).split(', ')
            auto_populate_task = str(child_row['Auto_Populate_Status'])

            child_row_cells = []

            if status in completed_on_status:

                first_status = completed_on_status[0]
                first_status_start_date = gtw_status_df[(gtw_status_df.name==first_status)]['date_entry'].min()
                subtask_dates = gtw_status_df[(gtw_status_df.name==auto_populate_task) & (gtw_status_df.date_exit<=first_status_start_date)].copy()
                subtask_dates =  subtask_dates[(subtask_dates.iteration_num==subtask_dates.iteration_num.min())]

                checkbox = True
                subtask_start_date = datetime.strptime(str(subtask_dates['date_entry'].iat[0]),'%Y-%m-%d %H:%M:%S').strftime('%m/%d/%Y')
                subtask_end_date = datetime.strptime(str(subtask_dates['date_exit'].iat[-1]),'%Y-%m-%d %H:%M:%S').strftime('%m/%d/%Y')
                completion_percent = "100%"

            else:
                checkbox = False
                subtask_start_date = None
                subtask_end_date = None
                completion_percent = None


            ### Add Other values for Child Task Rows
            child_row_cells.extend([
                {
                    'column_id': column_map['Task Name'],
                    'value': subtask
                },
                {
                    'column_id': column_map['Duration'],
                    'value': str(duration)+"d"
                },
                {
                    'column_id': column_map['GTW Born On Date'],
                    'value': subtask_open_date,
                    'strict': False
                },
                {
                    'column_id': column_map['Assigned To'],
                    'object_value': contact,
                    'strict': True
                },
                {
                    'column_id': column_map["Started"],
                    'type': 'CHECKBOX',
                    'value': checkbox,
                    'strict': True
                },
                {
                    'column_id': column_map["Finished"],
                    'type': 'CHECKBOX',
                    'value': checkbox,
                    'strict': True
                }
            ])

            if not subtask_end_date==None:
                child_row_cells.extend([  
                    {
                        'column_id': column_map["Started Date"],
                        'value': subtask_start_date,
                        'strict': False
                    },
                    {
                        'column_id': column_map["Finished Date"],
                        'value': subtask_end_date,
                        'strict': False
                    },
                    {
                        'column_id': column_map["% Complete"],
                        'value': completion_percent,
                        'strict': False
                    }
                ])
                
            update_sheet = sm.add_row_into_sheet(sheet_id=sheet_id,
                                                 parent_row_id=parent_row_id,
                                                 add_cells=child_row_cells,
                                                 to_bottom=True,
                                                 add_predecessor=True,
                                                 predecessor_column_id=column_map['Predecessors'],
                                                 predecessor_value=predecessor_value,
                                                 predecessor_type=predecessor_type)

            predecessor_value = update_sheet.rows[-1].id
            subtask_close_date = add_business_days(datetime.strptime(subtask_open_date,'%m/%d/%Y'),duration-1)
            subtask_open_date = add_business_days(subtask_close_date,1).strftime('%m/%d/%Y')

        #break

    return(update_sheet)



if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--site_code", required=True, help="Provide code for the site, USGRE = Greenville, USALL = Allentown, EUBSL = Basel")
    parser.add_argument("--smartsheet_template", required=True, default="../data/smartsheet_template.csv", help="Template file for auto-populating sub-tasks within Smartsheet")
    parser.add_argument("--credential", required=False, default="../conf/credentials.yml", help="File with database and smartsheet credentials")
    parser.add_argument("--sql_query", required=False,default="../data/sql_query.yml", help="File with SQL queries")
    parser.add_argument("--show_product_complaint", action="store_true", help="If true, only product complaint deviations will be inputed within smartsheet")

    args = parser.parse_args()

    start_time = datetime.now()
    print(f"Started At {start_time}")

    ### Read SQL Credentials
    print("Read Credentials")
    creds = read_credentials(args.credential)

    ### Build SQL connection
    print("Build DISCDEV Database Connection")
    discdev_dbcon,discdev_engine = create_db_connection(creds['DISCDEV'])

    retry_count = 3
    retry_delay = 5

    while not discdev_dbcon and retry_count > 0:
        print("Retrying DISCDEV Database Connection")
        time.sleep(retry_delay)
        discdev_dbcon,discdev_engine = create_db_connection(creds['DISCDEV'])
        retry_count -= 1

    ### Get defined sql queries
    if discdev_dbcon:
        print("Read SQL Queries")
        query_string = read_sql_query(args.sql_query,args.site_code,args.show_product_complaint)
        ### Read GTW SQL data into Dataframe
        print("Read Deviation data from GTW")
        df_sql = pd.read_sql(query_string["GTW"],discdev_dbcon)

        df_sql['date_opened'] = df_sql['date_opened'].dt.strftime('%m/%d/%Y')
        df_sql['date_closed'] = df_sql['date_closed'].dt.strftime('%m/%d/%Y')
        df_sql['reopen_date'] = df_sql['reopen_date'].dt.strftime('%m/%d/%Y')
        df_sql['due_date'] = df_sql['due_date'].dt.strftime('%m/%d/%Y')
        df_sql['date_last_activity'] = df_sql['date_last_activity'].dt.strftime('%m/%d/%Y')
        df_sql['date_current_state'] = df_sql['date_current_state'].dt.strftime('%m/%d/%Y')
        df_sql['responsible_name'] = df_sql.responsible_name.str.title()
        df_sql['responsible_email'] = df_sql.responsible_email.str.lower()
        df_sql['reporting_to'] = df_sql.reporting_to.str.title()
        df_sql['reporting_to_email'] = df_sql.reporting_to_email.str.lower()

        print("Read TAFQAR data from IMOST")
        df_sql_tafqar = pd.read_sql(query_string["IMOST"],discdev_dbcon)
        df_sql_tafqar = df_sql_tafqar.assign(open_dmrs=df_sql_tafqar.open_dmrs.str.split(','))
        df_sql_tafqar = df_sql_tafqar.explode('open_dmrs')
        df_sql_tafqar = df_sql_tafqar[~df_sql_tafqar.open_dmrs.isna()]
        df_sql_tafqar = df_sql_tafqar.astype({"open_dmrs":int})
        df_sql_tafqar = df_sql_tafqar.groupby(by=['open_dmrs'], as_index=False).agg({'m_batch': lambda x: ','.join(x.unique()),
                                                                                     'batch': lambda x: ','.join(x.unique()),
                                                                                     'tafqar_dt': np.min})
        df_sql_tafqar['tafqar_dt'] = df_sql_tafqar['tafqar_dt'].dt.strftime('%m/%d/%Y')

        print("Add Tafqar information into Deviation data")
        df_sql = df_sql.merge(df_sql_tafqar, how='left', left_on=['qr_id'], right_on=['open_dmrs'])

        #discdev_dbcon.close()
    else:
        print("Unable to create connection with DISCDEV Global Track Wise Database")
        sys.exit(1)

    print("Build SITE SQL Database Connection")
    sitesql_dbcon,sitesql_engine = create_db_connection(creds['USGRE_SITE_SQL_GTW_DEVIATION'])

    retry_count = 3
    retry_delay = 5

    while not sitesql_dbcon and retry_count > 0:
        print("Retrying Site SQL Database Connection")
        time.sleep(retry_delay)
        sitesql_dbcon,sitesql_engine = create_db_connection(creds['USGRE_SITE_SQL_GTW_DEVIATION'])
        retry_count -= 1
    
    if not sitesql_dbcon:
        print("Unable to create connection with Site SQL Database")
        sys.exit(1)

    ### Read Smartsheet Template Json File
    print("Read Template for Smartsheet")
    smart_template = read_smartsheet_template(args.smartsheet_template)

    ### Smartsheet API

    print("Initialize Smartsheet API")
    sm = ssa.smartsheet_api(creds['SMARTSHEET'])
    print("Get Smartsheet Id")
    sheet_id = sm.get_sheet_by_name_in_folder()
    print("Map Column Names To Column Id in Smartsheet")
    column_map = sm.get_column_name_id_map(sheet_id=sheet_id)

    ## Get current data in smartsheet
    print("Check for existing data in Smartsheet")
    current_smartsheet_df = pd.DataFrame()
    print("Get Row data From Smartsheet")
    rows = sm.get_rows_from_sheet(sheet_id=sheet_id)
    print("Save Smartsheet data into Dataframe")
    current_smartsheet_df = save_rows_to_df(rows,column_map['id_to_name'])

    df_sql = df_sql.fillna("N/A")
        
    ## Update current data in smartsheet
    print("Update data in Smartsheet based on GTW")
    if not current_smartsheet_df.empty:
        updated_sheet = run_smartsheet_update_data(smartsheet=sm,
                                                   sheet_id=sheet_id,
                                                   old_data_df=current_smartsheet_df,
                                                   new_data_df=df_sql,
                                                   column_map=column_map['name_to_id'],
                                                   subtask=smart_template,
                                                   sql_string=query_string["GTW_STATUS_DATE"],
                                                   oracle_engine=discdev_dbcon,
                                                   sql_engine=sitesql_engine)
        ## Filter to keep only new records
        df_sql_temp = df_sql[~df_sql.qr_id.isin(current_smartsheet_df.QR_Id.unique().tolist())]

        df_sql_temp = df_sql_temp[['qr_id', 'project', 'status', 'short_description', 'responsible_name',
                                     'responsible_email', 'reporting_to', 'reporting_to_email',
                                     'responsible_dept', 'client', 'dr_type', 'due_date', 'date_opened',
                                     'date_closed', 'date_last_activity', 'date_current_state',
                                     'deviation_reopened_after_closing', 'deviation_iteration_num', 'site',
                                     'open_dmrs', 'm_batch', 'batch', 'tafqar_dt', 'reopen_date','criticality']]
        df_sql = df_sql_temp.copy()
        sitesql_dbcon.close()

    df_sql = df_sql[~df_sql.status.isin(['Closed - Done', 'Closed - Cancelled'])]
    df_sql = df_sql[(df_sql.responsible_name!="N/A") & (df_sql.responsible_email!="N/A")]
    #df_sql = df_sql.fillna("N/A")

    ## Add new data to smartsheets
    print("Add New Records in Smartsheet")
    print(f"Adding {len(df_sql)} new deviation records")
    if datetime.strptime(date.today().strftime("%Y-%m-%d"),'%Y-%m-%d') < datetime.strptime('2024-03-19','%Y-%m-%d'):
        smart_template = smart_template[smart_template.Version==1]
    else:
        smart_template = smart_template[smart_template.Version==2]

    df_sql = df_sql.head(10)
    
    updated_sheet = run_smartsheet_add_data(smartsheet=sm,
                                            sheet_id=sheet_id,
                                            new_data_df=df_sql,
                                            column_map=column_map['name_to_id'],
                                            subtask=smart_template,
                                            sql_string=query_string["GTW_STATUS_DATE"],
                                            oracle_engine=discdev_dbcon)

    end_time = datetime.now()
    print(f"Finished At {end_time}")
    total_time = end_time - start_time
    total_time_s = total_time.total_seconds()
    total_time_h = divmod(total_time_s, 60)[0]
    print(f"Completed in {total_time_h} minutes")
    sys.exit(1)