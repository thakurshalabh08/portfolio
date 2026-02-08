import smartsheet
import time
import sys
from smartsheet.models import Contact
import requests

class smartsheet_api:

    def __init__(self,ss_creds):

        self.api_token = ss_creds['api_token']
        self.ss_client = smartsheet.Smartsheet(self.api_token)
        self.folder_id = None
        self.sheet_id = None
        self.sheet_name = None
        
        if 'folder_id' in ss_creds:
            self.folder_id = ss_creds['folder_id']
        if 'sheet_id' in ss_creds:
            self.sheet_id = ss_creds['sheet_id']
        if 'sheet_name' in ss_creds:
            self.sheet_name = ss_creds['sheet_name']

    def retry(self,func,*args,**kwargs):
        retry_count = 1
        retry_delay = 5

        while True:
            try:
                if len(args)>0 and len(kwargs)==0:
                    response = func(*args)
                    if response.request_response.status_code == requests.codes.ok:
                        return(response)
                elif len(kwargs)>0 and len(args)==0:
                    response = func(**kwargs)
                    if response.request_response.status_code == requests.codes.ok:
                        return(response)
                elif len(args)>0 and len(kwargs)>0:
                    response = func(*args)
                    if response.request_response.status_code == requests.codes.ok:
                        return(response)
                else:
                    response = func()
                    if response.request_response.status_code == requests.codes.ok:
                        return(response)
            except Exception as ex:
                if retry_count <= 5:
                    print(f"Failed Attempt {retry_count}/5")
                    retry_count += 1
                    time.sleep(retry_delay)
                    continue
                else:
                    print(f"Maximum attempts reached, exiting program due to an error {ex}")
                    ## Remove Parent and Child deviation record if error occurs during adding new data row.
                    if 'parent_row_id' in  kwargs:
                        parent_row_id = kwargs['parent_row_id']
                        sheet_id = args[0]
                        if not parent_row_id == None:
                            print("Rolling back due to error - Removing incomplete deviation task")
                            self.retry(self.delete_rows_from_sheet,sheet_id=sheet_id,row_ids=[parent_row_id])
                    sys.exit(1)
            break

    def create_sheet_in_folder(self,**kwargs):
        smartsheet_folder = self.folder_id
        new_sheet_name = kwargs['new_sheet_name']
        new_sheet_template = kwargs['new_sheet_template']

        new_sheet = smartsheet.models.Sheet({
            'name': new_sheet_name,
            'columns': new_sheet_template
        })

        ss_sheet = self.retry(self.ss_client.Folders.create_sheet_in_folder,smartsheet_folder, new_sheet)

        return(ss_sheet)

    def add_column_to_smartsheet(self,**kwargs):
        self.sheet_id=kwargs['sheet_id']
        column_name = kwargs['column_name']
        column_type = kwargs['column_type']
        column_index = kwargs['column_index']

        new_column = smartsheet.models.Column({
            'title': column_name,
            'type': column_type,
            'index': column_index
        })

        ss_sheet = self.retry(self.ss_client.Sheets.add_columns,self.sheet_id, new_column)

        return(ss_sheet)

    
    def get_sheet_by_name_in_folder(self,**kwargs):
        smartsheet_folder = self.folder_id
        if self.sheet_name != None:
            sheet_name = self.sheet_name
        else:
            sheet_name = kwargs['sheet_name']

        folder = self.retry(self.ss_client.Folders.get_folder,smartsheet_folder)
        for sheet in folder.sheets:
            if sheet.name == sheet_name:
                return(sheet.id)

    def get_all_sheets_in_folder(self):
        smartsheet_folder = self.folder_id
        folder = self.retry(self.ss_client.Folders.get_folder,smartsheet_folder)
        sheet_dict={}

        for sheet in folder.sheets:
            sheet_dict[sheet.name] = sheet.id

        return(sheet_dict)

    def get_column_name_id_map(self,**kwargs):

        self.sheet_id = kwargs['sheet_id']
        sheet = self.retry(self.ss_client.Sheets.get_sheet,self.sheet_id)

        column_map= {}
        column_map_name_to_id = {}
        column_map_id_to_name = {}

        for column in sheet.columns:
            column_map_name_to_id[column.title] = column.id
            column_map_id_to_name[column.id] = column.title
        column_map['name_to_id'] = column_map_name_to_id
        column_map['id_to_name'] = column_map_id_to_name

        return(column_map)
    
    def get_rows_from_sheet(self,**kwargs):
        self.sheet_id = kwargs['sheet_id']
        sheet = self.retry(self.ss_client.Sheets.get_sheet,self.sheet_id)
        return(sheet.rows)
    
    def delete_rows_from_sheet(self,**kwargs):
        self.sheet_id = kwargs['sheet_id']
        row_ids = kwargs['row_ids']
        self.retry(self.ss_client.Sheets.delete_rows,self.sheet_id,row_ids)
        sheet = self.retry(self.ss_client.Sheets.get_sheet,self.sheet_id)
        return(sheet)


    def add_cell_to_row(self,cell_dict, new_row):
        cell = smartsheet.models.Cell(cell_dict)
        new_row.cells.append(cell)
        return(new_row)

    def add_row_into_sheet(self,**kwargs):
        self.sheet_id = kwargs['sheet_id']
        sheet = self.retry(self.ss_client.Sheets.get_sheet,self.sheet_id)

        new_row = smartsheet.models.Row()
        new_row.to_bottom = kwargs['to_bottom']
        add_cells = kwargs['add_cells']
        parent_row_id = None

        if not kwargs['parent_row_id']==None:
            new_row.parent_id = kwargs['parent_row_id']
            parent_row_id = kwargs['parent_row_id']
        else:
            new_row.parent_id = None
            parent_row_id = new_row.id

        for cell_dict in add_cells:
            new_row = self.add_cell_to_row(cell_dict,new_row)
        
        if kwargs['add_predecessor']==True:
            if kwargs['predecessor_value']!=None:
                pred = smartsheet.models.Predecessor()
                pred.row_id = kwargs['predecessor_value']
                pred.type = kwargs['predecessor_type']

                pred_list = smartsheet.models.PredecessorList()
                pred_list.predecessors.append(pred)

                pred_cell_dict = {
                    'column_id': kwargs['predecessor_column_id']
                    ,'object.value.object_type': 'PREDECESSOR_LIST'
                    ,'object_value': pred_list
                }

                new_row = self.add_cell_to_row(pred_cell_dict,new_row)

        self.retry(self.ss_client.Sheets.add_rows,self.sheet_id, new_row, parent_row_id = parent_row_id)
        sheet = self.retry(self.ss_client.Sheets.get_sheet,self.sheet_id, parent_row_id = parent_row_id)

        return(sheet)


    def update_smartsheet_cell(self,**kwargs):
        self.sheet_id = kwargs['sheet_id']
        row = smartsheet.models.Row()
        row_id = None
        update_row = []

        for cell_param in kwargs['update_row_cells']:
            if cell_param['row_id'] != row_id:
                if row_id!=None:
                    update_row.append(row)
                row = smartsheet.models.Row()
                row.id = cell_param['row_id']
                row_id = row.id

            update_cell = smartsheet.models.Cell(cell_param)
            row.cells.append(update_cell)
        
        update_row.append(row)

        self.retry(self.ss_client.Sheets.update_rows,self.sheet_id,update_row)
        sheet = self.retry(self.ss_client.Sheets.get_sheet,self.sheet_id)
       
        return(sheet)
    

    def set_smartsheet_column_type(self,**kwargs):
        
        new_smartsheet_column = []
        smartsheet_column_type = {}
        set_primary_column=1

        df_sql = kwargs['df_sql']

        for column in df_sql.columns:
            if df_sql[column].dtype.name =='datetime64[ns]':
                new_smartsheet_column.append(smartsheet.models.Column({
                    'title': column,
                    'type': 'DATE'
                }))
                df_sql[column] = df_sql[column].dt.strftime('%m/%d/%Y')
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

        return(new_smartsheet_column,smartsheet_column_type)






    
    
