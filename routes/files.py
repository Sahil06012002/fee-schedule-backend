from fastapi import File, UploadFile, Depends
from fastapi import APIRouter
import psycopg2
import pandas as pd
import json
import numpy as np
from enums import Operations,Axis
from utils.helper import dtype_to_postgres,add_hash_col,convert_to_python_type
from services.table_manager import TableManager

from db import Database, get_db, engine




file_router = APIRouter()

@file_router.post("/upload")
async def upload_file(file: UploadFile = File(...), table_manager_obj: TableManager = Depends(TableManager),db : Database = Depends(get_db)):
    response = await table_manager_obj.insert_table(db,file)
    return {"data" : response}


@file_router.post("/compare")
async def calculate_dif(table_id: int ,cmp_file: UploadFile = File(...)) : 
    #get the file from the db and a excel from user 
    cur = conn.cursor()
    
    # Load the data from the database table
    cur.execute(f"SELECT * FROM demo")

    old_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    

    cur.close()
    conn.close()

    content = await  cmp_file.read()



    new_df = pd.read_excel(content)

    new_df = add_hash_col(new_df)
    changes = []
    table_changes = []

    # Set the unique code column as the index for comparison
    old_df.set_index("hash", inplace=True)
    new_df.set_index("hash", inplace=True)


    original_table = old_df
    res_tab = original_table.to_dict()
    # Extract unique codes
    old_codes = old_df.index
    new_codes = new_df.index

    # Detect added columns
    added_columns = {
        col: dtype_to_postgres(new_df[col].dtype) for col in new_df.columns if col not in old_df.columns
    }
    if added_columns:
        table_changes.append({
            "type" : Axis.COLUMN.name,
            "operation": Operations.ADD.name,
            "columns": json.dumps(added_columns)
        })
    
    # Detect deleted columns
    deleted_columns = {
        col: dtype_to_postgres(old_df[col].dtype) for col in old_df.columns if col not in new_df.columns
    }
    if deleted_columns:
        table_changes.append({
            "type" : Axis.COLUMN.name,
            "operation": Operations.DELETE.name,
            "columns": json.dumps(deleted_columns)
        })
    
    # Detect modifications at the cell level
    # old_code = list of hash val of existing table
    for code in old_codes:
        if code in new_codes:
            old_row = old_df.loc[code]
            new_row = new_df.loc[code]
            
            # before comparing make a check on the new_df ,if the col is present in  new_df, if not(col is deleted) new_val would be None
            for col in old_row.index:

                old_value = convert_to_python_type(old_row[col])
                if col not in new_row.index:
                    #this means col is deleted, 
                    continue 
                    # new_value = None
                    # changes.append({
                    #     "type": Operations.DELETE.name,
                    #     "code": code,
                    #     "column_name": col,
                    #     "old_value": old_value,
                    #     "new_value": new_value
                    # })
                else : 
                    new_value = convert_to_python_type(new_row[col])
                    if old_value != new_value:
                        changes.append({
                            "type": Operations.UPDATE.name,
                            "code": code,
                            "column_name": col,
                            "old_value": old_value,
                            "new_value": new_value
                        })

            #this iteration is for the cols that are present in the new df but are not in the old df(cols are added)
            for col in new_row.index:
                if col not in old_row.index:
                    new_value = convert_to_python_type(new_row[col])
                    changes.append({
                        "type": Operations.ADD.name,
                        "code": code,
                        "column_name": col,
                        "old_value": None,  # No old value since the column is new
                        "new_value": new_value
                    })


    # Detect added rows (added codes)
    for code in new_codes:
        if code not in old_codes:
            for col in new_df.columns:
                new_value = convert_to_python_type(new_df.at[code, col])
                changes.append({
                    "type": Operations.ADD.name,
                    "code": code,
                    "column_name": col,
                    "old_value" : None,
                    "new_value": new_value
                })

    # Detect deleted rows (deleted codes)
    deleted_rows = {}
    for code in old_codes:
        if code not in new_codes:
            #add all the hashes with type as empty and then store in table_changes
            deleted_rows[code] = ""
    table_changes.append({
            "type" : Axis.ROW.name,
            "operation": Operations.DELETE.name,
            "columns": json.dumps(deleted_rows)
            })    
            # for col in old_df.columns:
            #     old_value = convert_to_python_type(old_df.at[code, col])
            #     changes.append({
            #         "type": Operations.DELETE.name,
            #         "code": code,
            #         "column_name": col,
            #         "old_value": old_value,
            #         "new_val" : None
            #     })


    
    return {"original_table" : res_tab ,"changes": changes, "column_changes": table_changes}