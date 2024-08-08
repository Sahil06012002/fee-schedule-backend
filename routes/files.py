from fastapi import File, UploadFile, Depends
from fastapi import APIRouter
import psycopg2
import pandas as pd
import json
import numpy as np
from enums import Operations
from utils.helper import dtype_to_postgres,hash_row,add_hash_col,convert_to_python_type

DATABASE = "demo_db"
HOST = "ep-falling-wildflower-a5hi1gh6-pooler.us-east-2.aws.neon.tech"
USER = "demo_db_owner"
PASSWORD = "9GbDkis2zVYX"
PORT = "5432"

conn = psycopg2.connect(
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )


file_router = APIRouter()

@file_router.post("/upload")
async def upload_file(file: UploadFile = File(...)) :
    content = await file.read()
    df = pd.read_excel(content)
    #insert file data into db
    print(df)
    return {"upload": "file"}


@file_router.post("/compare")
async def calculate_dif(cmp_file: UploadFile = File(...)) : 


    #get the file from the db and a excel from user
    
    cur = conn.cursor()
    
    # Load the data from the database table
    cur.execute(f"SELECT * FROM demo")

    old_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    cur.close()
    conn.close()
    print(old_df)

    content = await  cmp_file.read()



    new_df = pd.read_excel(content)

    print(new_df)
    new_df = add_hash_col(new_df)
    print(new_df)
    changes = []
    column_changes = []

    # Set the unique code column as the index for comparison
    old_df.set_index("hash", inplace=True)
    new_df.set_index("hash", inplace=True)

    # Extract unique codes
    old_codes = old_df.index
    new_codes = new_df.index

    # Detect added columns
    added_columns = {
        col: dtype_to_postgres(new_df[col].dtype) for col in new_df.columns if col not in old_df.columns
    }
    if added_columns:
        column_changes.append({
            "type": Operations.ADD.name,
            "columns": json.dumps(added_columns)
        })
    
    # Detect deleted columns
    deleted_columns = {
        col: dtype_to_postgres(old_df[col].dtype) for col in old_df.columns if col not in new_df.columns
    }
    if deleted_columns:
        column_changes.append({
            "type": Operations.DELETE.name,
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
                    new_value = None
                    changes.append({
                        "type": Operations.DELETE.name,
                        "code": code,
                        "column_name": col,
                        "old_value": old_value,
                        "new_value": new_value
                    })
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
    for code in old_codes:
        if code not in new_codes:
            for col in old_df.columns:
                old_value = convert_to_python_type(old_df.at[code, col])
                changes.append({
                    "type": Operations.DELETE.name,
                    "code": code,
                    "column_name": col,
                    "old_value": old_value,
                    "new_val" : None
                })

    return {"changes": changes, "column_changes": column_changes}