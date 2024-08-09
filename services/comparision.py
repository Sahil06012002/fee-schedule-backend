import json
from fastapi import File, HTTPException, UploadFile
import pandas as pd
from sqlalchemy import text
from db import Database
from enums import Axis, Operations
from utils.helper import add_hash_col, convert_to_python_type, dtype_to_postgres
class Comparision : 
    def fetch_table_from_db(self,table_name : str,db :Database) :
        query = text(f"SELECT * FROM {table_name}")
        try :
            result = db.execute(query)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        rows = result.fetchall()

        columns = result.keys()  # Retrieve column names from the result

        df = pd.DataFrame(rows, columns=columns)
        return df
    
    async def compare(self,db: Database,table_name: str , cmp_file : UploadFile) :
        old_df = self.fetch_table_from_db(table_name ,db)

        print(old_df)

        content = await  cmp_file.read()
        new_df = pd.read_excel(content)
        #new df doesnt have hash column we have to retrieve the hash col from the table meta data add apply hash col
        hashable_cols = []
        query = text("SELECT hashable_cols FROM table_details WHERE table_name = :table_name")
        result = db.execute(query,{"table_name": table_name})

        query_data = result.fetchone()

        hashable_cols = query_data[0].split(",")
        print(hashable_cols)
 
        new_df = add_hash_col(new_df,hashable_cols)
        print(new_df)

        changes = []
        table_changes = []

        # Set the unique code column as the index for comparison
        new_df.set_index("hash", inplace=True)
        old_df.set_index("hash", inplace=True)
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
                "values": json.dumps(added_columns)
            })
        
        # Detect deleted columns
        deleted_columns = {
            col: dtype_to_postgres(old_df[col].dtype) for col in old_df.columns if col not in new_df.columns
        }
        if deleted_columns:
            table_changes.append({
                "type" : Axis.COLUMN.name,
                "operation": Operations.DELETE.name,
                "values": json.dumps(deleted_columns)
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
                                "row_name": code,
                                "column_name": col,
                                "old_value": old_value,
                                "new_value": new_value
                            })

                #this iteration is for the cols that are present in the new df but are not in the old df(cols are added)
                for col in new_row.index:
                    if col not in old_row.index:
                        new_value = convert_to_python_type(new_row[col])
                        changes.append({
                            "type": Operations.UPDATE.name,  #here we can say that the data is updated
                            "row_name": code,
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
                        "row_name": code,
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
                "values": json.dumps(deleted_rows)
                })    



        print(table_changes)
        return {"cell_changes": changes, "column_changes": table_changes}
 

