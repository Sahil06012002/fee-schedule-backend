from fastapi import File, UploadFile, Depends, HTTPException
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from db import Database, get_db, engine
from sqlalchemy.sql import text
from utils.helper import add_hash_col

class TableManager:
    def extract_code_colname(self, excel_table):
        hashable_col = None
        for col in excel_table.columns:
            if "code" in col.lower():
                hashable_col = col
        return hashable_col
        
    def calculate_hashable_col(self, df, exclude_col=""):
        max_unique_count = -1
        hashable_col = None
        for col in df.columns:
            if col == exclude_col:
                continue
            if pd.api.types.is_integer_dtype(df[col]):
                continue
            unique_count = df[col].nunique()
            if unique_count > max_unique_count:
                max_unique_count = unique_count
                hashable_col = col
        return hashable_col

    async def insert_table(self,db : Database, file: UploadFile = File(...)) :
        content = await file.read()
        file_name = file.filename
        table_name = file_name.split(".")[0].lower()
        df = pd.read_excel(content)
        hashable_cols = []

        col1 = self.extract_code_colname(df)
        if not col1:
            col1 = self.calculate_hashable_col(df)
        hashable_cols.append(col1)
        hashable_cols.append(self.calculate_hashable_col(df, col1))

        df = add_hash_col(df, hashable_cols)
        df.columns = [col.lower() for col in df.columns]
        send_df = df
        try:
            # with db.begin() as transaction:
                # Create the table
                df.to_sql(table_name, engine, index=False, if_exists='replace')
                print(df)

                # Insert the metadata
                hashable_cols_str = ",".join(hashable_cols)
                query = text("INSERT INTO table_details (table_name, hashable_cols) VALUES (:table_name, :hashable_cols)")
                db.execute(query, {"table_name": table_name, "hashable_cols": hashable_cols_str})
                
                # Commit the transaction
                # transaction.commit()

        except SQLAlchemyError as e:
            # Rollback the transaction in case of an error
            # transaction.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        # send_df.set_index("hash", inplace=True)

        print(send_df)
        return send_df.to_dict(orient='records')
    
    def get_table(self,db,table_id) :
        #return all the table data
        return 

