from fastapi import File, UploadFile
from sqlalchemy import text
from db import Database
class Comparision : 
    def compare(db: Database,table_name: String , file : UploadFile) :
        #get the table name corresponding 
        query = text("SELECT * FROM :table_name")
        db.execute(query,{"table_name" : table_name})
        return 

