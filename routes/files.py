from fastapi import File, UploadFile, Depends
from fastapi import APIRouter
from services.comparision import Comparision
from services.table_manager import TableManager

from db import Database, get_db, engine




file_router = APIRouter()

@file_router.post("/upload")
async def upload_file(file: UploadFile = File(...), table_manager_obj: TableManager = Depends(TableManager),db : Database = Depends(get_db)):
    data,id = await table_manager_obj.insert_table(db,file)
    return {"data" : data, "table_id" : id}


@file_router.post("/compare/{table_name}")
async def calculate_dif(table_name: str ,cmp_file: UploadFile = File(...),cmp_obj : Comparision = Depends(Comparision),db : Database = Depends(get_db)) : 
    print(table_name)
    response = await cmp_obj.compare(db,table_name,cmp_file)
    return {"data" : response}