from fastapi import File, UploadFile, Depends
from fastapi import APIRouter
from services.comparision import Comparision
from services.table_manager import TableManager

from db import Database, get_db, engine




file_router = APIRouter()
@file_router.get("/")
def get_all_uploaded_files(db : Database = Depends(get_db),table_manager_obj: TableManager = Depends(TableManager)) : 
    response = table_manager_obj.get_all_files(db)
    return {"data" : response}


@file_router.get("/file-data/{table_name}")
async def get_file_data(table_name : str,db : Database = Depends(get_db),cmp_obj : Comparision = Depends(Comparision)) :
    table_data  = cmp_obj.get_table_data(table_name,db)
    return {"data" : table_data}


@file_router.post("/upload",summary="Upload an XLSX File and Store Data in Database",
    description=(
        "Uploads an XLSX file, extracts data from it, and stores the extracted data "
        "in the database. The file should be in XLSX format, and the data will be inserted "
        "into a database table managed by the TableManager."
    ))
async def upload_file(file: UploadFile = File(...), table_manager_obj: TableManager = Depends(TableManager),db : Database = Depends(get_db)):
    data,table_name,file_name = await table_manager_obj.insert_table(db,file)
    return {"data" : data, "table_name" : table_name, "file_name" : file_name}


@file_router.post("/compare/{table_name}",  summary="Compare XLSX File with Existing Table Data",
    description=(
        "Uploads an XLSX file containing data to be compared with an existing table "
        "in the database. The file should have a similar structure to the data in the "
        "specified table. The endpoint compares the new file with the existing data and "
        "returns the differences or changes found."
    ))
async def calculate_dif(table_name: str ,cmp_file: UploadFile = File(...),cmp_obj : Comparision = Depends(Comparision),db : Database = Depends(get_db)) : 
    print(table_name)
    response = await cmp_obj.compare(db,table_name,cmp_file)
    return {"data" : response}


