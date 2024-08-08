from typing import Union
from fastapi import FastAPI
from routes import file_router 

app = FastAPI()
app.include_router(file_router, prefix="/files")


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}