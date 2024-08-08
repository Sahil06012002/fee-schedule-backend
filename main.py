from typing import Union

from fastapi import FastAPI

app = FastAPI()




@app.get("/getFileData")
def getFileData():
    data = {
      1: { id: 1, "name": "John Doe", "email": "john@example.com" },
      2: { id: 2, "name": "Jane Smith", "email": "jane@example.com" },
      3: { id: 3, "name": "Michael Brown", "email": "michael@example.com" },
    };
    return data