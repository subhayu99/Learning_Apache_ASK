from fastapi import FastAPI
from pydantic import BaseModel
from producer import send_data

app = FastAPI(title="Eningo Utility Platform")

class Data(BaseModel):
    system_time: str
    lat: str
    lon: str
    alt: str


@app.post("/data")
def user(request: Data):
    data = request.dict()
    send_data(value=data)
    return {"status": "success"}
