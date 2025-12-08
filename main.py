import asyncio
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic_settings import BaseSettings
from fastapi.middleware.cors import CORSMiddleware

# --- 1. Configuration Management (โหลดค่าจาก .env) ---
class Settings(BaseSettings):
    MONGO_URL: str
    MONGO_DB_NAME: str

    class Config:
        env_file = ".env"  # ระบุชื่อไฟล์ .env

# สร้าง instance ของ settings
settings = Settings()

# --- 2. Setup FastAPI & Database ---
app = FastAPI()

# --- เพิ่มส่วนนี้ต่อจาก app = FastAPI() เลยครับ ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ยอมให้ทุกเว็บยิงเข้ามาได้
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# เชื่อมต่อ Database โดยใช้ค่าจาก settings
client = AsyncIOMotorClient(settings.MONGO_URL)
db = client[settings.MONGO_DB_NAME]

# --- 3. Helper Function ---
def filter_active_status(data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """กรองเอาเฉพาะ status == 'Active'"""
    if not data_list:
        return []
    return [item for item in data_list if item.get("status") == "Active"]

# --- 4. Endpoint ---
@app.get("/search/cif/{cif_id}")
async def get_customer_data(cif_id: str):
    
    # Query ข้อมูลจาก 3 collections พร้อมกัน (Parallel Execution)
    address_task = db.address.find_one({"cif": cif_id})
    info_task = db.info.find_one({"cif": cif_id})
    phonenum_task = db.phonenum.find_one({"cif": cif_id})

    # รอผลลัพธ์ทั้งหมด
    address_doc, info_doc, phonenum_doc = await asyncio.gather(
        address_task, info_task, phonenum_task
    )

    # ตรวจสอบว่ามีข้อมูลหรือไม่ (ถ้าไม่เจอเลยสักที่ ให้ return 404)
    if not address_doc and not info_doc and not phonenum_doc:
        raise HTTPException(status_code=404, detail=f"CIF {cif_id} not found")

    # โครงสร้างผลลัพธ์
    result = {
        "cif": cif_id,
        "info": [],
        "addresses": [],
        "phone_numbers": []
    }

    # --- กรองข้อมูล Active ---
    
    # Collection: info -> field: profile
    if info_doc and "profile" in info_doc:
        result["info"] = filter_active_status(info_doc["profile"])

    # Collection: address -> field: addresses
    if address_doc and "addresses" in address_doc:
        result["addresses"] = filter_active_status(address_doc["addresses"])

    # Collection: phonenum -> field: PhoneNumber
    if phonenum_doc and "PhoneNumber" in phonenum_doc:
        result["phone_numbers"] = filter_active_status(phonenum_doc["PhoneNumber"])

    return result

if __name__ == "__main__":
    import uvicorn
    # รัน server
    uvicorn.run(app, host="0.0.0.0", port=8050)