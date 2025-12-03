from pymongo import MongoClient
import json
from decimal import Decimal
from bson import ObjectId # <--- ต้อง Import ตัวนี้เพิ่มครับ
import datetime

# ==========================================
# สร้างตัวแปลงพิเศษ (Encoder) 
# ให้รู้จักทั้ง ObjectId, Decimal และ Date
# ==========================================
class MongoEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj) # <--- ถ้าเจอ ObjectId ให้แปลงเป็น String
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return super(MongoEncoder, self).default(obj)

# ==========================================
# 1. ตั้งค่า Connection
# ==========================================
MONGO_URI = 'mongodb://admin:password@eden206.kube.baac.or.th:27044/'  # <--- อย่าลืมแก้ IP ตรงนี้นะครับ
client = MongoClient(MONGO_URI)
db = client['CDP']
collection = db['address']

# ==========================================
# 2. ดึงข้อมูลและแสดงผล
# ==========================================
try:
    doc = collection.find_one()

    print("\n" + "="*50)
    if doc:
        print("✅ พบข้อมูลใน MongoDB:")
        print("="*50)
        
        # ใช้ cls=MongoEncoder ที่เราสร้างไว้ข้างบน
        print(json.dumps(doc, indent=4, ensure_ascii=False, cls=MongoEncoder))
    else:
        print("❌ ไม่พบข้อมูลใน Collection นี้ (ว่างเปล่า)")
    
    print("="*50 + "\n")

except Exception as e:
    print(f"เกิดข้อผิดพลาด: {e}")

finally:
    client.close()