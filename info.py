import vertica_python
from pymongo import MongoClient
from itertools import groupby
import datetime
from decimal import Decimal
import json
import re  # <--- เพิ่มบรรทัดนี้ครับ
# ==============================================================================
# PART 1: CONFIGURATION
# ==============================================================================
VERTICA_CONN_INFO = {
    'host': '172.26.133.65',
    'port': 5433,
    'user': 'P6600566',
    'password': 'P@6600566',
    'database': 'BAACDWH',
    'read_timeout': 600,
    'tlsmode': 'disable'
}

MONGO_URI = 'mongodb://admin:password@eden206.kube.baac.or.th:27044/'
MONGO_DB = 'CDP'
MONGO_COLLECTION = 'info'

BATCH_SIZE = 5000

import vertica_python
from pymongo import MongoClient, UpdateOne
from itertools import groupby
import datetime
from decimal import Decimal
import json
import re

# ==============================================================================
# PART 1: CONFIGURATION
# ==============================================================================
VERTICA_CONN_INFO = {
    'host': '172.26.133.65',
    'port': 5433,
    'user': 'P6600566',
    'password': 'P@6600566',
    'database': 'BAACDWH',
    'read_timeout': 600,
    'tlsmode': 'disable'
}

MONGO_URI = 'mongodb://admin:password@eden206.kube.baac.or.th:27044/'
MONGO_DB = 'CDP'
MONGO_COLLECTION = 'info'
BATCH_SIZE = 2000 # ลดขนาดลงนิดหน่อยเพราะ 1 CIF จะสร้าง 2 Operations

FIXED_SOURCE_VALUE = "CBS"

# ==============================================================================
# PART 2: MAPPING CONFIGURATION
# ==============================================================================
PROFILE_MAPPING = {
    "ACN": "cif",
    "ZTITLE": "title",
    "FNAME": "firstName",
    "LNM": "lastName",
    "ZCIZID": "nationalId",
    "SEX": "gender",
    "NATION": "nationality",
    "ZKTBCCODE": "customerTypeCode",
    "DOB": "birthDate",
    "DOD": "deathDate",
    "MAR": "maritalStatus",
    "ZSPOUSETITLE": "spouseTitle",
    "ZSPOUSENM": "spouseName",
    "ZSPOUSELNM": "spouseLastName",
    "ZSPOUSEID": "spouseID",
    "DATE_KEY": "fieldUpdatedAt",
    "REC_STATUS": "status"
}

# [สำคัญ] แก้ชื่อ Column วันที่สำหรับ Sort ให้ตรงกับ Query SQL ด้านล่าง
SORT_DATE_COL = "DATE_KEY" 

# ==============================================================================
# PART 3: HELPER FUNCTIONS (คงเดิม)
# ==============================================================================

def sanitize_text(val):
    if val is None: return None
    val_str = str(val).strip()
    if val_str == "": return None
    cleaned_chars = [c for c in val_str if (0x0e00 <= ord(c) <= 0x0e7f) or (32 <= ord(c) <= 126)]
    result = "".join(cleaned_chars).strip()
    return result if result else None

def format_date_iso(dt):
    if isinstance(dt, (datetime.date, datetime.datetime)):
        return dt.isoformat()
    return None

def format_date_simple(dt):
    if isinstance(dt, datetime.datetime):
        return dt.date().isoformat()
    elif isinstance(dt, datetime.date):
        return dt.isoformat()
    return str(dt) if dt else None

def clean_value(val):
    if isinstance(val, Decimal):
        return int(val) if val % 1 == 0 else float(val)
    if isinstance(val, str):
        return sanitize_text(val)
    return val

def build_profile_entry(row, calculated_status):
    entry = { "source": FIXED_SOURCE_VALUE }
    for vertica_col, mongo_key in PROFILE_MAPPING.items():
        if vertica_col == "ACN": continue 
        raw_val = row.get(vertica_col)
        
        if mongo_key in ["birthDate", "deathDate"]:
            entry[mongo_key] = format_date_simple(raw_val)
        elif mongo_key == "fieldUpdatedAt":
            entry[mongo_key] = format_date_iso(raw_val)
        elif mongo_key == "status":
            # ถ้ามีค่า status มาจาก Vertica ให้ใช้ ถ้าไม่มีให้ใช้ที่คำนวณ
            entry[mongo_key] = clean_value(raw_val) if raw_val else calculated_status
        else:
            entry[mongo_key] = clean_value(raw_val)
    return entry

# ==============================================================================
# PART 4: MAIN EXECUTION (แก้ไข Logic Update)
# ==============================================================================

def run_profile_migration():
    print("🚀 Starting Customer Profile Migration (Upsert Mode)...")
    
    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

    try:
        with vertica_python.connect(**VERTICA_CONN_INFO) as conn:
            cursor = conn.cursor()

            # Query ข้อมูล
            query = """
            SELECT 
            ACN,ZCIZID,ZTITLE,FNAME,LNM,ZETITLE,ZEFNAME,ZELNM,
            SEX,DOB,DOD,NATION,MAR,ZKTBCCODE,
            ZSPOUSEID,ZSPOUSETITLE,ZSPOUSENM,ZSPOUSELNM,
            DATE_KEY
            FROM DA_PROD.cleansing_TB_CBS_CIF_20251130
            WHERE ACN > 2000000
            ORDER BY ACN 
            LIMIT 5000 
            """
            
            print("⏳ Executing SQL Query...")
            cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            
            def row_generator():
                while True:
                    row = cursor.fetchone()
                    if not row: break
                    yield dict(zip(columns, row))

            grouped_stream = groupby(row_generator(), key=lambda x: x['ACN'])
            
            bulk_ops = []
            total_processed = 0

            print("🔄 Processing Data & Building Bulk Operations...")

            for cif, group in grouped_stream:
                rows_list = list(group)
                
                # 1. เรียงลำดับข้อมูลใน Batch เดียวกัน (เอาวันที่ล่าสุดขึ้นก่อน)
                rows_list.sort(key=lambda r: r.get(SORT_DATE_COL) or datetime.datetime.min, reverse=True)
                
                new_profiles = []
                # 2. เตรียมข้อมูล Profile
                for i, row in enumerate(rows_list):
                    # ตัวแรกสุดของ Batch ให้เป็น Active, ตัวรองลงมา (ถ้ามีใน batch เดียวกัน) ให้ Inactive
                    current_status = "Active" if i == 0 else "Inactive"
                    p_entry = build_profile_entry(row, current_status)
                    new_profiles.append(p_entry)

                if not new_profiles:
                    continue

                # =========================================================
                # LOGIC สำคัญ: Deactivate Old -> Push New
                # =========================================================
                
                # Step 1: สั่ง Inactive ข้อมูลเก่า'ทั้งหมด'ที่มีอยู่ใน MongoDB Array
                # เราใช้ $[all] operator เพื่อ update ทุก element ใน array profile
                op_deactivate = UpdateOne(
                    {"cif": str(cif)},
                    {"$set": {"profile.$[].status": "Inactive"}}
                    # หมายเหตุ: ถ้า cif ไม่มีอยู่จริง คำสั่งนี้จะไม่ทำอะไร (Matched 0)
                )
                bulk_ops.append(op_deactivate)

                # Step 2: Push ข้อมูลใหม่ (Active) เข้าไปต่อท้าย
                op_push = UpdateOne(
                    {"cif": str(cif)},
                    {
                        "$push": {
                            "profile": {"$each": new_profiles}
                        },
                        # Optional: อัปเดต timestamp ที่ระดับ document หลักว่าแก้ไขเมื่อไหร่
                        # "$set": {"lastModified": datetime.datetime.now()}
                    },
                    upsert=True # สำคัญ: ถ้าไม่มี CIF นี้ ให้สร้างใหม่เลย
                )
                bulk_ops.append(op_push)

                # =========================================================

                # Execute Bulk Write เมื่อถึงขนาดที่กำหนด
                if len(bulk_ops) >= BATCH_SIZE:
                    # ordered=True สำคัญมาก! เพื่อรับประกันว่า Deactivate ทำงานก่อน Push เสมอ
                    collection.bulk_write(bulk_ops, ordered=True)
                    total_processed += (len(bulk_ops) // 2) # หาร 2 เพราะ 1 cif = 2 ops
                    print(f"   -> Synced {total_processed} CIFs...")
                    bulk_ops = []

            # Execute ส่วนที่เหลือ
            if bulk_ops:
                collection.bulk_write(bulk_ops, ordered=True)
                total_processed += (len(bulk_ops) // 2)
                print(f"   -> Synced remaining CIFs.")
                
    except Exception as e:
        print(f"❌ Error Occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        mongo_client.close()
        print(f"\n✅ Job Finished! Total CIFs processed: {total_processed}")

if __name__ == "__main__":
    run_profile_migration()