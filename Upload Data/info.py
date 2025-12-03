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

# กำหนดค่า Source ตายตัวที่นี่ (ไม่ต้องดึงจาก DB)
FIXED_SOURCE_VALUE = "CBS"

# ==============================================================================
# PART 2: MAPPING CONFIGURATION
# ==============================================================================
# ฝั่งซ้าย: Column Vertica
# ฝั่งขวา: Key ใน MongoDB
# หมายเหตุ: ตัด SRC_STM ออก เพราะจะใช้ค่า Fixed แทน
PROFILE_MAPPING = {
    "ACN": "cif",            # Key นี้ใช้สำหรับ Grouping
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

# Column ที่ใช้ Sort หา record ล่าสุด
SORT_DATE_COL = "LAST_UPD" 

# ==============================================================================
# PART 3: HELPER FUNCTIONS
# ==============================================================================

def sanitize_text(val):
    """
    *** พระเอกของเรา: ตัวกรองขยะ ***
    หน้าที่: รับค่ามา แล้วคัดเอาเฉพาะภาษาไทย อังกฤษ และตัวเลข
    ทิ้งตัวอักษรจีน หรือสัญลักษณ์ต่างดาว
    """
    if val is None:
        return None
    
    # แปลงเป็น String และตัดช่องว่างซ้ายขวาก่อน
    val_str = str(val).strip()
    if val_str == "":
        return None

    # Logic: วนลูปเช็คทีละตัวอักษร
    # เก็บเฉพาะ:
    # 1. ภาษาไทย (\u0e00 - \u0e7f)
    # 2. ASCII มาตรฐาน (ตัวเลข, อังกฤษ, เครื่องหมายวรรคตอน) (Code 32-126)
    cleaned_chars = [
        c for c in val_str 
        if (0x0e00 <= ord(c) <= 0x0e7f) or (32 <= ord(c) <= 126)
    ]
    
    # รวมกลับเป็นข้อความ
    result = "".join(cleaned_chars).strip()
    
    # ถ้ากรองแล้วไม่เหลืออะไรเลย (เช่น เดิมเป็นภาษาจีนล้วน) ให้ส่งกลับเป็น None
    return result if result else None

def format_date_iso(dt):
    """สำหรับ fieldUpdatedAt"""
    if isinstance(dt, (datetime.date, datetime.datetime)):
        return dt.isoformat()
    return None

def format_date_simple(dt):
    """สำหรับ birthDate/deathDate"""
    if isinstance(dt, datetime.datetime):
        return dt.date().isoformat()
    elif isinstance(dt, datetime.date):
        return dt.isoformat()
    return str(dt) if dt else None

def clean_value(val):
    """
    ตัวกลางจัดการข้อมูล:
    1. ถ้าเป็นตัวเลข Decimal -> แปลงเป็น int/float
    2. ถ้าเป็น String -> ส่งให้พระเอก sanitize_text จัดการ
    """
    # จัดการ Decimal จาก Database
    if isinstance(val, Decimal):
        return int(val) if val % 1 == 0 else float(val)
    
    # จัดการ String ผ่านพระเอกของเรา
    if isinstance(val, str):
        return sanitize_text(val)
        
    return val

def build_profile_entry(row, calculated_status):
    """สร้าง Dict สำหรับ Profile"""
    entry = {
        "source": FIXED_SOURCE_VALUE
    }
    
    for vertica_col, mongo_key in PROFILE_MAPPING.items():
        if vertica_col == "ACN": continue 
        
        raw_val = row.get(vertica_col)
        
        if mongo_key in ["birthDate", "deathDate"]:
            entry[mongo_key] = format_date_simple(raw_val)
            
        elif mongo_key == "fieldUpdatedAt":
            entry[mongo_key] = format_date_iso(raw_val)
            
        elif mongo_key == "status":
            entry[mongo_key] = clean_value(raw_val) if raw_val else calculated_status
            
        else:
            # ส่งข้อมูลเข้า clean_value (ซึ่งจะเรียก sanitize_text ต่อ)
            entry[mongo_key] = clean_value(raw_val)

    return entry
# ==============================================================================
# PART 4: MAIN EXECUTION
# ==============================================================================

def run_profile_migration():
    print("🚀 Starting Customer Profile Migration...")
    
    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

    try:
        with vertica_python.connect(**VERTICA_CONN_INFO) as conn:
            cursor = conn.cursor()

            # --- DYNAMIC SQL QUERY ---
            # สร้าง SELECT list จาก Mapping โดยอัตโนมัติ
            cols_to_select = list(PROFILE_MAPPING.keys())
            
            # อย่าลืมใส่ Schema.Table ให้ถูกต้อง
            query = """
            SELECT 
            ACN,ZCIZID,ZTITLE,FNAME,LNM,ZETITLE,ZEFNAME,ZELNM,
            SEX,DOB,DOD,NATION,MAR,ZKTBCCODE,
            ZSPOUSEID,ZSPOUSETITLE,ZSPOUSENM,ZSPOUSELNM,
            DATE_KEY
              
            FROM DA_PROD.cleansing_TB_CBS_CIF_20251031
            WHERE ACN > 1500000
            ORDER BY ACN 
            LIMIT 50000 
            """
            
            print("⏳ Executing SQL Query...")
            # print(query) 
            cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            
            def row_generator():
                while True:
                    row = cursor.fetchone()
                    if not row: break
                    yield dict(zip(columns, row))

            # Grouping by ACN
            grouped_stream = groupby(row_generator(), key=lambda x: x['ACN'])
            
            batch_docs = []
            total_inserted = 0

            print("🔄 Processing Data...")

            for cif, group in grouped_stream:
                rows_list = list(group)
                
                # Sort ข้อมูลตามวันที่อัปเดต (ใหม่สุดขึ้นก่อน) เพื่อหา Active
                rows_list.sort(key=lambda r: r.get(SORT_DATE_COL) or datetime.datetime.min, reverse=True)
                
                profile_list = []
                for i, row in enumerate(rows_list):
                    # Record แรกถือเป็น Active
                    current_status = "Active" if i == 0 else "Inactive"
                    p_entry = build_profile_entry(row, current_status)
                    profile_list.append(p_entry)

                # สร้าง Document ตามโครงสร้างที่ต้องการ
                document = {
                    "cif": str(cif),
                    "profile": profile_list
                }
                
                batch_docs.append(document)
                
                if len(batch_docs) >= BATCH_SIZE:
                    collection.insert_many(batch_docs)
                    total_inserted += len(batch_docs)
                    print(f"   -> Inserted {len(batch_docs)} Profiles (Total: {total_inserted})")
                    batch_docs = []

            if batch_docs:
                collection.insert_many(batch_docs)
                total_inserted += len(batch_docs)
                print(f"   -> Inserted remaining {len(batch_docs)} Profiles.")
                
    except Exception as e:
        print(f"❌ Error Occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        mongo_client.close()
        print(f"\n✅ Job Finished! Total processed: {total_inserted}")

if __name__ == "__main__":
    run_profile_migration()