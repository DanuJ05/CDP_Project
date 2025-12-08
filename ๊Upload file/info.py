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
BATCH_SIZE = 2000

FIXED_SOURCE_VALUE = "CBS"
SORT_DATE_COL = "DATE_KEY" 

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

# ==============================================================================
# PART 3: HELPER FUNCTIONS
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
            entry[mongo_key] = clean_value(raw_val) if raw_val else calculated_status
        else:
            entry[mongo_key] = clean_value(raw_val)
    return entry

# [NEW] ฟังก์ชันหา Active Profile ปัจจุบันใน MongoDB
def get_current_active_profile(mongo_doc):
    if not mongo_doc or 'profile' not in mongo_doc:
        return None
    # หาตัวสุดท้ายที่เป็น Active (ตาม Logic Append)
    for p in reversed(mongo_doc['profile']):
        if p.get('status') == 'Active':
            return p
    return None

# [NEW] ฟังก์ชันเปรียบเทียบข้อมูล (ไม่นับวันที่และสถานะ)
def is_data_identical(new_data, old_data):
    if not old_data: return False
    
    # keys ที่ไม่เอามาเทียบ
    ignored_keys = {'fieldUpdatedAt', 'status', 'lastModified'}
    
    # เทียบทุก key ที่อยู่ใน new_data
    for key, val in new_data.items():
        if key in ignored_keys: continue
        # ถ้าค่าไม่ตรงกัน (convert เป็น str เพื่อความชัวร์ในการเทียบ)
        if str(val) != str(old_data.get(key)):
            return False
            
    return True

# ==============================================================================
# PART 4: MAIN EXECUTION
# ==============================================================================

def run_profile_migration():
    print("🚀 Starting Customer Profile Migration (Smart Update)...")
    
    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

    try:
        with vertica_python.connect(**VERTICA_CONN_INFO) as conn:
            cursor = conn.cursor()

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
            
            # โหลดข้อมูลเข้า Memory ทั้งหมดก่อน (เนื่องจาก Limit 5000 ไม่เยอะมาก)
            # เพื่อให้ง่ายต่อการ Pre-fetch MongoDB
            all_rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Group ข้อมูลตาม ACN
            all_rows.sort(key=lambda x: x['ACN']) # groupby ต้องการ sorted data
            grouped_data = {k: list(v) for k, v in groupby(all_rows, key=lambda x: x['ACN'])}
            
            # Extract CIF List เพื่อไปดึงข้อมูลเก่าจาก MongoDB ทีเดียว
            cif_list = [str(k) for k in grouped_data.keys()]
            
            print(f"🔄 Fetching existing data for {len(cif_list)} CIFs from MongoDB...")
            
            # [Optimization] ดึงข้อมูลเก่ามาเก็บใส่ Dict เพื่อลดการ Query ทีละรอบ
            existing_cursor = collection.find(
                {"cif": {"$in": cif_list}},
                {"cif": 1, "profile": 1}
            )
            existing_docs_map = {doc['cif']: doc for doc in existing_cursor}

            bulk_ops = []
            stats = {"updated_timestamp": 0, "appended_new": 0}

            print("🔄 Comparing and Building Operations...")

            for cif_raw, rows_list in grouped_data.items():
                cif = str(cif_raw)
                
                # เรียงวันที่ล่าสุดขึ้นก่อน
                rows_list.sort(key=lambda r: r.get(SORT_DATE_COL) or datetime.datetime.min, reverse=True)
                
                # เอาเฉพาะ record ล่าสุดจาก Vertica มาเช็ค
                latest_row = rows_list[0]
                new_profile_entry = build_profile_entry(latest_row, "Active")
                
                # หาข้อมูลเก่าใน Map
                old_doc = existing_docs_map.get(cif)
                current_active = get_current_active_profile(old_doc)
                
                # =========================================================
                # LOGIC: Compare -> Decide
                # =========================================================
                
                # CASE 1: ข้อมูลเหมือนเดิมเป๊ะ (Update Timestamp Only)
                if current_active and is_data_identical(new_profile_entry, current_active):
                    
                    new_date = new_profile_entry['fieldUpdatedAt']
                    
                    # สั่ง Update เฉพาะ fieldUpdatedAt ของตัวที่ Active อยู่
                    op_touch = UpdateOne(
                        {"cif": cif},
                        {"$set": {"profile.$[elem].fieldUpdatedAt": new_date}},
                        array_filters=[{"elem.status": "Active"}]
                    )
                    bulk_ops.append(op_touch)
                    stats["updated_timestamp"] += 1
                    
                # CASE 2: ข้อมูลเปลี่ยน หรือ เป็นลูกค้าใหม่ (Deactivate Old -> Push New)
                else:
                    # ถ้ามีของเก่า ต้อง Deactivate ก่อน
                    if current_active:
                        op_deactivate = UpdateOne(
                            {"cif": cif},
                            {"$set": {"profile.$[elem].status": "Inactive"}},
                            array_filters=[{"elem.status": "Active"}]
                        )
                        bulk_ops.append(op_deactivate)

                    # Push ของใหม่
                    op_push = UpdateOne(
                        {"cif": cif},
                        {
                            "$push": { "profile": new_profile_entry },
                             # Optional: Update Last Modified Doc
                            # "$set": { "lastModified": datetime.datetime.now() }
                        },
                        upsert=True
                    )
                    bulk_ops.append(op_push)
                    stats["appended_new"] += 1

                # Execute Bulk Write เป็นระยะๆ
                if len(bulk_ops) >= BATCH_SIZE:
                    collection.bulk_write(bulk_ops, ordered=True)
                    bulk_ops = []
                    print(f"   -> Processed batch...")

            # เก็บตกที่เหลือ
            if bulk_ops:
                collection.bulk_write(bulk_ops, ordered=True)
                
            print(f"\n📊 Summary:")
            print(f"   - Timestamp Updated (Data Unchanged): {stats['updated_timestamp']}")
            print(f"   - New Data Appended (Data Changed/New): {stats['appended_new']}")
                
    except Exception as e:
        print(f"❌ Error Occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        mongo_client.close()
        print(f"\n✅ Job Finished!")

if __name__ == "__main__":
    run_profile_migration()