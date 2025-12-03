import vertica_python
from pymongo import MongoClient
from itertools import groupby
import datetime
from decimal import Decimal
import json

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
MONGO_COLLECTION = 'address'

BATCH_SIZE = 5000
MASTER_DATE_COL = "row_updated_at" 

# ==============================================================================
# PART 2: MAPPING CONFIGURATION
# ==============================================================================
ADDRESS_MAPPINGS = [
    {
        "source": "CBS",
        "category": "CARDID",
        "cols": {
            "houseNo": "card_houseno", 
            "villageNo": "PMOO",
            "road": "card_road",       
            "subdistrict": "ZPSDISCD",
            "district": "PCITY",
            "province": "PSTATE",
            "postalCode": "PZIP",
            "country": "PCNTRY",
            "updatedAt": "row_updated_at"
        }
    },
    {
        "source": "CBS",
        "category": "HOME",
        "cols": {
            "houseNo": "home_houseno",
            "villageNo": "MMOO",
            "road": "home_road",
            "subdistrict": "ZMSDISCD",
            "district": "MCITY",
            "province": "MSTATE",
            "postalCode": "MZIP",
            "country": "MCNTRY",
            "updatedAt": "row_updated_at"
        }
    },
    {
        "source": "CBS",
        "category": "WORK",
        "cols": {
            "houseNo": "work_houseno",
            "villageNo": "ZOMOO",
            "road": "work_road",
            "subdistrict": "ZOSDISCD",
            "district": "ZOCITY",
            "province": "ZOSTATE",
            "postalCode": "ZOZIP",
            "country": "ZOCNTRY",
            "updatedAt": "row_updated_at"
        }
    }
]

# ==============================================================================
# PART 3: HELPER FUNCTIONS (ใส่ตัวกรองขยะที่นี่)
# ==============================================================================

def format_date(dt):
    if isinstance(dt, (datetime.date, datetime.datetime)):
        return dt.isoformat()
    return None

def clean_decimal(val):
    if isinstance(val, Decimal):
        return int(val) if val % 1 == 0 else float(val)
    return val

def sanitize_text(val):
    """
    *** พระเอกของเรา: ตัวกรองขยะ ***
    หน้าที่: รับค่ามา แล้วคัดเอาเฉพาะภาษาไทย อังกฤษ และตัวเลข
    ทิ้งตัวอักษรจีน หรือสัญลักษณ์ต่างดาว
    """
    if val is None:
        return None
    
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
    
    return result if result else None

def build_address_object(row, rule, calculated_status):
    cols = rule['cols']
    
    # 1. ดึงบ้านเลขที่และกรองขยะทันที
    raw_houseno = row.get(cols.get('houseNo'))
    final_houseno = sanitize_text(raw_houseno)
    
    # ถ้ากรองแล้วไม่เหลืออะไรเลย (เช่นมีแต่ขยะจีน) ก็ไม่ต้องบันทึก
    if not final_houseno: 
        return None

    # 2. สร้าง Object และกรองขยะทุก field ที่เป็น Text
    return {
        "source": rule['source'],
        "category": rule['category'],
        "fieldUpdatedAt": format_date(row.get(cols.get('updatedAt'))),
        
        "houseNo": final_houseno,
        "villageNo": clean_decimal(row.get(cols.get('villageNo'))), 
        
        "road": sanitize_text(row.get(cols.get('road'))),
        "subdistrictKhwaeng": sanitize_text(row.get(cols.get('subdistrict'))),
        "districtKhet": sanitize_text(row.get(cols.get('district'))),
        "province": sanitize_text(row.get(cols.get('province'))),
        "postalCode": sanitize_text(row.get(cols.get('postalCode'))),
        "country": sanitize_text(row.get(cols.get('country'))) or "ไทย",
        
        "status": calculated_status
    }

# ==============================================================================
# PART 4: MAIN EXECUTION
# ==============================================================================

def run_full_migration():
    print("🚀 Starting Migration Process...")
    
    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

    try:
        with vertica_python.connect(**VERTICA_CONN_INFO) as conn:
            cursor = conn.cursor()

            # --- SQL QUERY ---
            query = """
            SELECT 
                ACN,
                DATE_KEY AS row_updated_at,

                -- [1. CARDID]
                TRIM(NVL(CAST(PAD1 AS VARCHAR), '') || ' ' || NVL(CAST(PAD2 AS VARCHAR), '')) AS card_houseno,
                PAD3 AS card_road,
                PMOO, ZPSDISCD, PCITY, PSTATE, PCNTRY, PZIP,

                -- [2. HOME]
                TRIM(NVL(CAST(MAD1 AS VARCHAR), '') || ' ' || NVL(CAST(MAD2 AS VARCHAR), '')) AS home_houseno,
                MAD3 AS home_road,
                MMOO, ZMSDISCD, MCITY, MSTATE, MCNTRY, MZIP,

                -- [3. WORK]
                TRIM(NVL(CAST(ZOAD1 AS VARCHAR), '') || ' ' || NVL(CAST(ZOAD2 AS VARCHAR), '')) AS work_houseno,
                ZOAD3 AS work_road,
                ZOMOO, ZOSDISCD, ZOCITY, ZOSTATE, ZOCNTRY, ZOZIP

            FROM DA_PROD.cleansing_TB_CBS_CIF_20251031
            WHERE ACN > 150000
            ORDER BY ACN
            LIMIT 50000
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
            
            batch_docs = []
            total_inserted = 0
            first_run = True

            print("🔄 Processing Data...")

            for ACN, group in grouped_stream:
                document = {
                    "cif": str(ACN),
                    "addresses": []
                }
                
                rows_list = list(group)
                rows_list.sort(key=lambda r: r.get(MASTER_DATE_COL) or datetime.datetime.min, reverse=True)
                
                for i, row in enumerate(rows_list):
                    current_status = "Active" if i == 0 else "Inactive"
                    
                    for rule in ADDRESS_MAPPINGS:
                        addr_obj = build_address_object(row, rule, current_status)
                        if addr_obj:
                            document["addresses"].append(addr_obj)
                            
                            # --- DEBUG: ดูผลลัพธ์ภาษาไทย ---
                            if first_run:
                                print("\n" + "="*50)
                                print("🔎 CHECK RESULT (SHOULD BE CLEAN THAI)")
                                print("="*50)
                                print(json.dumps(addr_obj, indent=4, ensure_ascii=False)) 
                                print("="*50 + "\n")
                                first_run = False
                
                if document["addresses"]:
                    batch_docs.append(document)
                
                if len(batch_docs) >= BATCH_SIZE:
                    collection.insert_many(batch_docs)
                    total_inserted += len(batch_docs)
                    print(f"   -> Inserted {len(batch_docs)} CIFs (Total: {total_inserted})")
                    batch_docs = []

            if batch_docs:
                collection.insert_many(batch_docs)
                total_inserted += len(batch_docs)
                print(f"   -> Inserted remaining {len(batch_docs)} CIFs.")
                
    except Exception as e:
        print(f"❌ Error Occurred: {e}")
    finally:
        mongo_client.close()
        print(f"\n✅ Job Finished! Total processed: {total_inserted}")

if __name__ == "__main__":
    run_full_migration()