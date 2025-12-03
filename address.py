import vertica_python
from pymongo import MongoClient, UpdateOne
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

BATCH_SIZE = 2000 # ลดขนาดลงนิดหน่อยเพราะ 1 รายการอาจสร้าง 2 operations
MASTER_DATE_COL = "row_updated_at" 

# ==============================================================================
# PART 2: MAPPING CONFIGURATION (คงเดิม)
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
# PART 3: HELPER FUNCTIONS
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
    if val is None: return None
    val_str = str(val).strip()
    if val_str == "": return None
    cleaned_chars = [c for c in val_str if (0x0e00 <= ord(c) <= 0x0e7f) or (32 <= ord(c) <= 126)]
    result = "".join(cleaned_chars).strip()
    return result if result else None

def build_address_object(row, rule, calculated_status):
    cols = rule['cols']
    
    raw_houseno = row.get(cols.get('houseNo'))
    final_houseno = sanitize_text(raw_houseno)
    
    if not final_houseno: return None

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
# PART 4: MAIN EXECUTION (Updated Logic)
# ==============================================================================

def run_full_migration():
    print("🚀 Starting Migration Process (Upsert/Append Mode)...")
    
    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

    try:
        with vertica_python.connect(**VERTICA_CONN_INFO) as conn:
            cursor = conn.cursor()

            # SQL Query
            query = """
            SELECT 
                ACN,
                DATE_KEY AS row_updated_at,
                -- [1. CARDID]
                TRIM(NVL(CAST(PAD1 AS VARCHAR), '') || ' ' || NVL(CAST(PAD2 AS VARCHAR), '')) AS card_houseno,
                PAD3 AS card_road, PMOO, ZPSDISCD, PCITY, PSTATE, PCNTRY, PZIP,
                -- [2. HOME]
                TRIM(NVL(CAST(MAD1 AS VARCHAR), '') || ' ' || NVL(CAST(MAD2 AS VARCHAR), '')) AS home_houseno,
                MAD3 AS home_road, MMOO, ZMSDISCD, MCITY, MSTATE, MCNTRY, MZIP,
                -- [3. WORK]
                TRIM(NVL(CAST(ZOAD1 AS VARCHAR), '') || ' ' || NVL(CAST(ZOAD2 AS VARCHAR), '')) AS work_houseno,
                ZOAD3 AS work_road, ZOMOO, ZOSDISCD, ZOCITY, ZOSTATE, ZOCNTRY, ZOZIP
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
            total_ops_count = 0

            print("🔄 Processing Data...")

            for ACN, group in grouped_stream:
                rows_list = list(group)
                # เรียงตามวันที่ล่าสุดก่อน (เพื่อกำหนด Active)
                rows_list.sort(key=lambda r: r.get(MASTER_DATE_COL) or datetime.datetime.min, reverse=True)
                
                # เก็บ Address ทั้งหมดของ CIF นี้ที่จะเพิ่มเข้าไป
                new_addresses_to_push = []

                for i, row in enumerate(rows_list):
                    current_status = "Active" if i == 0 else "Inactive"
                    
                    for rule in ADDRESS_MAPPINGS:
                        addr_obj = build_address_object(row, rule, current_status)
                        if addr_obj:
                            new_addresses_to_push.append(addr_obj)

                if not new_addresses_to_push:
                    continue

                # =========================================================
                # LOGIC: Deactivate Old -> Push New
                # =========================================================
                
                cif_str = str(ACN)

                # วนลูปทุก Address ใหม่ที่จะใส่เข้าไป
                for addr in new_addresses_to_push:
                    
                    # ถ้า Address ใหม่เป็น Active -> ต้องไปปิดของเก่า (Inactive) ก่อน
                    if addr['status'] == 'Active':
                        target_category = addr['category'] # เช่น HOME, WORK
                        
                        # Op 1: หา Address เดิมใน MongoDB ที่เป็น Category เดียวกัน แล้วแก้เป็น Inactive
                        op_deactivate = UpdateOne(
                            {'cif': cif_str},
                            {'$set': {'addresses.$[elem].status': 'Inactive'}},
                            array_filters=[{'elem.category': target_category}], # กรองเฉพาะสมาชิก array ที่ category ตรงกัน
                            upsert=False
                        )
                        bulk_ops.append(op_deactivate)

                    # Op 2: เพิ่ม Address นี้เข้าไปใน Array (Push)
                    op_push = UpdateOne(
                        {'cif': cif_str},
                        {
                            '$push': {'addresses': addr}
                            # Optional: '$set': {'last_updated': datetime.datetime.now()}
                        },
                        upsert=True # ถ้ายังไม่มี CIF นี้ให้สร้างใหม่
                    )
                    bulk_ops.append(op_push)

                # Execute Bulk Write
                if len(bulk_ops) >= BATCH_SIZE:
                    # ordered=True สำคัญมาก! เพื่อให้ Deactivate ทำงานก่อน Push
                    collection.bulk_write(bulk_ops, ordered=True)
                    total_ops_count += len(bulk_ops)
                    print(f"   -> Executed {len(bulk_ops)} operations...")
                    bulk_ops = []

            # เก็บตกรายการที่เหลือ
            if bulk_ops:
                collection.bulk_write(bulk_ops, ordered=True)
                total_ops_count += len(bulk_ops)
                print(f"   -> Executed remaining operations.")
                
    except Exception as e:
        print(f"❌ Error Occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        mongo_client.close()
        print(f"\n✅ Job Finished! Total operations: {total_ops_count}")

if __name__ == "__main__":
    run_full_migration()