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

# [NEW] ฟังก์ชันดึง Address ที่ Active อยู่ปัจจุบันใน MongoDB (แยกตาม Source/Category)
def get_current_active_address(mongo_doc, source, category):
    if not mongo_doc or 'addresses' not in mongo_doc:
        return None
    
    # วนหาตัวที่เป็น Active ที่ตรงกับ Source และ Category
    for addr in mongo_doc['addresses']:
        if addr.get('status') == 'Active' and \
           addr.get('source') == source and \
           addr.get('category') == category:
            return addr
    return None

# [NEW] ฟังก์ชันเทียบข้อมูล (ไม่สนใจวันที่)
def is_address_identical(new_addr, old_addr):
    if not old_addr: return False
    
    # field ที่ไม่เอามาเทียบ
    ignored_keys = {'fieldUpdatedAt', 'status', 'lastModified'}
    
    for key, val in new_addr.items():
        if key in ignored_keys: continue
        # เทียบค่า (แปลงเป็น string เพื่อความชัวร์)
        if str(val) != str(old_addr.get(key)):
            return False
            
    return True

# ==============================================================================
# PART 4: MAIN EXECUTION
# ==============================================================================

def run_full_migration():
    print("🚀 Starting Migration Process (Smart Update Mode)...")
    
    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

    try:
        with vertica_python.connect(**VERTICA_CONN_INFO) as conn:
            cursor = conn.cursor()

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
            
            # ดึงข้อมูลทั้งหมดมาไว้ใน Memory (List of Dicts)
            all_rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Group ข้อมูลตาม ACN (ต้อง sort ก่อน groupby เสมอ)
            all_rows.sort(key=lambda x: x['ACN'])
            grouped_data = {k: list(v) for k, v in groupby(all_rows, key=lambda x: x['ACN'])}
            
            # เตรียมดึงข้อมูลเก่าจาก MongoDB
            cif_list = [str(k) for k in grouped_data.keys()]
            print(f"🔄 Fetching existing data for {len(cif_list)} CIFs from MongoDB...")
            
            existing_cursor = collection.find(
                {"cif": {"$in": cif_list}},
                {"cif": 1, "addresses": 1}
            )
            existing_docs_map = {doc['cif']: doc for doc in existing_cursor}
            
            bulk_ops = []
            stats = {"updated_timestamp": 0, "appended_new": 0}

            print("🔄 Processing Data & Building Operations...")

            for cif_raw, rows_list in grouped_data.items():
                cif_str = str(cif_raw)
                
                # เรียงตามวันที่ล่าสุดก่อน
                rows_list.sort(key=lambda r: r.get(MASTER_DATE_COL) or datetime.datetime.min, reverse=True)
                
                # เก็บ Address ใหม่ทั้งหมดที่จะ process จาก Vertica
                # (Logic เดิม: แถวแรกเป็น Active, ที่เหลือ Inactive)
                # แต่ใน Smart Update เราสนใจแค่ตัว Active ล่าสุดของ Vertica เพื่อไปเทียบกับ MongoDB
                
                latest_row = rows_list[0] # แถวล่าสุด
                
                # ดึงข้อมูลเก่าของคนนี้
                mongo_doc = existing_docs_map.get(cif_str)
                
                # วนลูปสร้าง Address Object ตาม Mapping (HOME, WORK, CARDID)
                for rule in ADDRESS_MAPPINGS:
                    # สร้าง Object จากข้อมูลใหม่ (ตั้งเป็น Active ไว้ก่อน)
                    new_addr_obj = build_address_object(latest_row, rule, "Active")
                    
                    if not new_addr_obj: continue
                    
                    target_source = new_addr_obj['source']
                    target_category = new_addr_obj['category']
                    
                    # หา Active Address เดิมใน Mongo
                    current_active_addr = get_current_active_address(mongo_doc, target_source, target_category)
                    
                    # ------------------------------------------------------------------
                    # CASE 1: ข้อมูลเหมือนเดิมเป๊ะ (Update Timestamp Only)
                    # ------------------------------------------------------------------
                    if current_active_addr and is_address_identical(new_addr_obj, current_active_addr):
                        
                        new_updated_at = new_addr_obj['fieldUpdatedAt']
                        
                        op_touch = UpdateOne(
                            {'cif': cif_str},
                            {'$set': {'addresses.$[elem].fieldUpdatedAt': new_updated_at}},
                            array_filters=[{
                                'elem.source': target_source,
                                'elem.category': target_category,
                                'elem.status': 'Active'
                            }],
                            upsert=False
                        )
                        bulk_ops.append(op_touch)
                        stats["updated_timestamp"] += 1
                        
                    # ------------------------------------------------------------------
                    # CASE 2: ข้อมูลเปลี่ยน หรือ เป็นข้อมูลใหม่ (Deactivate -> Push)
                    # ------------------------------------------------------------------
                    else:
                        # Op 1: ถ้ามีของเก่า ให้ Deactivate ก่อน
                        if current_active_addr:
                            op_deactivate = UpdateOne(
                                {'cif': cif_str},
                                {'$set': {'addresses.$[elem].status': 'Inactive'}},
                                array_filters=[{
                                    'elem.source': target_source,
                                    'elem.category': target_category,
                                    'elem.status': 'Active'
                                }],
                                upsert=False
                            )
                            bulk_ops.append(op_deactivate)
                        
                        # Op 2: Push ของใหม่
                        op_push = UpdateOne(
                            {'cif': cif_str},
                            {
                                '$push': {'addresses': new_addr_obj}
                            },
                            upsert=True
                        )
                        bulk_ops.append(op_push)
                        stats["appended_new"] += 1

                # Execute Bulk Write
                if len(bulk_ops) >= BATCH_SIZE:
                    collection.bulk_write(bulk_ops, ordered=True)
                    print(f"   -> Executed batch operations...")
                    bulk_ops = []

            # เก็บตกรายการที่เหลือ
            if bulk_ops:
                collection.bulk_write(bulk_ops, ordered=True)
                
            print(f"\n📊 Summary:")
            print(f"   - Timestamp Updated (No Change): {stats['updated_timestamp']}")
            print(f"   - New Address Appended (Changed): {stats['appended_new']}")
                
    except Exception as e:
        print(f"❌ Error Occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        mongo_client.close()
        print(f"\n✅ Job Finished!")

if __name__ == "__main__":
    run_full_migration()