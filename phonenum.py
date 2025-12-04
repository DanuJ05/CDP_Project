import vertica_python
import pandas as pd
from pymongo import MongoClient, UpdateOne
import sys
import datetime

# =============================================================================
# 1. CONFIGURATION (ตั้งค่าระบบ)
# =============================================================================

# ตั้งค่า Vertica
VERTICA_CONN_INFO = {
    'host': '172.26.133.65',       # IP ของ Database
    'port': 5433,
    'user': 'P6600566',         # Username
    'password': 'P@6600566',    # Password
    'database': 'BAACDWH',       # Database Name
    'unicode_error': 'replace' # [สำคัญ] แก้ปัญหา UnicodeDecodeError (อ่านไม่ออกให้ข้าม/แทนที่)
}

# ตั้งค่า MongoDB
MONGO_URI = "mongodb://admin:password@eden206.kube.baac.or.th:27044/"
MONGO_DB_NAME = "CDP"
MONGO_COLLECTION_NAME = "phonenum"
BATCH_SIZE = 2000  # จำนวนข้อมูลต่อ 1 ครั้งที่อ่านจาก Vertica


# MAPPING CONFIGURATION
MAPPING_CONFIG = [
    { "source": "CBS", "category": "HOME",     "main_col": "HPH", "ext_col": "ZHPHEXT", "date_col": "DATE_KEY" },
    { "source": "CBS", "category": "WORK",     "main_col": "BPH", "ext_col": "BPHEXT",  "date_col": "DATE_KEY" },
    { "source": "CBS", "category": "PERSONAL", "main_col": "APH", "ext_col": None,      "date_col": "DATE_KEY" },
    
    # Other Sources
    { "source": "SMS",        "category": "PERSONAL", "main_col": "mobile_no",      "ext_col": None, "date_col": "register_date" },
    { "source": "LINEMOBILE", "category": "PERSONAL", "main_col": "verify_mobile_no", "ext_col": None, "date_col": "Data_date" },
    
    # [AMOBILE]: date_col เป็น None -> จะถูกแทนที่ด้วยวันที่ปัจจุบันใน code
    { "source": "AMOBILE",    "category": "PERSONAL", "main_col": "PHONE_NUMBER",     "ext_col": None, "date_col": None }
]

# =============================================================================
# 2. DATA TRANSFORMATION LOGIC
# =============================================================================

def transform_wide_to_long(df_raw):
    """ แปลงข้อมูลจาก Wide Format -> Long Format """
    all_data_frames = []
    
    # ดึงวันที่ปัจจุบันเตรียมไว้ (format YYYY-MM-DD)
    current_date_str = datetime.datetime.now().strftime('%Y-%m-%d')

    for config in MAPPING_CONFIG:
        source = config['source']
        category = config['category']
        main_col = config['main_col']
        ext_col = config['ext_col']
        date_col = config.get('date_col') # อาจจะเป็นชื่อ Column หรือ None

        # เช็คว่ามี Column เบอร์โทรใน DataFrame หรือไม่
        if main_col not in df_raw.columns:
            continue
            
        # Filter เฉพาะแถวที่มีเบอร์
        sub_df = df_raw[df_raw[main_col].notna() & (df_raw[main_col] != '')].copy()

        if sub_df.empty:
            continue

        # =========================================================
        # [LOGIC จัดการวันที่]
        # =========================================================
        if date_col is None:
            # กรณี AMOBILE: ถ้า config บอกว่าเป็น None ให้ใช้วันที่ปัจจุบัน
            sub_df['formatted_date'] = current_date_str
        else:
            # กรณีอื่นๆ: ให้ดึงจาก Column ใน Vertica
            # กันเหนียว: ถ้าหา column ไม่เจอให้ลอง updated_date หรือใช้ 1970
            target_date_col = date_col if date_col in sub_df.columns else 'updated_date'
            
            try:
                if target_date_col in sub_df.columns:
                    sub_df['formatted_date'] = pd.to_datetime(sub_df[target_date_col], errors='coerce').dt.strftime('%Y-%m-%d')
                    sub_df['formatted_date'] = sub_df['formatted_date'].fillna("1970-01-01")
                else:
                    sub_df['formatted_date'] = "1970-01-01"
            except Exception:
                sub_df['formatted_date'] = "1970-01-01"
        # =========================================================

        def combine_phone(row):
            main_num = str(row[main_col]).strip()
            if not ext_col or pd.isna(row[ext_col]) or ext_col not in row:
                return main_num
            ext_num = str(row[ext_col]).strip()
            if ext_num in ['', '0', '-', 'nan', 'None']:
                return main_num
            return f"{main_num}:{ext_num}"

        temp_df = pd.DataFrame()
        temp_df['cif'] = sub_df[str('ACN')]
        temp_df['source'] = source
        temp_df['category'] = category
        temp_df['fieldUpdatedAt'] = sub_df['formatted_date']
        temp_df['number'] = sub_df.apply(combine_phone, axis=1)
        
        all_data_frames.append(temp_df)

    if not all_data_frames:
        return pd.DataFrame()

    return pd.concat(all_data_frames, ignore_index=True)

def group_and_calculate_status(df_long):
    """ คำนวณ Active Status (Per Source) """
    def process_cif_group(group):
        max_dates = group.groupby(['category', 'source'])['fieldUpdatedAt'].max().to_dict()
        records = []
        for _, row in group.iterrows():
            cat = row['category']
            src = row['source']
            is_latest = (row['fieldUpdatedAt'] == max_dates.get((cat, src)))
            records.append({
                'source': src,
                'category': cat,
                'fieldUpdatedAt': row['fieldUpdatedAt'],
                'number': row['number'],
                'status': 'Active' if is_latest else 'Inactive'
            })
        return records

    print("Grouping data and calculating status...")
    return df_long.groupby('cif').apply(process_cif_group).reset_index(name='PhoneNumber')

# =============================================================================
# 3. HELPER FOR CHECKING DUPLICATES
# =============================================================================

def get_current_active_number(cif_doc, source, category):
    """ หาเบอร์ Active ปัจจุบันใน MongoDB """
    if not cif_doc or 'PhoneNumber' not in cif_doc:
        return None
    
    for item in cif_doc['PhoneNumber']:
        if item.get('status') == 'Active' and \
           item.get('source') == source and \
           item.get('category') == category:
            return item.get('number')
            
    return None

# =============================================================================
# 4. MAIN EXECUTION
# =============================================================================

def run_etl():
    print("--- STARTING ETL ---")
    
    # 1. Read Vertica
    print(f"Connecting to Vertica...")
    sql = """
        SELECT 
            a.ACN,
            -- CBS
            a.HPH,a.ZHPHEXT,a.BPH,a.BPHEXT,a.APH,a.DATE_KEY,
            -- SMS
            b.mobile_no,b.register_date,
            -- LINEMOBILE
            c.verify_mobile_no,c.Data_date,
            -- AMOBILE
            d.PHONE_NUMBER
        FROM DA_PROD.cleansing_TB_CBS_CIF_20251130 a
        LEFT JOIN DA_PROD.Mobile_SMS_Master b on a.ACN=b.cif
        LEFT JOIN DA_PROD.Mobile_Line_Master c on a.ACN=c.cif
        LEFT JOIN DA_PROD.cleansing_PHONE_MBP d on a.ACN=d.acn
        WHERE a.ACN > 2000000 
        ORDER BY a.ACN 
        LIMIT 5000
    """
    
    try:
        with vertica_python.connect(**VERTICA_CONN_INFO) as conn:
            df_raw = pd.read_sql(sql, conn)
    except Exception as e:
        print(f"Error reading Vertica: {e}")
        return

    if df_raw.empty:
        print("No data found.")
        return
    
    df_raw['ACN'] = df_raw['ACN'].astype(str).apply(lambda x: x.split('.')[0])
    print(f"Loaded {len(df_raw)} raw rows.")

    # 2. Transform (Logic AMOBILE ใช้ Current Date อยู่ที่นี่)
    print("Transforming data format...")
    df_long = transform_wide_to_long(df_raw)
    
    if df_long.empty:
        print("No phone numbers found after transformation.")
        return

    # 3. Group & Status Logic
    final_df = group_and_calculate_status(df_long)
    
    # 4. Update MongoDB
    print(f"Prepared {len(final_df)} CIFs. Checking against existing MongoDB data...")

    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        coll = db[MONGO_COLLECTION_NAME]
        
        # Pre-fetch Existing Data
        cif_list_in_batch = final_df['cif'].unique().tolist()
        existing_cursor = coll.find(
            {'cif': {'$in': cif_list_in_batch}},
            {'cif': 1, 'PhoneNumber': 1}
        )
        existing_docs_map = {doc['cif']: doc for doc in existing_cursor}
        
        operations = []
        timestamp_updated_count = 0
        
        for index, row in final_df.iterrows():
            cif = row['cif']
            new_phones_list = row['PhoneNumber'] 
            
            current_doc_in_mongo = existing_docs_map.get(cif)
            
            for phone_item in new_phones_list:
                
                if phone_item['status'] == 'Active':
                    target_source = phone_item['source']
                    target_category = phone_item['category']
                    new_number_val = phone_item['number']
                    new_updated_date = phone_item['fieldUpdatedAt'] # สำหรับ AMOBILE ค่านี้คือ Today แล้ว
                    
                    current_active_num = get_current_active_number(current_doc_in_mongo, target_source, target_category)
                    
                    # CASE 1: เบอร์เดิม -> อัปเดตวันที่เป็นปัจจุบัน (Touch)
                    if current_active_num == new_number_val:
                        op_update_timestamp = UpdateOne(
                            {'cif': cif},
                            {'$set': {'PhoneNumber.$[elem].fieldUpdatedAt': new_updated_date}},
                            array_filters=[{
                                'elem.category': target_category,
                                'elem.source': target_source,
                                'elem.status': 'Active'
                            }],
                            upsert=False
                        )
                        operations.append(op_update_timestamp)
                        timestamp_updated_count += 1
                        continue 
                    
                    # CASE 2: เบอร์ใหม่ -> Deactivate เก่า + Push ใหม่
                    if current_active_num is not None:
                         op_deactivate = UpdateOne(
                            {'cif': cif}, 
                            {'$set': {'PhoneNumber.$[elem].status': 'Inactive'}}, 
                            array_filters=[{
                                'elem.category': target_category,
                                'elem.source': target_source 
                            }],   
                            upsert=False
                        )
                         operations.append(op_deactivate)

                    op_push = UpdateOne(
                        {'cif': cif},
                        {
                            '$push': {'PhoneNumber': phone_item}
                        },
                        upsert=True 
                    )
                    operations.append(op_push)

        print(f"Stats: {timestamp_updated_count} existing records will have timestamp updated.")
        
        if operations:
            print(f"Executing {len(operations)} operations...")
            result = coll.bulk_write(operations, ordered=True)
            print(f"Done! Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_count}")
        else:
            print("No operations needed.")
            
    except Exception as e:
        print(f"Error writing to MongoDB: {e}")
    finally:
        if client:
            client.close()

    print("--- FINISHED ---")

if __name__ == "__main__":
    run_etl()