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

# --- MAPPING CONFIGURATION ---
COLUMN_MAPPING = {
    "HOME": {
        "main_col": "HPH",
        "ext_col": "ZHPHEXT",
        "date_col": "DATE_KEY"
    },
    "WORK": {
        "main_col": "BPH",
        "ext_col": "BPHEXT",
        "date_col": "DATE_KEY"
    },
    "PERSONAL": {
        "main_col": "APH",
        "ext_col": None,
        "date_col": "DATE_KEY"
    }
}

# =============================================================================
# 2. DATA TRANSFORMATION LOGIC
# =============================================================================

def transform_wide_to_long(df_raw):
    """ แปลงข้อมูลจาก Wide Format -> Long Format """
    all_data_frames = []

    for category, cols in COLUMN_MAPPING.items():
        main_col = cols['main_col']
        ext_col = cols['ext_col']
        date_col = cols.get('date_col', 'updated_date')

        if main_col not in df_raw.columns:
            continue
            
        if date_col not in df_raw.columns and 'updated_date' in df_raw.columns:
            date_col = 'updated_date'

        # Filter แถวที่มีเบอร์
        sub_df = df_raw[df_raw[main_col].notna() & (df_raw[main_col] != '')].copy()

        if sub_df.empty:
            continue

        # จัด Format วันที่
        try:
            sub_df['formatted_date'] = pd.to_datetime(sub_df[date_col], errors='coerce') \
                                         .dt.strftime('%Y-%m-%d')
            sub_df['formatted_date'] = sub_df['formatted_date'].fillna("1970-01-01")
        except Exception:
            sub_df['formatted_date'] = "1970-01-01"

        # Logic รวมเบอร์
        def combine_phone(row):
            main_num = str(row[main_col]).strip()
            if not ext_col or pd.isna(row[ext_col]):
                return main_num
            ext_num = str(row[ext_col]).strip()
            if ext_num in ['', '0', '-', 'nan', 'None']:
                return main_num
            return f"{main_num}:{ext_num}"

        temp_df = pd.DataFrame()
        temp_df['cif'] = sub_df[str('ACN')]
        temp_df['source'] = 'CBS'
        temp_df['category'] = category
        temp_df['fieldUpdatedAt'] = sub_df['formatted_date']
        temp_df['number'] = sub_df.apply(combine_phone, axis=1)
        
        all_data_frames.append(temp_df)

    if not all_data_frames:
        return pd.DataFrame()

    return pd.concat(all_data_frames, ignore_index=True)

def group_and_calculate_status(df_long):
    """ คำนวณ Active Status เฉพาะใน Batch ปัจจุบัน """
    def process_cif_group(group):
        # หา Max Date ของแต่ละ Category ภายในคนๆ นี้ใน Batch นี้
        max_dates_by_cat = group.groupby('category')['fieldUpdatedAt'].max().to_dict()
        
        records = []
        for _, row in group.iterrows():
            cat = row['category']
            is_latest = (row['fieldUpdatedAt'] == max_dates_by_cat.get(cat))
            
            records.append({
                'source': row['source'],
                'category': cat,
                'fieldUpdatedAt': row['fieldUpdatedAt'],
                'number': row['number'],
                'status': 'Active' if is_latest else 'Inactive'
            })
        return records

    print("Grouping data and calculating status (in-batch)...")
    return df_long.groupby('cif').apply(process_cif_group).reset_index(name='PhoneNumber')

# =============================================================================
# 3. MAIN EXECUTION
# =============================================================================

def run_etl():
    print("--- STARTING ETL ---")
    
    # 1. Read Vertica
    print(f"Connecting to Vertica...")
    # SQL Query (ปรับ Limit หรือ Condition ตามต้องการ)
    sql = "SELECT ACN,HPH,ZHPHEXT,BPH,BPHEXT,APH,DATE_KEY FROM DA_PROD.cleansing_TB_CBS_CIF_20251130 WHERE ACN >2000000 ORDER BY ACN LIMIT 5000"
    
    try:
        with vertica_python.connect(**VERTICA_CONN_INFO) as conn:
            df_raw = pd.read_sql(sql, conn)
    except Exception as e:
        print(f"Error reading Vertica: {e}")
        return

    if df_raw.empty:
        print("No data found.")
        return
    
    # Clean ACN (เอาจุดทศนิยมออกถ้ามี)
    df_raw['ACN'] = df_raw['ACN'].astype(str).apply(lambda x: x.split('.')[0])
    print(f"Loaded {len(df_raw)} raw rows.")

    # 2. Transform (Wide -> Long)
    print("Transforming data format...")
    df_long = transform_wide_to_long(df_raw)
    
    if df_long.empty:
        print("No phone numbers found after transformation.")
        return

    # 3. Group & Status Logic
    final_df = group_and_calculate_status(df_long)
    
    # 4. Insert/Update to MongoDB (Logic: Deactivate Old -> Push New)
    print(f"Prepared {len(final_df)} CIFs. Processing bulk operations...")

    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        coll = db[MONGO_COLLECTION_NAME]
        
        operations = []
        
        # วนลูป CIF แต่ละคน
        for index, row in final_df.iterrows():
            cif = row['cif']
            new_phones_list = row['PhoneNumber'] # list ของเบอร์ใหม่จาก Vertica
            
            # วนลูปเบอร์โทรแต่ละเบอร์ใน List
            for phone_item in new_phones_list:
                
                # CASE: เบอร์ใหม่เป็น Active -> ต้องไปปิดเบอร์เก่าก่อน
                if phone_item['status'] == 'Active':
                    target_category = phone_item['category']
                    
                    # Op 1: หาเบอร์เก่าที่เป็น Category เดียวกัน แล้วแก้เป็น Inactive
                    op_deactivate = UpdateOne(
                        {'cif': cif}, 
                        {'$set': {'PhoneNumber.$[elem].status': 'Inactive'}}, 
                        array_filters=[{'elem.category': target_category}],   
                        upsert=False
                    )
                    operations.append(op_deactivate)

                # Op 2: เอาเบอร์ใหม่ใส่เข้าไป (Push)
                # ใช้ upsert=True เพื่อว่าถ้าเป็น CIF ใหม่ที่ไม่เคยมี document เลย จะได้สร้างให้
                op_push = UpdateOne(
                    {'cif': cif},
                    {
                        '$push': {'PhoneNumber': phone_item},
                        # อัปเดตเวลาแก้ไขล่าสุดของ Document (Optional)
                        # '$set': {'last_updated': datetime.datetime.now()} 
                    },
                    upsert=True 
                )
                operations.append(op_push)

        # ยิงคำสั่งเข้า MongoDB (Bulk Write)
        if operations:
            print(f"Executing {len(operations)} operations...")
            # ordered=True สำคัญมาก! เพื่อให้ Deactivate ทำงานก่อน Push เสมอ
            result = coll.bulk_write(operations, ordered=True)
            print(f"Done! Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_count}")
        else:
            print("No operations to execute.")
            
    except Exception as e:
        print(f"Error writing to MongoDB: {e}")
    finally:
        if client:
            client.close()

    print("--- FINISHED ---")

if __name__ == "__main__":
    run_etl()