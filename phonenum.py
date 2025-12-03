import vertica_python
import pandas as pd
from pymongo import MongoClient
import sys

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

# --- MAPPING CONFIGURATION (หัวใจหลัก) ---
# กำหนดว่า Category ไหน ใช้ Column ไหนจาก Vertica
# main_col: เบอร์หลัก
# ext_col : เบอร์ต่อ (ใส่ None ถ้าไม่มี)
# date_col: วันที่อัปเดตของเบอร์นั้นๆ (เพื่อเอามาหา Active/Inactive)

COLUMN_MAPPING = {
    "HOME": {
        "main_col": "HPH",
        "ext_col": "ZHPHEXT",
        "date_col": "DATE_KEY"  # <-- แก้ชื่อ Column วันที่จริงของเบอร์บ้านที่นี่
    },
    "WORK": {
        "main_col": "BPH",
        "ext_col": "BPHEXT",
        "date_col": "DATE_KEY"  # <-- แก้ชื่อ Column วันที่จริงของเบอร์ที่ทำงานที่นี่
    },
    "PERSONAL": {
        "main_col": "APH",
        "ext_col": None,               # Personal ไม่รวมเบอร์ต่อ
        "date_col": "DATE_KEY"  # <-- แก้ชื่อ Column วันที่จริงของเบอร์ส่วนตัวที่นี่
    }
}

# =============================================================================
# 2. DATA TRANSFORMATION LOGIC
# =============================================================================

def transform_wide_to_long(df_raw):
    """
    แปลงข้อมูลจาก Wide Format -> Long Format และจัด Format วันที่เหลือแค่ YYYY-MM-DD
    """
    all_data_frames = []

    for category, cols in COLUMN_MAPPING.items():
        main_col = cols['main_col']
        ext_col = cols['ext_col']
        date_col = cols.get('date_col', 'updated_date')

        # เช็ค Column
        if main_col not in df_raw.columns:
            print(f"Warning: Column '{main_col}' not found. Skipping {category}.")
            continue
            
        if date_col not in df_raw.columns and 'updated_date' in df_raw.columns:
            date_col = 'updated_date'

        # Filter แถวที่มีเบอร์
        sub_df = df_raw[df_raw[main_col].notna() & (df_raw[main_col] != '')].copy()

        if sub_df.empty:
            continue

        # =========================================================
        # แก้ไขจุดที่ 1: เปลี่ยน Format วันที่ตรงนี้
        # =========================================================
        try:
            # แปลงเป็นวันที่ -> จัด Format เอาแค่ ปี-เดือน-วัน
            sub_df['formatted_date'] = pd.to_datetime(sub_df[date_col], errors='coerce') \
                                         .dt.strftime('%Y-%m-%d')
                                         
            # ถ้าวันที่เป็น Null/Error ให้ใส่วันที่ Default หรือปล่อยเป็น None
            # ในที่นี้ใส่ 1970-01-01 เพื่อกัน Error ตอนเปรียบเทียบหา Active
            sub_df['formatted_date'] = sub_df['formatted_date'].fillna("1970-01-01")
        except Exception:
            sub_df['formatted_date'] = "1970-01-01"

        # =========================================================

        # Logic รวมเบอร์ (เหมือนเดิม)
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
        temp_df['fieldUpdatedAt'] = sub_df['formatted_date'] # ค่าที่แปลงแล้ว
        temp_df['number'] = sub_df.apply(combine_phone, axis=1)
        
        all_data_frames.append(temp_df)

    if not all_data_frames:
        return pd.DataFrame()

    return pd.concat(all_data_frames, ignore_index=True)

def group_and_calculate_status(df_long):
    """
    Group by CIF -> สร้าง Array -> คำนวณ Active Status
    """
    def process_cif_group(group):
        # 1. หา Max Date ของแต่ละ Category ภายในคนๆ นี้
        # ผลลัพธ์: {'HOME': '2023-12...', 'WORK': '2020-01...'}
        max_dates_by_cat = group.groupby('category')['fieldUpdatedAt'].max().to_dict()
        
        records = []
        for _, row in group.iterrows():
            cat = row['category']
            
            # 2. เช็คว่าวันที่ของแถวนี้ คือวันที่ล่าสุดของ Category นี้หรือไม่
            is_latest = (row['fieldUpdatedAt'] == max_dates_by_cat.get(cat))
            
            records.append({
                'source': row['source'],
                'category': cat,
                'fieldUpdatedAt': row['fieldUpdatedAt'],
                'number': row['number'],
                'status': 'Active' if is_latest else 'Inactive'
            })
        return records

    print("Grouping data and calculating status...")
    # GroupBy CIF และ Apply Logic
    return df_long.groupby('cif').apply(process_cif_group).reset_index(name='PhoneNumber')

# =============================================================================
# 3. MAIN EXECUTION
# =============================================================================

def run_etl():
    print("--- STARTING ETL ---")
    
    # 1. Read Vertica
    # ดึงมาแค่ Raw Data ทั้งหมด (หรือเฉพาะ Column ที่จำเป็นถ้าตารางใหญ่มาก)
    print(f"Connecting to Vertica...")
    sql = "SELECT  ACN,HPH,ZHPHEXT,BPH,BPHEXT,APH,DATE_KEY FROM DA_PROD.cleansing_TB_CBS_CIF_20251031 WHERE ACN >150000 ORDER BY ACN LIMIT 50000" # <-- แก้ชื่อ Table ตรงนี้
    
    try:
        with vertica_python.connect(**VERTICA_CONN_INFO) as conn:
            df_raw = pd.read_sql(sql, conn)
    except Exception as e:
        print(f"Error reading Vertica: {e}")
        return

    if df_raw.empty:
        print("No data found.")
        return
    
    df_raw['ACN'] = df_raw['ACN'].astype(str) 
    df_raw['ACN'] = df_raw['ACN'].apply(lambda x: x.split('.')[0])

    print(f"Loaded {len(df_raw)} raw rows.")

    # 2. Transform (Wide -> Long)
    print("Transforming data format...")
    df_long = transform_wide_to_long(df_raw)
    
    if df_long.empty:
        print("No phone numbers found after transformation.")
        return

    # 3. Group & Status Logic
    final_df = group_and_calculate_status(df_long)
    
    # 4. Insert to MongoDB
    mongo_docs = final_df.to_dict('records')
    print(f"Prepared {len(mongo_docs)} documents for MongoDB.")
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        coll = db[MONGO_COLLECTION_NAME]
        
        if mongo_docs:
            # ใช้ insert_many เพื่อความเร็ว
            result = coll.insert_many(mongo_docs)
            print(f"Successfully inserted {len(result.inserted_ids)} documents.")
            
        client.close()
    except Exception as e:
        print(f"Error writing to MongoDB: {e}")

    print("--- FINISHED ---")

if __name__ == "__main__":
    run_etl()