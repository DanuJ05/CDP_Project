##### CDP_Project

#Upload File

-Info

-Address

-Phonenum

-----

📄 Customer Profile Migration (CBS -> MongoDB)
โครงการนี้คือสคริปต์ ETL ที่มีกลไก Smart Update เพื่อดึงข้อมูลประวัติลูกค้า (Customer Profile) จาก Vertica และจัดเก็บใน MongoDB ด้วยวิธีการบันทึกประวัติการเปลี่ยนแปลง (SCD Type 2) .

💡 หลักการทำงาน (Smart Update & History Tracking)
สคริปต์นี้ถูกออกแบบมาเพื่อ:

ดึงข้อมูลล่าสุด: ดึง Record ล่าสุดของ Profile ลูกค้าแต่ละรายจาก Vertica.

เปรียบเทียบข้อมูล: เทียบข้อมูล Profile ล่าสุดกับ Profile ที่มีสถานะเป็น Active ใน MongoDB.

จัดการการเปลี่ยนแปลง:

ไม่มีการเปลี่ยนแปลง: อัปเดตเพียง fieldUpdatedAt ของ Record ที่ Active ใน MongoDB เพื่อ "Touch" (อัปเดต Timestamp) ว่าข้อมูลนี้ยังคงถูกต้อง.

มีการเปลี่ยนแปลง: เปลี่ยนสถานะของ Record ที่ Active เดิมให้เป็น Inactive จากนั้นจึง Push Record ใหม่ (พร้อมสถานะ Active) เข้าไปในอาร์เรย์ profile เพื่อบันทึกประวัติ.

📦 ข้อกำหนดเบื้องต้น (Prerequisites)
Python 3.x

การเชื่อมต่อเครือข่ายไปยัง Vertica Server และ MongoDB Server

ไลบรารี: vertica-python, pymongo

🛠️ การติดตั้งและการตั้งค่า (Setup and Configuration)
1. การติดตั้งไลบรารี
Bash

pip install vertica-python pymongo
2. การตั้งค่าการเชื่อมต่อ
แก้ไขข้อมูลการเชื่อมต่อฐานข้อมูลในส่วน PART 1: CONFIGURATION ของไฟล์สคริปต์:

Python

# VERTICA
VERTICA_CONN_INFO = {
    'host': 'YOUR_VERTICA_HOST',
    'user': 'YOUR_VERTICA_USER',
    'password': 'YOUR_VERTICA_PASSWORD',
    # ...
}

# MONGODB
MONGO_URI = 'YOUR_MONGO_URI'
MONGO_DB = 'CDP'
MONGO_COLLECTION = 'info'
BATCH_SIZE = 2000 # ขนาด Batch สำหรับ Bulk Write
3. การตั้งค่า Mapping
ตรวจสอบและแก้ไข PROFILE_MAPPING ใน PART 2 หากมีการเปลี่ยนแปลงชื่อคอลัมน์ใน Vertica หรือต้องการเปลี่ยนชื่อ Field ใน MongoDB.

▶️ วิธีการรันสคริปต์ (Usage)
รันสคริปต์โดยตรง:

Bash

python your_script_name.py
📝 โครงสร้างข้อมูล MongoDB (Output Structure)
ข้อมูลลูกค้าจะถูกจัดเก็บในรูปแบบเอกสาร (Document) ใน MongoDB ดังนี้:

JSON

{
  "_id": ObjectId("..."),
  "cif": "0123456789",
  "profile": [
    {
      "source": "CBS",
      "nationalId": "110200...",
      "firstName": "สมชาย",
      "lastName": "ใจดี",
      "birthDate": "1990-01-01",
      "fieldUpdatedAt": "2023-01-01T10:00:00",
      "status": "Inactive"
    },
    {
      "source": "CBS",
      "nationalId": "110200...",
      "firstName": "สมชาย",
      "lastName": "รักดี", // ข้อมูลมีการเปลี่ยนแปลง
      "birthDate": "1990-01-01",
      "fieldUpdatedAt": "2025-12-04T14:30:00",
      "status": "Active" // Record ปัจจุบัน
    }
  ]
}

-----

📄 ETL ที่อยู่ลูกค้า (CBS -> MongoDB Address)
โครงการนี้คือสคริปต์ ETL ที่มีกลไก Smart Update สำหรับการดึงและจัดการข้อมูลที่อยู่ลูกค้า (Address) จาก Vertica (แหล่งข้อมูล CBS) และจัดเก็บใน MongoDB โดยเน้นการบันทึกประวัติการเปลี่ยนแปลงของที่อยู่แต่ละประเภท (SCD Type 2).

💡 หลักการทำงาน (Smart Update & History Tracking)
สคริปต์นี้ใช้ตรรกะการเปรียบเทียบข้อมูลหลักของที่อยู่ (บ้านเลขที่, ถนน, ตำบล, ฯลฯ) เพื่อตัดสินใจว่าจะอัปเดตอย่างไร:

ดึงข้อมูลล่าสุด: ดึงข้อมูลที่อยู่ CARDID, HOME, และ WORK ล่าสุดของลูกค้าแต่ละรายจาก Vertica.

เปรียบเทียบแยกตามประเภท: เทียบข้อมูลที่อยู่ใหม่กับที่อยู่เดิมที่สถานะ Active ใน MongoDB สำหรับ (Source, Category) เดียวกัน.

ผลลัพธ์การอัปเดต:

ไม่มีการเปลี่ยนแปลง (Touch): ข้อมูลที่อยู่หลักเหมือนเดิม -> อัปเดตเพียง fieldUpdatedAt เท่านั้น.

มีการเปลี่ยนแปลง (SCD Type 2): ข้อมูลที่อยู่หลักมีการเปลี่ยนแปลง -> 1) เปลี่ยนสถานะของ Address Active เดิมเป็น Inactive และ 2) Push Address ใหม่ (สถานะ Active) เข้าไปในอาร์เรย์ addresses.

📦 ข้อกำหนดเบื้องต้น (Prerequisites)
Python 3.x

การเชื่อมต่อเครือข่ายไปยัง Vertica Server และ MongoDB Server

ไลบรารี: vertica-python, pymongo, pandas (ไม่ได้ใช้โดยตรง แต่เป็นมาตรฐานของ ETL), itertools

🛠️ การติดตั้งและการตั้งค่า (Setup and Configuration)
1. การติดตั้งไลบรารี
Bash

pip install vertica-python pymongo
2. การตั้งค่าการเชื่อมต่อ
แก้ไขข้อมูลการเชื่อมต่อฐานข้อมูลในส่วน PART 1: CONFIGURATION ของไฟล์สคริปต์.

Python

# VERTICA
VERTICA_CONN_INFO = {
    'host': 'YOUR_VERTICA_HOST',
    'user': 'YOUR_VERTICA_USER',
    # ...
}

# MONGODB
MONGO_URI = 'YOUR_MONGO_URI'
MONGO_DB = 'CDP'
MONGO_COLLECTION = 'address' # Collection เป้าหมาย
3. การตั้งค่า Mapping
ตรวจสอบและแก้ไข ADDRESS_MAPPINGS ใน PART 2 หากมีการเปลี่ยนแปลงแหล่งข้อมูลหรือชื่อคอลัมน์ใน Vertica.

▶️ วิธีการรันสคริปต์ (Usage)
รันสคริปต์โดยตรง:

Bash

python your_script_name.py
📝 โครงสร้างข้อมูล MongoDB (Output Structure)
ข้อมูลที่อยู่ลูกค้าจะถูกจัดเก็บในรูปแบบเอกสาร (Document) ในคอลเลกชัน address ดังนี้:

JSON

{
  "_id": ObjectId("..."),
  "cif": "0123456789",
  "addresses": [
    {
      "source": "CBS",
      "category": "CARDID", 
      "fieldUpdatedAt": "2023-01-01T00:00:00",
      "houseNo": "123/45",
      "road": "ถนนสุขุมวิท",
      // ... รายละเอียดที่อยู่
      "status": "Inactive"
    },
    {
      "source": "CBS",
      "category": "CARDID", 
      "fieldUpdatedAt": "2025-12-04T14:30:00",
      "houseNo": "567/89", // ที่อยู่ใหม่
      "road": "ถนนรามคำแหง",
      // ... รายละเอียดที่อยู่
      "status": "Active" // Record ปัจจุบัน
    },
    // ... Address Category อื่นๆ (HOME, WORK)
  ]
}

-----

Phonenum.py

-----

### 🚀 สถาปัตยกรรมโดยรวม

สคริปต์นี้ทำงานตามขั้นตอนดังนี้:

1.  **Extract**: เชื่อมต่อ Vertica และดึงข้อมูลดิบของเบอร์โทรศัพท์จากหลายแหล่งข้อมูล (CBS, SMS, LINEMOBILE, AMOBILE).
2.  **Transform**:
      * แปลงข้อมูลจาก Wide Format เป็น Long Format.
      * รวมเบอร์โทรศัพท์กับส่วนขยาย (ถ้ามี).
      * คำนวณสถานะ Active/Inactive ของเบอร์โทรศัพท์แต่ละรายการ โดยอิงตามวันที่อัปเดตล่าสุดของแต่ละ `(Source, Category)`.
3.  **Load**: เชื่อมต่อ MongoDB และทำการอัปเดต/เพิ่ม (Upsert) ข้อมูลเบอร์โทรศัพท์สำหรับแต่ละ CIF โดยใช้การทำรายการแบบ Bulk Write.

-----

### 🛠️ ข้อกำหนดเบื้องต้น (Prerequisites)

  * Python 3.x
  * การเชื่อมต่อเครือข่ายไปยัง Vertica Server และ MongoDB Server
  * Credentials สำหรับการเข้าถึงฐานข้อมูลทั้งสอง

### 📦 การติดตั้ง (Installation)

ติดตั้งไลบรารี Python ที่จำเป็น:

```bash
pip install vertica-python pandas pymongo
```

### ⚙️ การตั้งค่า (Configuration)

ทำการแก้ไขข้อมูลการเชื่อมต่อและ Mapping ในส่วน **1. CONFIGURATION** ของไฟล์สคริปต์:

#### **การตั้งค่า Vertica**

อัปเดตข้อมูลการเชื่อมต่อใน `VERTICA_CONN_INFO`:

```python
VERTICA_CONN_INFO = {
    'host': '172.26.133.65',
    'port': 5433,
    'user': 'YOUR_USER',
    'password': 'YOUR_PASSWORD',
    'database': 'BAACDWH',
    'unicode_error': 'replace'
}
```

#### **การตั้งค่า MongoDB**

อัปเดต URI และชื่อฐานข้อมูล/คอลเลกชัน:

```python
MONGO_URI = "mongodb://admin:password@eden206.kube.baac.or.th:27044/"
MONGO_DB_NAME = "CDP"
MONGO_COLLECTION_NAME = "phonenum"
```

#### **MAPPING CONFIGURATION**

อัปเดตรายการ Mapping หากมีการเพิ่มหรือเปลี่ยนแหล่งข้อมูลใหม่ (ระบุ `source`, `category`, คอลัมน์เบอร์โทรหลัก `main_col`, และคอลัมน์วันที่ `date_col`):

```python
MAPPING_CONFIG = [
    { "source": "CBS", "category": "HOME", "main_col": "HPH", "ext_col": "ZHPHEXT", "date_col": "DATE_KEY" },
    # ... รายการอื่นๆ
    { "source": "AMOBILE", "category": "PERSONAL", "main_col": "PHONE_NUMBER", "ext_col": None, "date_col": None }
]
```

> **หมายเหตุ:** สำหรับ `date_col: None` (เช่น AMOBILE) สคริปต์จะใช้ **วันที่ปัจจุบัน** เป็น `fieldUpdatedAt`

-----

### ▶️ วิธีการใช้งาน (Usage)

รันสคริปต์ Python โดยตรง:

```bash
python your_script_name.py
```

### 📝 หลักการทำงานและการประมวลผลสถานะ

  * **การแปลงข้อมูล:** ข้อมูลดิบจากหลายคอลัมน์จะถูกรวมให้อยู่ในรูปแบบรายการเบอร์โทรศัพท์ (Long Format) โดยมี CIF เป็นกุญแจหลัก.
  * **การกำหนดสถานะ Active/Inactive:**
      * สถานะจะถูกคำนวณต่อ `(CIF, Source, Category)`.
      * เบอร์โทรศัพท์ที่มี **`fieldUpdatedAt` ล่าสุดที่สุด** สำหรับกลุ่ม `(Source, Category)` นั้น จะถูกกำหนดเป็น **'Active'**.
      * เบอร์โทรอื่นๆ ในกลุ่มเดียวกันจะถูกกำหนดเป็น **'Inactive'**.
  * **ตรรกะการอัปเดต MongoDB:**
    1.  **Check (Touch):** หากเบอร์ Active ใหม่ **ตรงกับ** เบอร์ Active เดิมใน MongoDB จะทำการอัปเดตแค่ Timestamp (`fieldUpdatedAt`) เท่านั้น.
    2.  **Replace (Deactivate + Push):** หากเบอร์ Active ใหม่ **ไม่ตรงกับ** เบอร์ Active เดิม:
          * เบอร์ Active เดิมจะถูกเปลี่ยนสถานะเป็น 'Inactive'.
          * เบอร์ Active ใหม่จะถูก `$push` เข้าไปในอาร์เรย์ `PhoneNumber`.

### 🗃️ โครงสร้างข้อมูล MongoDB

เอกสารในคอลเลกชัน `phonenum` จะมีลักษณะดังนี้:

```json
{
  "_id": ObjectId("..."),
  "cif": "0123456789",
  "PhoneNumber": [
    {
      "source": "CBS",
      "category": "HOME",
      "fieldUpdatedAt": "2023-11-30",
      "number": "021234567:1234",
      "status": "Active" 
    },
    {
      "source": "CBS",
      "category": "HOME",
      "fieldUpdatedAt": "2023-05-15",
      "number": "029876543",
      "status": "Inactive"
    },
    {
      "source": "AMOBILE",
      "category": "PERSONAL",
      "fieldUpdatedAt": "2025-12-04", # จะเป็นวันที่รันสคริปต์
      "number": "0811112222",
      "status": "Active" 
    }
  ]
}
```

------

##API 

📂 โครงสร้างไฟล์
main.py: Backend API ที่เขียนด้วย FastAPI ทำหน้าที่ดึงข้อมูลจาก MongoDB (Collections: info, address, phonenum)

index.html: Frontend หน้าเว็บสำหรับค้นหาข้อมูลลูกค้าด้วยเลข CIF

.env: ไฟล์เก็บค่า Configuration สำหรับเชื่อมต่อฐานข้อมูล

🛠️ ความต้องการระบบ (Prerequisites)
Python 3.8 ขึ้นไป

MongoDB Database (ตาม Connection String ที่ระบุ)

🚀 วิธีการติดตั้งและรันโปรเจกต์ (Installation)
1. ติดตั้ง Library ที่จำเป็น
เปิด Terminal แล้วรันคำสั่งต่อไปนี้เพื่อติดตั้ง Python Packages ที่ใช้ใน main.py:

Bash

pip install fastapi uvicorn motor pydantic-settings
(อ้างอิง dependencies จาก main.py: fastapi, motor, pydantic-settings, uvicorn)

2. ตั้งค่า Environment Variables
สร้างไฟล์ชื่อ .env ในโฟลเดอร์เดียวกับ main.py และใส่ค่าตามตัวอย่างด้านล่าง (แก้ไข MONGO_URL ให้ตรงกับเครื่องของคุณ):

Code snippet

MONGO_URL=mongodb://admin:password@eden206.kube.baac.or.th:27044
MONGO_DB_NAME=CDP
(อ้างอิงค่าจากไฟล์ env.txt)

3. รัน Backend Server
รันคำสั่งนี้ใน Terminal เพื่อเริ่มการทำงานของ API:

Bash

python main.py
Server จะเริ่มทำงานที่ http://0.0.0.0:8050

API Doc (Swagger UI) เข้าได้ที่: http://localhost:8050/docs

4. ใช้งาน Frontend
เปิดไฟล์ index.html ด้วย Web Browser (Chrome, Edge, etc.)

ระบบจะเชื่อมต่อ API ไปที่ http://127.0.0.1:8050 โดยอัตโนมัติ

กรอกเลข CIF (เช่น 2001247) แล้วกดปุ่ม ค้นหา

🔌 API Documentation
GET /search/cif/{cif_id}
ดึงข้อมูลลูกค้าจากเลข CIF โดยจะรวมข้อมูลจาก 3 แหล่งและคัดเฉพาะสถานะ Active เท่านั้น

Response Example:

JSON

{
  "cif": "2001247",
  "info": [ ... ],
  "addresses": [ ... ],
  "phone_numbers": [ ... ]
}
⚠️ หมายเหตุ
ตรวจสอบให้แน่ใจว่า Backend (main.py) กำลังรันอยู่ ก่อนที่จะเปิดหน้าเว็บใช้งาน

หากเชื่อมต่อฐานข้อมูลไม่ได้ ให้ตรวจสอบค่า MONGO_URL ในไฟล์ .env อีกครั้ง
