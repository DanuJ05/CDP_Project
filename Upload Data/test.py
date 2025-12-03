# ใส่ข้อความจีนที่มีปัญหาลงไป
bad_text = "䙽㻲ᗖ萠䪫朏ᑜ飸䩽㷑峸ꐵꑵ㔲䟮乒莣㡂莨荓躂Ҽ"

print(f"ข้อความ: {bad_text}")
print("-" * 30)

# ดูค่า Hex จริงๆ ของข้อมูล
raw_bytes = bad_text.encode('utf-16-le')
print(f"HEX (RAW): {raw_bytes.hex().upper()}")

print("-" * 30)
print("วิเคราะห์เบื้องต้น:")
if len(bad_text) > 0:
    print("ถ้า Hex ขึ้นต้นด้วย C8 7F ... แสดงว่าข้อมูลอาจจะเสีย")
    print("ถ้า Hex ดูเป็นระเบียบ อาจเป็น EBCDIC (Mainframe)")